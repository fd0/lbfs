/*
 *
 * Copyright (C) 1998-2000 David Mazieres (dm@uun.org)
 * Copyright (C) 2002 Benjie Chen (benjie@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include <typeinfo>
#include "sfslbcd.h"
#include "axprt_crypt.h"
#include "axprt_compress.h"
#include "lbfs_prot.h"

int lbfs (getenv("LBFS") ? atoi (getenv ("LBFS")) : 2);

inline bool
operator< (const nfstime3 &t1, const nfstime3 &t2) {
  unsigned int ts1 = t1.seconds;
  unsigned int tns1 = t1.nseconds;
  unsigned int ts2 = t2.seconds;
  unsigned int tns2 = t2.nseconds;
  ts1 += (tns1/1000000000);
  tns1 = (tns1%1000000000);
  ts2 += (tns2/1000000000);
  tns2 = (tns2%1000000000);
  return (ts1 < ts2 || (ts1 == ts2 && tns1 < tns2));
}

void
server::access_reply_cached(nfscall *nc, int32_t perm, fattr3 fa, bool ok)
{
  if (ok) {
    access3args *a = nc->template getarg<access3args> ();
    fcache *e = fc[a->object];
    if (!e) {
      fcache_insert(a->object,fa);
      e = fc[a->object];
      assert(e);
    }
    e->fa = fa;
    e->users++;
    access3res res(NFS3_OK);
    res.resok->obj_attributes.set_present (true);
    *res.resok->obj_attributes.attributes = fa;
    res.resok->access = perm;
    nc->reply (&res);
  }
  else
    nc->reject (SYSTEM_ERR);
  return;
}

void
server::access_reply (time_t rqtime, nfscall *nc, void *res, clnt_stat err)
{
  ex_access3res *ares = static_cast<ex_access3res *> (res);
  if (!err && ares->status == NFS3_OK) {
    access3args *a = nc->template getarg<access3args> ();
    ex_fattr3 fa = *ares->resok->obj_attributes.attributes;
    fcache *e = fc[a->object];
    // update cache if cache time < mtime and file is not open
    if (fa.type == NF3REG &&
	(!e || (e->fa.mtime < fa.mtime && e->users == 0))) {
      str f = fh2fn(a->object);
      lbfs_read(f, a->object, fa.size, nfsc, authof(nc->getaid()),
	        wrap(mkref(this), &server::cache_file_reply, rqtime, nc, res));
      return;
    }
  }
  getreply(rqtime, nc, res, err);
}

void
server::cache_file_reply (time_t rqtime, nfscall *nc, void *res, bool ok)
{
  if (ok) {
    access3args *a = nc->template getarg<access3args> ();
    ex_access3res *ares = static_cast<ex_access3res *> (res);
    ex_fattr3 *f = ares->resok->obj_attributes.attributes;
    fattr3 fa = *reinterpret_cast<const fattr3 *> (f);
    fcache *e = fc[a->object];
    if (!e) {
      fcache_insert(a->object,fa);
      e = fc[a->object];
      assert(e);
    }
    e->fa = fa;
    e->users++;
    getreply(rqtime, nc, res, RPC_SUCCESS);
  }
  else
    getreply(rqtime, nc, res, RPC_SYSTEMERROR);
}

void
server::read_from_cache(nfscall *nc, fcache *e)
{
  read3args *a = nc->template getarg<read3args> ();
  str fn = fh2fn(e->fh);
  int fd = open (fn, O_RDONLY);
  if (fd < 0) {
    perror("open cache file");
    nc->reject (SYSTEM_ERR);
    return;
  }
  if (lseek(fd, a->offset, SEEK_SET) < 0) {
    perror("lseek in cache file");
    nc->reject (SYSTEM_ERR);
    return;
  }
  char buf[a->count+1];
  int n = read(fd, &buf[0], a->count+1);
  if (n < 0) {
    perror("reading cache file");
    nc->reject (SYSTEM_ERR);
    return;
  }
  close(fd);

  int x = ((unsigned)n) > a->count ? a->count : n;
  read3res res(NFS3_OK);
  res.resok->count = x;
  res.resok->data.setsize(x);
  memcpy(res.resok->data.base(), buf, x);
  res.resok->eof = (((unsigned)n) <= a->count);
  res.resok->file_attributes.set_present (true);
  *res.resok->file_attributes.attributes = e->fa;
  warn << "read from cache, eof " << res.resok->eof << "\n";
  nc->reply(&res);
}

void
server::getreply (time_t rqtime, nfscall *nc, void *res, clnt_stat err)
{
  auto_xdr_delete axd (ex_nfs_program_3.tbl[nc->proc ()].xdr_res, res);
  if (err) {
    if (err == RPC_CANTSEND || err == RPC_CANTRECV)
      getnfscall (nc);
    else
      nc->reject (SYSTEM_ERR);
    return;
  }
  xattrvec xv;
  nfs3_getxattr (&xv, nc->proc (), nc->getvoidarg (), res);
  for (xattr *x = xv.base (); x < xv.lim (); x++) {
    if (x->fattr)
      x->fattr->expire += rqtime;
    ac.attr_enter (*x->fh, x->fattr, x->wattr);

    if (nc->proc () == NFSPROC3_ACCESS) {
      ex_access3res *ares = static_cast<ex_access3res *> (res);
      access3args *a = nc->template getarg<access3args> ();
      if (ares->status)
	ac.flush_access (a->object, nc->getaid ());
      else
	ac.access_enter (a->object, nc->getaid (),
			 a->access, ares->resok->access);
    }
  }
  nfs3_exp_disable (nc->proc (), res);
  nc->reply (res);
}

void
server::cbdispatch (svccb *sbp)
{
  if (!sbp)
    return;

  switch (sbp->proc ()) {
  case ex_NFSCBPROC3_NULL:
    sbp->reply (NULL);
    break;
  case ex_NFSCBPROC3_INVALIDATE:
    {
      ex_invalidate3args *xa = sbp->template getarg<ex_invalidate3args> ();
      ex_fattr3 *a = NULL;
      if (xa->attributes.present && xa->attributes.attributes->expire) {
	a = xa->attributes.attributes.addr ();
	a->expire += timenow;
      }
      ac.attr_enter (xa->handle, a, NULL);
      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
server::flushstate ()
{
  ac.flush_attr ();
  nfsc = NULL;
  nfscbs = NULL;
  super::flushstate ();
}

void
server::authclear (sfs_aid aid)
{
  ac.flush_access (aid);
  super::authclear (aid);
}

void
server::dispatch_dummy (svccb* sbp)
{
  sfsdispatch(sbp);
}

void
server::setfd (int fd)
{
  assert (fd >= 0);
  x = New refcounted<axprt_zcrypt>(fd, axprt_zcrypt::ps());
  sfsc = aclnt::alloc (x, sfs_program_1);
  sfscbs = asrv::alloc (x, sfscb_program_1,
                        wrap (mkref(this), &server::dispatch_dummy));
}

void
server::setrootfh (const sfs_fsinfo *fsi, callback<void, bool>::ref err_cb)
{
  if (fsi->prog != ex_NFS_PROGRAM || fsi->nfs->vers != ex_NFS_V3) {
    err_cb (true);
    return;
  }
  nfs_fh3 fh (fsi->nfs->v3->root);
  if (fsinfo && rootfh.data != fh.data) {
    err_cb (true);
    return;
  }

  rootfh = fh;
  static_cast<axprt_zcrypt *> (x.get ())->compress ();
  nfsc = aclnt::alloc (x, lbfs_program_3);
  nfscbs = asrv::alloc (x, ex_nfscb_program_3,
			wrap (mkref(this), &server::cbdispatch));
  err_cb (false);
}

void
server::dispatch (nfscall *nc)
{
  if (nc->proc() == cl_NFSPROC3_CLOSE) {
    nfs_fh3 *a = nc->template getarg<nfs_fh3> ();
    fcache *e = fc[*a];
    if (e) {
      if (e->users > 0)
	e->users--;
      else
	warn << "dangling close: " << *a << "\n";
    }
    nc->error (NFS3_OK);
    return;
  }

  else if (nc->proc () == NFSPROC3_GETATTR) {
    const ex_fattr3 *f = ac.attr_lookup (*nc->template getarg<nfs_fh3> ());
    if (f) {
      getattr3res res (NFS3_OK);
      *res.attributes = *reinterpret_cast<const fattr3 *> (f);
      nc->reply (&res);
      return;
    }
  }

  else if (nc->proc () == NFSPROC3_ACCESS) {
    access3args *a = nc->template getarg<access3args> ();
    int32_t perm = ac.access_lookup (a->object, nc->getaid (), a->access);
    if (perm > 0) {
      fattr3 fa =
	*reinterpret_cast<const fattr3 *> (ac.attr_lookup (a->object));
      fcache *e = fc[a->object];
      // update cache if cache time < mtime and file is not open
      if (fa.type == NF3REG &&
	  (!e || (e->fa.mtime < fa.mtime && e->users == 0))) {
        str f = fh2fn(a->object);
        lbfs_read
	  (f, a->object, fa.size, nfsc, authof(nc->getaid()),
	   wrap(mkref(this), &server::access_reply_cached, nc, perm, fa));
        return;
      }
      access_reply_cached(nc, perm, fa, true);
      return;
    }
 
    void *res = ex_nfs_program_3.tbl[nc->proc ()].alloc_res ();
    nfsc->call (nc->proc (), nc->getvoidarg (), res,
	        wrap (mkref(this), &server::access_reply, timenow, nc, res),
	        authof (nc->getaid ()));
    return;
  }

  else if (nc->proc () == NFSPROC3_READ) {
    read3args *a = nc->template getarg<read3args> ();
    fcache *e = fc[a->file];
    if (e && e->users > 0) {
      read_from_cache(nc, e);
      return;
    }
    else
      warn << "dangling read: " << a->file << "\n";
  }

  void *res = ex_nfs_program_3.tbl[nc->proc ()].alloc_res ();
  nfsc->call (nc->proc (), nc->getvoidarg (), res,
	      wrap (mkref(this), &server::getreply, timenow, nc, res),
	      authof (nc->getaid ()));
}

void
server::remove_cache (server::fcache e)
{
  str fn = fh2fn(e.fh);
  warn << "remove " << fn << "\n";
  if (unlink(fn.cstr()) < 0)
    perror("removing cache file");
}

