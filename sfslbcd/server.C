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

void
server::access_cached_reply (nfscall *nc, int32_t perm, fattr3 fa,
                             bool update_fa, bool ok)
{
  if (ok) {
    access3args *a = nc->template getarg<access3args> ();
    fcache *e = fc[a->object];
    if (update_fa) {
      if (!e) {
        fcache_insert(a->object,fa);
        e = fc[a->object];
        assert(e);
      }
      e->osize = fa.size;
      e->fa = fa;
    }
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
  if (!err && ares->status == NFS3_OK && ares->resok->obj_attributes.present) {
    access3args *a = nc->template getarg<access3args> ();
    ex_fattr3 *fa = reinterpret_cast<ex_fattr3 *>
      (ares->resok->obj_attributes.attributes.addr ());
    fcache *e = fc[a->object];
    // update file cache if cache time < mtime and file is not dirty
    if (fa->type == NF3REG && (!e || (e->fa.mtime < fa->mtime && !e->dirty))) {
      str f = fh2fn(a->object);
      lbfs_read(f, a->object, fa->size, mkref(this), authof(nc->getaid()),
	        wrap(mkref(this), &server::file_cached, rqtime, nc, res));
      return;
    }
  }
  getreply(rqtime, nc, res, err);
}

void
server::file_cached (time_t rqtime, nfscall *nc, void *res, bool ok)
{
  if (ok) {
    access3args *a = nc->template getarg<access3args> ();
    ex_access3res *ares = static_cast<ex_access3res *> (res);
    ex_fattr3 *f = reinterpret_cast<ex_fattr3 *>
      (ares->resok->obj_attributes.attributes.addr ());
    fattr3 fa = *reinterpret_cast<const fattr3 *> (f);
    fcache *e = fc[a->object];
    if (!e) {
      fcache_insert(a->object,fa);
      e = fc[a->object];
      assert(e);
    }
    e->osize = fa.size;
    e->fa = fa;
    getreply(rqtime, nc, res, RPC_SUCCESS);
  }
  else
    getreply(rqtime, nc, res, RPC_SYSTEMERROR);
}

void
server::read_from_cache (nfscall *nc, fcache *e)
{
  read3args *a = nc->template getarg<read3args> ();
  if (e->fd < 0) {
    str fn = fh2fn(e->fh);
    e->fd = open (fn, O_RDWR);
    if (e->fd < 0) {
      perror("open cache file");
      nc->reject (SYSTEM_ERR);
      return;
    }
  }
  if (lseek(e->fd, a->offset, SEEK_SET) < 0) {
    perror("lseek in cache file");
    nc->reject (SYSTEM_ERR);
    return;
  }
  char buf[a->count+1];
  int n = read(e->fd, &buf[0], a->count+1);
  if (n < 0) {
    perror("reading cache file");
    nc->reject (SYSTEM_ERR);
    return;
  }

  int x = ((unsigned)n) > a->count ? a->count : n;
  read3res res(NFS3_OK);
  res.resok->count = x;
  res.resok->data.setsize(x);
  memcpy(res.resok->data.base(), buf, x);
  res.resok->eof = (((unsigned)n) <= a->count);
  res.resok->file_attributes.set_present (true);
  *res.resok->file_attributes.attributes = e->fa;
  nc->reply(&res);
}

void
server::write_to_cache (nfscall *nc, fcache *e)
{
  write3args *a = nc->template getarg<write3args> ();
  if (e->fd < 0) {
    str fn = fh2fn(e->fh);
    e->fd = open (fn, O_RDWR);
    if (e->fd < 0) {
      perror("open cache file");
      nc->reject (SYSTEM_ERR);
      return;
    }
  }
  if (lseek(e->fd, a->offset, SEEK_SET) < 0) {
    perror("lseek in cache file");
    nc->reject (SYSTEM_ERR);
    return;
  }

  int n = write(e->fd, a->data.base(), a->count);
  if (n < 0) {
    perror("write cache file");
    nc->reject(SYSTEM_ERR);
    return;
  }
  e->dirty = true;

  write3res res(NFS3_OK);
  res.resok->count = n;
  // COMMIT everything on close, so we can return FILE_SYNC here
  res.resok->committed = FILE_SYNC;
  
  // change size, but not mtime, in both attribute and file cache. wcc
  // data reflects file size before and after update.
  res.resok->file_wcc.before.set_present (true);
  (res.resok->file_wcc.before.attributes)->size = e->fa.size;
  (res.resok->file_wcc.before.attributes)->mtime = e->fa.mtime;
  (res.resok->file_wcc.before.attributes)->ctime = e->fa.ctime;
  if (a->offset+n > e->fa.size)
    e->fa.size = a->offset+n;
  ex_fattr3 *f = const_cast<ex_fattr3*>(ac.attr_lookup (e->fh));
  if (f)
    f->size = e->fa.size;
  res.resok->file_wcc.after.set_present (true);
  *(res.resok->file_wcc.after.attributes) = e->fa;
  // res.resok->verf = ;
  nc->reply(&res);
}

int
server::truncate_cache (uint64 size, fcache *e)
{
  if (e->fd < 0) {
    str fn = fh2fn(e->fh);
    e->fd = open (fn, O_RDWR);
    if (e->fd < 0)
      return -1;
  }
  if (ftruncate(e->fd, size) < 0)
    return -1;
 
  // update size in both the attribute cache and file cache
  e->fa.size = size;
  ex_fattr3 *f = const_cast<ex_fattr3*>(ac.attr_lookup (e->fh));
  if (f)
    f->size = e->fa.size;
  e->dirty = true;
  return 0;
}

void
server::close_reply (nfscall *nc, fattr3 fa, bool ok)
{
  if (ok) {
    nfs_fh3 *a = nc->template getarg<nfs_fh3> ();
    fcache *e = fc[*a];
    if (e) {
      if (!e->dirty)
	e->fa = fa;
      // update osize to reflect what the server knows
      e->osize = fa.size;
    }
    nc->error (NFS3_OK);
  }
  else
    nc->reject (SYSTEM_ERR);
}

void
server::flush_cache (nfscall *nc, fcache *e)
{
  // need to set e->dirty to false before calling lbfs_write, so
  // lbfs_write will update the attribute cache when making nfs calls.
  e->dirty = false;
  str fn = fh2fn(e->fh);
  // set size to the size of the file when it was first cached, so wcc
  // checking will work
  fattr3 fa = e->fa;
  fa.size = e->osize;
  lbfs_write
    (fn, e->fh, e->fa.size, fa, mkref(this), authof(nc->getaid()),
     wrap(mkref(this), &server::close_reply, nc));
}

void
server::getxattr (time_t rqtime, unsigned int proc,
                  sfs_aid aid, void *arg, void *res)
{
  nfs_fh3 *fh = static_cast<nfs_fh3 *> (arg);
  fcache *e = fc[*fh];
  if (e && e->dirty) // if file is dirty, don't update attribute cache
    return;

  xattrvec xv;
  nfs3_getxattr (&xv, proc, arg, res);
  for (xattr *x = xv.base (); x < xv.lim (); x++) {
    if (x->fattr)
      x->fattr->expire += rqtime;
    ac.attr_enter (*x->fh, x->fattr, x->wattr);
 
    if (proc == NFSPROC3_ACCESS) {
      ex_access3res *ares = static_cast<ex_access3res *> (res);
      access3args *a = static_cast<access3args *>(arg);
      if (ares->status)
	ac.flush_access (a->object, aid);
      else
	ac.access_enter (a->object, aid, a->access, ares->resok->access);
    }
  }
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
  if (nc->proc () == NFSPROC3_FSINFO) {
    ex_fsinfo3res *fres = static_cast<ex_fsinfo3res *> (res);
    if (!fres->status) {
      rtpref = fres->resok->rtpref;
      wtpref = fres->resok->wtpref;
    }
  }
  else if (nc->proc () == NFSPROC3_SETATTR) {
    setattr3args *a = nc->template getarg<setattr3args> ();
    ex_wccstat3 *sres = static_cast<ex_wccstat3 *> (res);
    fcache *e = fc[a->object];
    if (!sres->status && a->new_attributes.size.set && e &&
	sres->wcc->before.present && sres->wcc->after.present) {
      // does wcc checking. if this is the only client making a change
      // to the object, install the post operation attribute (ignoring
      // the size, as we may have a more up-to-date size) as the
      // attribute for the file cache.
      if ((sres->wcc->before.attributes)->size == e->osize &&
	  (sres->wcc->before.attributes)->mtime == e->fa.mtime &&
	  (sres->wcc->before.attributes)->ctime == e->fa.ctime) {
	ex_fattr3 *f = sres->wcc->after.attributes;
	uint64 s = e->fa.size;
	e->fa = *reinterpret_cast<fattr3 *> (f);
	// update osize to reflect what the server knows
        e->osize = e->fa.size;
	e->fa.size = s;
      }
      else {
        warn << "setattr wcc failed: "
	     << e->osize << ":" << e->fa.mtime.seconds << ":"
	     << e->fa.ctime.seconds << " -- "
	     << (sres->wcc->before.attributes)->size << ":"
	     << (sres->wcc->before.attributes)->mtime.seconds << ":"
	     << (sres->wcc->before.attributes)->ctime.seconds << "\n";
      }
    }
  }

  getxattr (rqtime, nc->proc (), nc->getaid (), nc->getvoidarg (), res);
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
  if (nc->proc () == NFSPROC3_GETATTR) {
    // if file is cached and dirty, return attribute from file cache
    fcache *e = fc[*nc->template getarg<nfs_fh3> ()];
    if (e && e->dirty) {
      warn << "returning attribute from file cache\n";
      getattr3res res (NFS3_OK);
      *res.attributes = e->fa;
      nc->reply (&res);
      return;
    }
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
    fcache *e = fc[a->object];
    if (perm > 0) {
      fattr3 fa =
	*reinterpret_cast<const fattr3 *> (ac.attr_lookup (a->object));
      // update file cache if cache time < mtime and file is not dirty
      if (fa.type == NF3REG && (!e || (e->fa.mtime < fa.mtime && !e->dirty))) {
        str f = fh2fn(a->object);
        lbfs_read
	  (f, a->object, fa.size, mkref(this), authof(nc->getaid()),
	   wrap(mkref(this), &server::access_cached_reply, nc, perm, fa, true));
        return;
      }
      access_cached_reply (nc, perm, fa, false, true);
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
    if (e) {
      read_from_cache (nc, e);
      return;
    }
    else
      warn << "dangling read: " << a->file << "\n";
  }

  else if (nc->proc() == cl_NFSPROC3_CLOSE) {
    nfs_fh3 *a = nc->template getarg<nfs_fh3> ();
    fcache *e = fc[*a];
    if (e && e->fd >= 0) {
      close(e->fd);
      e->fd = -1;
    }
    if (e && e->dirty)
      flush_cache (nc, e);
    else {
      if (!e)
	warn << "dangling close: " << *a << "\n";
      nc->error (NFS3_OK);
    }
    return;
  }

  else if (nc->proc () == NFSPROC3_WRITE) {
    write3args *a = nc->template getarg<write3args> ();
    fcache *e = fc[a->file];
    if (e) {
      write_to_cache (nc, e);
      return;
    }
    else
      warn << "dangling write: " << a->file << "\n";
  }

  else if (nc->proc () == NFSPROC3_SETATTR) {
    // if size is given, truncate cache file to the appropriate size
    // and mark it dirty. update size, but not mtime. forward RPC to
    // server always since we won't forward other attributes to server
    // on CLOSE.
    setattr3args *a = nc->template getarg<setattr3args> ();
    fcache *e = fc[a->object];
    if (a->new_attributes.size.set && e) {
      if (truncate_cache (*(a->new_attributes.size.val), e) < 0) {
        nc->reject (SYSTEM_ERR);
	return;
      }
    }
  }
  
  else if (nc->proc () == NFSPROC3_COMMIT) {
    // because all writes to cache files are returned with FILE_SYNC,
    // we should only see dangling commits that did not follow an
    // ACCESS rpc.
    warn << "sfslbcd sees COMMIT, forward to server\n";
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

