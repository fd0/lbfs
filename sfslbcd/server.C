/*
 *
 * Copyright (C) 2002 Benjie Chen (benjie@lcs.mit.edu)
 * Copyright (C) 1998-2000 David Mazieres (dm@uun.org)
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

// implementation description is in "notes"

#define warn_debug  if (0) warn
#define warn_lookup if (0) warn

#include <typeinfo>
#include "sfslbcd.h"
#include "axprt_crypt.h"
#include "axprt_compress.h"
#include "lbfs_prot.h"
#include "ranges.h"

void
server::check_cache (nfs_fh3 obj, fattr3 fa, sfs_aid aid)
{
  file_cache *e = file_cache_lookup(obj);
  if (fa.type == NF3REG) {
    // update file cache if cache time < mtime and file is not dirty
    // or fetch
    if (!e || ((e->fa.mtime < fa.mtime) && e->is_idle())) {
      if (e) {
	warn << "re-fetch file " << e->fh << "\n";
      }
      else {
	warn_debug << "fetch file " << e->fh << "\n";
      }
      if (!e) {
        file_cache_insert (obj);
        e = file_cache_lookup(obj);
        assert(e);
      }
      e->fa = fa;
      e->osize = fa.size;
      str f = fh2fn (obj);
      lbfs_read (f, obj, fa.size, mkref(this), authof(aid),
	         wrap(mkref(this), &server::file_cached, e));
    }
    // if file is in sync with server, sync attribute in file cache
    // with that in attribute cache.
    else if (e && e->is_idle()) {
      e->fa = fa;
      e->osize = fa.size;
    }
  }
  else if (fa.type == NF3DIR) {
    dir_cache **dp = dc[obj];
    if (dp) {
      if ((*dp)->attr.mtime < fa.mtime) {
	warn_lookup << "clear lookup cache " << obj << "\n";
	nlc_remove(obj);
	lc_remove(obj);
      }
    }
  }
}

void
server::access_reply (time_t rqtime, nfscall *nc, void *res, clnt_stat err)
{
  // MUST call check_cache before getreply: check_cache may invalidate
  // a directory's lookup/negative lookup cache if cached mtime is
  // less than the mtime returned from server. but in getreply we set
  // cache mtime to that returned from server. the correctness of the
  // lookup and negative lookup cache depends on the client calling
  // ACCESS before each lookup in directory if it hasn't done a lookup
  // in awhile.

  ex_access3res *ares = static_cast<ex_access3res *> (res);
  if (!err && ares->status == NFS3_OK && ares->resok->obj_attributes.present) {
    access3args *a = nc->template getarg<access3args> ();
    ex_fattr3 *f = reinterpret_cast<ex_fattr3 *>
      (ares->resok->obj_attributes.attributes.addr ());
    fattr3 fa = *reinterpret_cast<const fattr3 *> (f);
    check_cache (a->object, fa, nc->getaid());
  }
  getreply(rqtime, nc, res, err);
}

void
server::file_cached (file_cache *e, bool done, bool ok)
{
  if (done) {
    if (ok)
      e->idle();
    else
      e->error();
  }
  vec<nfscall *> rpcs;
  for (unsigned i=0; i<e->rpcs.size(); i++)
    rpcs.push_back(e->rpcs[i]);
  e->rpcs.clear();
  for (unsigned i=0; i<rpcs.size(); i++)
    dispatch(rpcs[i]);
}

void
server::read_from_cache (nfscall *nc, file_cache *e)
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
server::write_to_cache (nfscall *nc, file_cache *e)
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
  assert(e->is_idle() || e->is_dirty());
  e->dirty();

  write3res res(NFS3_OK);
  res.resok->count = n;
  // we COMMIT everything on close, so we can return FILE_SYNC here
  res.resok->committed = FILE_SYNC;
  
  res.resok->file_wcc.before.set_present (true);
  (res.resok->file_wcc.before.attributes)->size = e->fa.size;
  (res.resok->file_wcc.before.attributes)->mtime = e->fa.mtime;
  (res.resok->file_wcc.before.attributes)->ctime = e->fa.ctime;
  if (a->offset+n > e->fa.size)
    e->fa.size = a->offset+n;
  res.resok->file_wcc.after.set_present (true);
  *(res.resok->file_wcc.after.attributes) = e->fa;
  // res.resok->verf = ;
  nc->reply(&res);
}

int
server::truncate_cache (uint64 size, file_cache *e)
{
  if (e->fd < 0) {
    str fn = fh2fn(e->fh);
    e->fd = open (fn, O_RDWR);
    if (e->fd < 0)
      return -1;
  }
  if (ftruncate(e->fd, size) < 0)
    return -1;
 
  e->fa.size = size;
  assert(e->is_idle() || e->is_dirty());
  e->dirty();
  return 0;
}

void
server::close_done (nfscall *nc, nfs_fh3 fh, fattr3 fa, bool ok)
{
  file_cache *e = file_cache_lookup(fh);
  assert(e && e->is_flush());
  if (ok) {
    e->fa = fa;
    // update osize to reflect what the server knows
    e->osize = fa.size;
    e->idle();
    nc->error (NFS3_OK);
  }
  else {
    e->error();
    nc->reject (SYSTEM_ERR);
  }
  vec<nfscall *> rpcs;
  for (unsigned i=0; i<e->rpcs.size(); i++)
    rpcs.push_back(e->rpcs[i]);
  e->rpcs.clear();
  for (unsigned i=0; i<rpcs.size(); i++)
    dispatch(rpcs[i]);
}

void
server::flush_cache (nfscall *nc, file_cache *e)
{
  sfs_aid aid = nc->getaid();
  // mark e as being flushed. after lbfs_write finishes we will update
  // the attribute cache.
  assert(e->is_dirty());
  e->flush();
  str fn = fh2fn(e->fh);
  // set size to the size of the file when it was first cached, so wcc
  // checking will work
  fattr3 fa = e->fa;
  fa.size = e->osize;
  lbfs_write
    (fn, e->fh, e->fa.size, fa, mkref(this), authof(aid),
     wrap(mkref(this), &server::close_done, nc, e->fh));
}

void
server::getxattr (time_t rqtime, unsigned int proc,
                  sfs_aid aid, void *arg, void *res)
{
  xattrvec xv;
  nfs3_getxattr (&xv, proc, arg, res);
  for (xattr *x = xv.base (); x < xv.lim (); x++) {
    if (x->fattr)
      x->fattr->expire += rqtime;
    ac.attr_enter (*x->fh, x->fattr, x->wattr);
    if (x->fattr && x->fattr->type == NF3DIR) {
      dir_cache **dp = dc[*x->fh];
      if (!dp) {
        dir_cache *d = New dir_cache;
	d->attr = *x->fattr;
	dc.insert(*x->fh, d);
      }
      else
	(*dp)->attr = *x->fattr;
    }
 
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

  getxattr (rqtime, nc->proc (), nc->getaid (), nc->getvoidarg (), res);

  if (nc->proc () == NFSPROC3_FSINFO) {
    ex_fsinfo3res *fres = static_cast<ex_fsinfo3res *> (res);
    if (!fres->status) {
      rtpref = fres->resok->rtpref;
      wtpref = fres->resok->wtpref;
    }
  }

  // if file is dirty or being flushed, replace the size attributed
  // returned from server with up-to-date size from the file cache.
  // in case of SETATTR, we want to update the cached file's attribute
  // with attribute from server, ignoring the server's size and mtime
  // values.
  //
  // also intercept RPCs that may affect the lookup cache and the
  // negative lookup cache.

  if (nc->proc () == NFSPROC3_ACCESS) {
    access3args *a = nc->template getarg<access3args> ();
    ex_access3res *r = static_cast<ex_access3res *> (res);
    file_cache *e = file_cache_lookup(a->object);
    if (!r->status) {
      if (e && (e->is_dirty() || e->is_flush()) &&
	  e->fa.size != (r->resok->obj_attributes.attributes)->size)
	(r->resok->obj_attributes.attributes)->size = e->fa.size;
    }
  }

  else if (nc->proc () == NFSPROC3_LOOKUP) {
    diropargs3 *a = nc->template getarg<diropargs3> ();
    ex_lookup3res *r = static_cast<ex_lookup3res *> (res);
    file_cache *e = 0;
    if (!r->status && (e = file_cache_lookup(r->resok->object))) {
      if ((e->is_dirty() || e->is_flush()) &&
	  e->fa.size != (r->resok->obj_attributes.attributes)->size)
	(r->resok->obj_attributes.attributes)->size = e->fa.size;
    }
    if (r->status == NFS3ERR_NOENT) {
      nlc_insert(a->dir, a->name);
      warn_lookup << "- lc: " << a->name << "\n";
      lc_remove(a->dir, a->name);
    }
    else if (!r->status) {
      nlc_remove(a->dir, a->name);
      warn_lookup << "+ lc: " << a->name << "\n";
      lc_insert(a->dir, a->name, r->resok->object);
    }
  }

  else if (nc->proc () == NFSPROC3_READDIR) {
    readdir3args *a = nc->template getarg<readdir3args> ();
    ex_readdir3res *r = static_cast<ex_readdir3res *> (res);
    if (r->status) {
      nlc_remove(a->dir);
      lc_remove(a->dir);
    }
    else {
      for (entry3 *e = r->resok->reply.entries; e; e = e->nextentry)
	nlc_remove(a->dir, e->name);
    }
  }

  else if (nc->proc () == NFSPROC3_SETATTR) {
    // on each SETATTR, copy attributes from server to file cache, but
    // don't override the size and mtime fields if file is dirty or
    // being flushed. we also perform wcc checking when file is not
    // being flushed. the goal of doing this wcc checking is to end up
    // with the same attribute as the server, so that on the next open
    // we can avoid a fetch if only one client modified the file.
    setattr3args *a = nc->template getarg<setattr3args> ();
    ex_wccstat3 *sres = static_cast<ex_wccstat3 *> (res);
    file_cache *e = file_cache_lookup(a->object);
    if (!sres->status && e && sres->wcc->after.present) {
      if (e->is_flush()) {
	ex_fattr3 *f = sres->wcc->after.attributes;
	uint64 s = e->fa.size;
	nfstime3 m = e->fa.mtime;
	e->fa = *reinterpret_cast<fattr3 *> (f);
	e->fa.size = s;
	e->fa.mtime = m;
      }
      else if (sres->wcc->before.present) {
        if ((sres->wcc->before.attributes)->size == e->osize &&
	    (sres->wcc->before.attributes)->mtime == e->fa.mtime) {
	  ex_fattr3 *f = sres->wcc->after.attributes;
	  uint64 s = e->fa.size;
	  e->fa = *reinterpret_cast<fattr3 *> (f);
	  // update osize to reflect what the server knows
          e->osize = e->fa.size;
	  e->fa.size = s;
        }
        else {
          warn << "setattr wcc failed: "
	       << e->osize << ":" << e->fa.mtime.seconds << " -- "
	       << (sres->wcc->before.attributes)->size << ":"
	       << (sres->wcc->before.attributes)->mtime.seconds << "\n";
        }
      }
      // replace size attribute w/ up-to-date size from file cache
      if (e->fa.size != (sres->wcc->after.attributes)->size) {
	assert(e->is_dirty() || e->is_flush());
	(sres->wcc->after.attributes)->size = e->fa.size;
      }
    }
  }

  if (nc->proc () == NFSPROC3_CREATE  || nc->proc () == NFSPROC3_MKDIR ||
      nc->proc () == NFSPROC3_SYMLINK || nc->proc () == NFSPROC3_RENAME ||
      nc->proc () == NFSPROC3_LINK    || nc->proc () == NFSPROC3_REMOVE ||    
      nc->proc () == NFSPROC3_RMDIR)
  {
    if (nc->proc() == NFSPROC3_CREATE) {
      create3args *a = nc->template getarg<create3args> ();
      nlc_remove(a->where.dir, a->where.name);
      warn_lookup << "- nlc: " << a->where.name << "\n";
      ex_diropres3 *r = static_cast<ex_diropres3 *> (res);
      if (!r->status && r->resok->obj.present) {
	lc_insert(a->where.dir, a->where.name, *(r->resok->obj.handle));
	warn_lookup << "+ lc: " << a->where.name << "\n";
      }
    }
    else if (nc->proc() == NFSPROC3_MKDIR) {
      mkdir3args *a = nc->template getarg<mkdir3args> ();
      nlc_remove(a->where.dir, a->where.name);
      warn_lookup << "- nlc: " << a->where.name << "\n";
      ex_diropres3 *r = static_cast<ex_diropres3 *> (res);
      if (!r->status && r->resok->obj.present) {
	lc_insert(a->where.dir, a->where.name, *(r->resok->obj.handle));
	warn_lookup << "+ lc: " << a->where.name << "\n";
      }
    }
    else if (nc->proc() == NFSPROC3_SYMLINK) {
      symlink3args *a = nc->template getarg<symlink3args> ();
      nlc_remove(a->where.dir, a->where.name);
      warn_lookup << "- nlc: " << a->where.name << "\n";
      ex_diropres3 *r = static_cast<ex_diropres3 *> (res);
      if (!r->status && r->resok->obj.present) {
	lc_insert(a->where.dir, a->where.name, *(r->resok->obj.handle));
	warn_lookup << "+ lc: " << a->where.name << "\n";
      }
    }
    else if (nc->proc() == NFSPROC3_RENAME) {
      rename3args *a = nc->template getarg<rename3args> ();
      nlc_remove(a->to.dir, a->to.name);
      lc_remove(a->to.dir, a->to.name);
      lc_remove(a->from.dir, a->from.name);
      warn_lookup << "- nlc: " << a->to.name << "\n";
      warn_lookup << "- lc: " << a->to.name << "\n";
      warn_lookup << "- lc: " << a->from.name << "\n";
    }
    else if (nc->proc() == NFSPROC3_LINK) {
      link3args *a = nc->template getarg<link3args> ();
      nlc_remove(a->link.dir, a->link.name);
      warn_lookup << "- nlc: " << a->link.name << "\n";
    }
    else if (nc->proc() == NFSPROC3_REMOVE || nc->proc() == NFSPROC3_RMDIR) {
      diropargs3 *a = nc->template getarg<diropargs3> ();
      lc_remove(a->dir, a->name);
      warn_lookup << "- lc: " << a->name << "\n";
      ex_wccstat3 *r = static_cast<ex_wccstat3 *> (res);
      if (!r->status) {
	nlc_insert(a->dir, a->name);
        warn_lookup << "+ nlc: " << a->name << "\n";
      }
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

      // if directory has been invalidated, clear both lookup caches
      if (dc[xa->handle]) {
        warn_lookup << "invalidate lookup cache for " << xa->handle << "\n";
        nlc_remove(xa->handle);
        lc_remove(xa->handle);
      }
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

  if (try_compress) {
    static_cast<axprt_zcrypt *> (x.get ())->compress ();
    try_compress = false;
  }
  nfsc = aclnt::alloc (x, lbfs_program_3);
  nfscbs = asrv::alloc (x, ex_nfscb_program_3,
			wrap (mkref(this), &server::cbdispatch));
  err_cb (false);
}

bool
server::dont_run_rpc (nfscall *nc)
{
  if (nc->proc () != NFSPROC3_SETATTR &&
      nc->proc () != NFSPROC3_READ &&
      nc->proc () != NFSPROC3_WRITE &&
      nc->proc () != cl_NFSPROC3_CLOSE)
    return false;

  nfs_fh3 *fh = static_cast<nfs_fh3 *> (nc->getvoidarg ());
  file_cache *e = file_cache_lookup(*fh);

  if (nc->proc () == cl_NFSPROC3_CLOSE && (e->is_idle() || e->is_fetch()))
    return false;

  if (e) {
    if (e->is_flush() && nc->proc () != NFSPROC3_READ &&
	nc->proc () != cl_NFSPROC3_CLOSE) {
      // block WRITE and SETATTR RPCs that set size of file
      if (nc->proc () == NFSPROC3_SETATTR) {
        setattr3args *a = nc->template getarg<setattr3args> ();
        if (!a->new_attributes.size.set)
	  return false;
      }
      warn_debug << "RPC " << nc->proc () << " blocked due to flush\n";
      e->rpcs.push_back(nc);
      return true;
    }
    // if file is being fetched, can execute READ if range requested
    // in the RPC has already been fetched, other READ RPCs and all
    // WRITE and SETATTR RPCs must be queued
    else if (e->is_fetch()) {
      if (nc->proc () == NFSPROC3_READ) {
        read3args *a = nc->template getarg<read3args> ();
	uint64 offset = a->offset;
	uint64 size = a->count;
	if (e->received(offset, size))
	  return false;
        warn_debug << "RPC " << nc->proc () << " blocked: "
	           << offset << ":" << size << "\n";
	if (nc->proc() == NFSPROC3_READ)
	  // *32 is an optimization: if client requests a read at
	  // offset N, it's likely to want blocks from N to end of
	  // file before from 0 to N
	  e->want(offset, size*32, rtpref);
      }
      else {
	warn_debug << "RPC " << nc->proc () << " blocked\n";
      }
      e->rpcs.push_back(nc);
      return true;
    }
    else if (e->is_error()) {
      nc->reject (SYSTEM_ERR);
      return true;
    }
  }
  return false; 
}

void
server::dispatch (nfscall *nc)
{
  if (nc->proc () == NFSPROC3_GETATTR) {
    // if file is cached, and is dirty or being flushed, return
    // attribute from file cache
    file_cache *e = file_cache_lookup(*nc->template getarg<nfs_fh3> ());
    if (e && (e->is_dirty() || e->is_flush())) {
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

  if (dont_run_rpc (nc))
    return;

  if (nc->proc () == NFSPROC3_ACCESS) {
    access3args *a = nc->template getarg<access3args> ();
    int32_t perm = ac.access_lookup (a->object, nc->getaid (), a->access);
    if (perm > 0) {
      fattr3 fa =
	*reinterpret_cast<const fattr3 *> (ac.attr_lookup (a->object));
      check_cache (a->object, fa, nc->getaid());
      access3res res(NFS3_OK);
      res.resok->obj_attributes.set_present (true);
      *res.resok->obj_attributes.attributes = fa;
      res.resok->access = perm;
      nc->reply (&res);
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
    file_cache *e = file_cache_lookup(a->file);
    if (e) {
      read_from_cache (nc, e);
      return;
    }
    else
      warn << "dangling read: " << a->file << "\n";
  }

  else if (nc->proc() == cl_NFSPROC3_CLOSE) {
    nfs_fh3 *a = nc->template getarg<nfs_fh3> ();
    file_cache *e = file_cache_lookup(*a);
    if (e && e->fd >= 0) {
      close(e->fd);
      e->fd = -1;
    }
    if (e && e->is_flush()) // someone else is closing this file
      nc->error (NFS3_OK);
    else if (e && e->is_dirty())
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
    file_cache *e = file_cache_lookup(a->file);
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
    file_cache *e = file_cache_lookup(a->object);
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

  else if (nc->proc () == NFSPROC3_LOOKUP) {
    diropargs3 *a = nc->template getarg<diropargs3> ();
    bool has_negative = nlc_lookup(a->dir, a->name);
    if (has_negative) {
      const ex_fattr3 *f = ac.attr_lookup (a->dir);
      if (f) {
        lookup3res res(NFS3ERR_NOENT);
        res.resfail->set_present (true);
        *res.resfail->attributes = *reinterpret_cast<const fattr3 *> (f);
        nc->reply (&res);
	return;
      }
    }
    nfs_fh3 fh;
    bool has_positive = lc_lookup(a->dir, a->name, fh);
    if (has_positive) {
      lookup3res res(NFS3_OK);
      res.resok->object = fh;
      const ex_fattr3 *f = ac.attr_lookup (a->dir);
      if (f) {
        res.resok->dir_attributes.set_present (true);
        *(res.resok->dir_attributes.attributes) =
	  *reinterpret_cast<const fattr3 *> (f);
      }
      else
        res.resok->dir_attributes.set_present (false);
      f = ac.attr_lookup (fh);
      if (f) {
        res.resok->obj_attributes.set_present (true);
        *(res.resok->obj_attributes.attributes) =
	  *reinterpret_cast<const fattr3 *> (f);
      }
      else
        res.resok->obj_attributes.set_present (false);
      nc->reply (&res);
      return;
    }
    warn_lookup << "lookup " << a->name << ", " << a->dir << "\n";
  }

  void *res = ex_nfs_program_3.tbl[nc->proc ()].alloc_res ();
  nfsc->call (nc->proc (), nc->getvoidarg (), res,
	      wrap (mkref(this), &server::getreply, timenow, nc, res),
	      authof (nc->getaid ()));
}

void
server::file_cache_gc_remove (file_cache *e)
{
  str fn = fh2fn(e->fh);
  warn << "remove " << fn << "\n";
  if (unlink(fn.cstr()) < 0)
    perror("removing cache file");
  delete e;
}

void
server::dir_cache_gc_remove (dir_cache *d)
{
  delete d;
}

