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

#include <typeinfo>
#include "sfslbcd.h"
#include "axprt_crypt.h"
#include "axprt_compress.h"
#include "lbfs_prot.h"
#include "ranges.h"

aiod* file_cache::a = New aiod (2);

void
server::check_cache (nfs_fh3 obj, fattr3 fa, sfs_aid aid)
{
  file_cache *e = file_cache_lookup(obj);
  assert(e && e->is_open());
  warn_debug << "checking cache for " << obj <<  "\n";
  if (fa.type == NF3REG) {
    // update file cache if cache time != mtime
    if (e->fa.mtime < fa.mtime ||
	fa.mtime < e->fa.mtime) {
      e->fa = fa;
      e->osize = fa.size;
      str f = fh2fn (obj);
      e->fetch(fa.size);
      warn_debug << "fetch cache file " << obj << "\n";
      lbfs_read (f, obj, fa.size, mkref(this), authof(aid),
	         wrap(mkref(this), &server::fetch_done, e));
    }
    else {
      e->fa = fa;
      e->osize = fa.size;
      warn_debug << "don't need to fetch " << obj << "\n";
      warn_debug << "mtime " << e->fa.mtime.seconds 
	         << ", " << fa.mtime.seconds << "\n";
      e->idle();
    }
  }
}

void
server::access_reply (time_t rqtime, nfscall *nc, void *res, clnt_stat err)
{
  ex_access3res *ares = static_cast<ex_access3res *> (res);
  if (!err && ares->status == NFS3_OK && ares->resok->obj_attributes.present) {
    access3args *a = nc->template getarg<access3args> ();
    ex_fattr3 *f = reinterpret_cast<ex_fattr3 *>
      (ares->resok->obj_attributes.attributes.addr ());
    fattr3 fa = *reinterpret_cast<const fattr3 *> (f);
    if (fa.type == NF3REG) {
      file_cache *e = file_cache_lookup(a->object);
      if (!e) {
        file_cache_insert (a->object);
        e = file_cache_lookup(a->object);
        assert(e);
	e->fa.mtime.seconds = 0;
	e->fa.mtime.nseconds = 0;
        e->open();
      }
      else if (e->is_idle())
        e->open();
    }
  }
  getreply(rqtime, nc, res, err);
}

void
server::fetch_done (file_cache *e, bool done, bool ok)
{
  assert(e->is_fetch());
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
  if (e->afh == 0) {
    str fn = fh2fn(e->fh);
    file_cache::a->open (fn, O_RDWR, 0666,
                         wrap (this, &server::read_from_cache_open, nc, e));
  }
  else
    read_from_cache_open (nc, e, e->afh, 0);
}

void
server::read_from_cache_open (nfscall *nc, file_cache *e,
                              ptr<aiofh> afh, int err)
{
  if (!afh) {
    warn << "read_from_cache: open failed: " << err << "\n";
    nc->reject (SYSTEM_ERR);
    return;
  }
  e->afh = afh;

  read3args *a = nc->template getarg<read3args> ();
  ptr<aiobuf> buf = file_cache::a->bufalloc (a->count+1);
  if (!buf) {
    file_cache::a->bufwait
      (wrap (this, &server::read_from_cache_open, nc, e, e->afh, 0));
    return;
  }

  e->afh->read (a->offset, buf,
                wrap (this, &server::read_from_cache_read, nc, e));
}

void
server::read_from_cache_read (nfscall *nc, file_cache *e,
                              ptr<aiobuf> buf, ssize_t sz, int err)
{
  if (err) {
    warn << "read_from_cache: read failed: " << err << "\n";
    nc->reject (SYSTEM_ERR);
  }

  read3args *a = nc->template getarg<read3args> ();
  int x = ((unsigned)sz) > a->count ? a->count : sz;
  read3res res(NFS3_OK);
  res.resok->count = x;
  res.resok->data.setsize(x);
  memcpy(res.resok->data.base(), buf->base(), x);
  res.resok->eof = (((unsigned)sz) <= a->count);
  res.resok->file_attributes.set_present (true);
  *res.resok->file_attributes.attributes = e->fa;
  nc->reply (&res);
}

void 
server::write_to_cache (nfscall *sbp, file_cache *e)
{   
  e->outstanding_op ();
  
  if (e->afh == 0) {
    str fn = fh2fn(e->fh);
    file_cache::a->open (fn, O_RDWR, 0666,
                         wrap (this, &server::write_to_cache_open, sbp, e));
  }
  else
    write_to_cache_open (sbp, e, e->afh, 0);
}   

void
server::write_to_cache_open (nfscall *sbp, file_cache *e,
                             ptr<aiofh> afh, int err)
{ 
  if (!afh) {
    warn << "write_to_cache: open failed: " << err << "\n";
    e->error ();
    sbp->reject (SYSTEM_ERR);
    run_rpcs (e);
    return;
  }
  
  write3args *a = sbp->template getarg<write3args> (); 
    
  e->afh = afh;
  assert (e->is_idle() || e->is_dirty());
  
  ptr<aiobuf> buf = file_cache::a->bufalloc (a->count);
  if (!buf) {
    file_cache::a->bufwait
      (wrap (this, &server::write_to_cache_open, sbp, e, e->afh, 0));
    return;
  }
  
  memmove(buf->base (), a->data.base (), a->count);
  e->afh->write (a->offset, buf,
                 wrap (this, &server::write_to_cache_write, sbp, e));
}

void
server::write_to_cache_write (nfscall *sbp, file_cache *e,
                              ptr<aiobuf> buf, ssize_t sz, int err)
{
  write3args *a = sbp->template getarg<write3args> ();
  e->outstanding_op_done ();

  if (err || (unsigned)sz != a->count) {
    warn << "write_to_cache: write failed: " << err << "\n";
    e->error ();
    sbp->reject (SYSTEM_ERR);
  }
  else {
    e->dirty ();
    if (a->offset+sz > e->fa.size)
      e->fa.size = a->offset+sz;

    // XXX if stable, flush data

    // mark region as modified
    if (e->mstart == 0 && e->mend == 0) {
      e->mstart = a->offset;
      e->mend = a->offset + a->count;
    }
    else {
      if (e->mstart > a->offset)
        e->mstart = a->offset;
      if (e->mend < a->offset + a->count)
        e->mend = a->offset + a->count;
    }

    write3res res(NFS3_OK);
    res.resok->count = a->count;
    res.resok->committed = FILE_SYNC; // XXX
  
    res.resok->file_wcc.before.set_present (true);
    (res.resok->file_wcc.before.attributes)->size = e->fa.size;
    (res.resok->file_wcc.before.attributes)->mtime = e->fa.mtime;
    (res.resok->file_wcc.before.attributes)->ctime = e->fa.ctime;
    res.resok->file_wcc.after.set_present (true);
    *(res.resok->file_wcc.after.attributes) = e->fa;
    res.resok->verf = verf3;
    sbp->reply (&res);
  }

  // check if there are any CLOSE or COMMIT that we blocked
  run_rpcs (e);
}

void
server::truncate_cache (nfscall *sbp, file_cache *e, uint64 size)
{
  if (e->afh == 0) {
    str fn = fh2fn(e->fh);
    file_cache::a->open (fn, O_RDWR | O_CREAT, 0666,
                         wrap (this, &server::truncate_cache_open,
			       sbp, e, size));
  }
  else
    truncate_cache_open (sbp, e, size, e->afh, 0);
}

void
server::truncate_cache_open (nfscall *sbp, file_cache *e, uint64 size,
                             ptr<aiofh> afh, int err)
{
  if (!afh) {
    warn << "truncate_cache: open failed: " << err << "\n";
    sbp->reject (SYSTEM_ERR);
    return;
  }
  e->afh = afh;
  assert (e->is_dirty() || e->is_idle() || e->is_open ());

  e->afh->ftrunc (size,
                  wrap (this, &server::truncate_cache_truncate, sbp, e, size));
}

void
server::dispatch_to_server (nfscall *nc)
{
  void *res = ex_nfs_program_3.tbl[nc->proc ()].alloc_res ();
  nfsc->call (nc->proc (), nc->getvoidarg (), res,
              wrap (mkref(this), &server::getreply, timenow, nc, res),
	            authof (nc->getaid ()));
}

void
server::truncate_cache_truncate (nfscall *nc, file_cache *e, uint64 size,
                                 int err)
{
  if (err) {
    warn << "truncate_cache: truncate failed: " << err << "\n";
    nc->reject (SYSTEM_ERR);
    return;
  }
  e->fa.size = size;
  dispatch_to_server (nc);
}

void
server::flush_done (nfscall *nc, nfs_fh3 fh, fattr3 fa, bool ok)
{
  file_cache *e = file_cache_lookup(fh);
  assert(e && e->is_flush());
  if (ok) {
    e->fa = fa;
    // update osize to reflect what the server knows
    e->osize = fa.size;
    if (e->mstart == e->mend && e->mstart == 0) {
      if (nc->proc () == cl_NFSPROC3_CLOSE)
        e->open ();
      else
        e->idle ();
    }
    else
      e->dirty ();
    if (nc->proc () == NFSPROC3_COMMIT) {
      commit3res res (NFS3_OK);
      res.resok->verf = verf3;
      nc->reply (&res);
    }
    else
      nc->error (NFS3_OK);
  }
  else {
    e->error();
    nc->reject (SYSTEM_ERR);
  }
  run_rpcs (e);
}

void
server::run_rpcs (file_cache *e)
{
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

  uint64 start = e->mstart;
  uint64 size = e->mend - e->mstart;

  if (nc->proc () == NFSPROC3_COMMIT) {
    commit3args *a = nc->template getarg<commit3args> ();
    start = a->offset;
    size = a->count;
  }

  if (start <= e->mstart && (start+size) > e->mstart)
    e->mstart = start + size;
  if (start + size >= e->mend)
    e->mend = start;
  if (e->mstart >= e->mend) {
    e->mstart = 0;
    e->mend = 0;
  }

  lbfs_write
    (fn, e, e->fh, start, size, fa, mkref(this), authof(aid),
     wrap(mkref(this), &server::flush_done, nc, e->fh));
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

  fixlc (nc, res);
  getxattr (rqtime, nc->proc (), nc->getaid (), nc->getvoidarg (), res);
  
  // if file is dirty or being flushed, replace the size attributed
  // returned from server with up-to-date size from the file cache.

  if (nc->proc () == NFSPROC3_FSINFO) {
    ex_fsinfo3res *fres = static_cast<ex_fsinfo3res *> (res);
    if (!fres->status) {
      rtpref = fres->resok->rtpref;
      wtpref = fres->resok->wtpref;
    }
  }
  else if (nc->proc () == NFSPROC3_ACCESS) {
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
    ex_lookup3res *r = static_cast<ex_lookup3res *> (res);
    file_cache *e = 0;
    if (!r->status && (e = file_cache_lookup(r->resok->object))) {
      if ((e->is_dirty() || e->is_flush()) &&
	  e->fa.size != (r->resok->obj_attributes.attributes)->size)
	(r->resok->obj_attributes.attributes)->size = e->fa.size;
    }
  }
  else if (nc->proc () == NFSPROC3_SETATTR) {
    // on SETATTR when file is dirty: copy attributes from server to
    // file cache, but don't override the size and mtime fields. do a
    // wcc checking to avoid re-fetching this file later if only one
    // client is writing it.
    setattr3args *a = nc->template getarg<setattr3args> ();
    ex_wccstat3 *sres = static_cast<ex_wccstat3 *> (res);
    file_cache *e = file_cache_lookup(a->object);
    if (e)
      assert(!e->is_flush());
    if (!sres->status && e && sres->wcc->after.present) {
      if (sres->wcc->before.present) {
        if ((sres->wcc->before.attributes)->size == e->osize &&
	    (sres->wcc->before.attributes)->mtime == e->fa.mtime) {
	  ex_fattr3 *f = sres->wcc->after.attributes;
	  uint64 s = e->fa.size;
	  e->fa = *reinterpret_cast<fattr3 *> (f);
          e->osize = e->fa.size;
	  e->fa.size = s;
        }
        else {
	  // wcc checking failed, don't update mtime (will have to
	  // re-fetch the file on next open)
	  ex_fattr3 *f = sres->wcc->after.attributes;
	  nfstime3 m = e->fa.mtime;
	  uint64 s = e->fa.size;
	  e->fa = *reinterpret_cast<fattr3 *> (f);
	  e->fa.size = s;
	  e->fa.mtime = m;
        }
      }
      // if file is dirty or being flushed: replace size attribute w/
      // up-to-date size from file cache
      if (e->fa.size != (sres->wcc->after.attributes)->size) {
	if (e->is_dirty())
	  (sres->wcc->after.attributes)->size = e->fa.size;
      }
    }
  }

  nfs3_exp_disable (nc->proc (), res);
  nc->reply (res);
}

void
server::fixlc (nfscall *nc, void *res)
{
  // create lc entry and update dir mtime if we see an ACCESS or
  // READDIR on the dir or modifying the dir.
  if (nc->proc () == NFSPROC3_CREATE  || nc->proc () == NFSPROC3_MKDIR ||
      nc->proc () == NFSPROC3_SYMLINK || nc->proc () == NFSPROC3_RENAME ||
      nc->proc () == NFSPROC3_LINK    || nc->proc () == NFSPROC3_REMOVE ||    
      nc->proc () == NFSPROC3_RMDIR   || nc->proc () == NFSPROC3_ACCESS ||
      nc->proc () == NFSPROC3_READDIR) {
    xattrvec xv;
    nfs3_getxattr (&xv, nc->proc (), nc->getvoidarg (), res);
    for (xattr *x = xv.base (); x < xv.lim (); x++) {
      if (x->fattr && x->fattr->type == NF3DIR) {
        dir_lc **dp = lc[*x->fh];
        if (!dp) {
          dir_lc *d = New dir_lc;
	  d->attr = *x->fattr;
	  lc.insert(*x->fh, d);
        }
	else if (nc->proc () != NFSPROC3_READDIR &&
	         nc->proc () != NFSPROC3_ACCESS)
	  (*dp)->attr = *x->fattr;
      }
    }
  }

  if (nc->proc () == NFSPROC3_ACCESS) {
    ex_access3res *r = static_cast<ex_access3res *> (res);
    if (r->status == NFS3ERR_STALE)
      lc.clear();
  }

  else if (nc->proc () == NFSPROC3_GETATTR) {
    ex_getattr3res *r = static_cast<ex_getattr3res *> (res);
    if (r->status == NFS3ERR_STALE)
      lc.clear();
  }

  else if (nc->proc () == NFSPROC3_LOOKUP) {
    diropargs3 *a = nc->template getarg<diropargs3> ();
    ex_lookup3res *r = static_cast<ex_lookup3res *> (res);
    if (r->status == NFS3ERR_NOENT) {
      nlc_insert(a->dir, a->name);
      lc_remove(a->dir, a->name);
    }
    else if (!r->status) {
      nlc_remove(a->dir, a->name);
      lc_insert(a->dir, a->name, r->resok->object);
    }
  }

  else if (nc->proc () == NFSPROC3_READDIR) {
    readdir3args *a = nc->template getarg<readdir3args> ();
    ex_readdir3res *r = static_cast<ex_readdir3res *> (res);
    if (r->status == NFS3ERR_STALE)
      lc_clear(a->dir);
    else if (!r->status) {
      for (entry3 *e = r->resok->reply.entries; e; e = e->nextentry)
	nlc_remove(a->dir, e->name);
    }
  }

  else if (nc->proc() == NFSPROC3_CREATE) {
    create3args *a = nc->template getarg<create3args> ();
    ex_diropres3 *r = static_cast<ex_diropres3 *> (res);
    if (!r->status) {
      nlc_remove(a->where.dir, a->where.name);
      if (r->resok->obj.present)
	lc_insert(a->where.dir, a->where.name, *(r->resok->obj.handle));
    }
  }

  else if (nc->proc() == NFSPROC3_MKDIR) {
    mkdir3args *a = nc->template getarg<mkdir3args> ();
    ex_diropres3 *r = static_cast<ex_diropres3 *> (res);
    if (!r->status) {
      nlc_remove(a->where.dir, a->where.name);
      if (r->resok->obj.present)
	lc_insert(a->where.dir, a->where.name, *(r->resok->obj.handle));
    }
  }

  else if (nc->proc() == NFSPROC3_SYMLINK) {
    symlink3args *a = nc->template getarg<symlink3args> ();
    ex_diropres3 *r = static_cast<ex_diropres3 *> (res);
    if (!r->status) {
      nlc_remove(a->where.dir, a->where.name);
      if (r->resok->obj.present)
	lc_insert(a->where.dir, a->where.name, *(r->resok->obj.handle));
    }
  }

  else if (nc->proc() == NFSPROC3_RENAME) {
    rename3args *a = nc->template getarg<rename3args> ();
    ex_rename3res *r = static_cast<ex_rename3res *> (res);
    if (!r->status) {
      nlc_remove(a->to.dir, a->to.name);
      nlc_insert(a->from.dir, a->from.name);
      lc_remove(a->from.dir, a->from.name);
      lc_remove(a->to.dir, a->to.name);
    }
  }

  else if (nc->proc() == NFSPROC3_LINK) {
    link3args *a = nc->template getarg<link3args> ();
    ex_link3res *r = static_cast<ex_link3res *> (res);
    if (!r->status) {
      nlc_remove(a->link.dir, a->link.name);
      lc_insert(a->link.dir, a->link.name, a->file);
    }
  }

  else if (nc->proc() == NFSPROC3_REMOVE || nc->proc() == NFSPROC3_RMDIR) {
    diropargs3 *a = nc->template getarg<diropargs3> ();
    ex_wccstat3 *r = static_cast<ex_wccstat3 *> (res);
    if (!r->status || r->status == NFS3ERR_NOENT) {
      nlc_insert(a->dir, a->name);
      lc_remove(a->dir, a->name);
    }
  }
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
      if (lc[xa->handle])
	lc_clear(xa->handle);
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
      nc->proc () != cl_NFSPROC3_CLOSE &&
      nc->proc () != NFSPROC3_COMMIT)
    return false;

  nfs_fh3 *fh = static_cast<nfs_fh3 *> (nc->getvoidarg ());
  file_cache *e = file_cache_lookup(*fh);

  if (e) {
    if ((nc->proc () == cl_NFSPROC3_CLOSE || nc->proc () == NFSPROC3_COMMIT)) {
      if (!e->is_dirty() && !e->is_flush() && !e->being_modified ())
        return false;
      else if (e->being_modified ()) {
        warn_debug << "RPC " << nc->proc ()
	           << " blocked due to unfinished writes\n";
        e->rpcs.push_back(nc);
        return true;
      }
    }

    // flush mode: block WRITEs and SETATTRs
    if (e->is_flush() && nc->proc () != NFSPROC3_READ) {
      warn_debug << "RPC " << nc->proc () << " blocked due to flush\n";
      e->rpcs.push_back(nc);
      return true;
    }

    // open mode: on READ and WRITE, fetch file
    else if (e->is_open()) {
      if (nc->proc () == NFSPROC3_READ || nc->proc () == NFSPROC3_WRITE) {
	nfs_fh3 file;
	if (nc->proc() == NFSPROC3_WRITE) {
	  write3args *a = nc->template getarg<write3args> ();
	  file = a->file;
	}
	else {
	  read3args *a = nc->template getarg<read3args> ();
	  file = a->file;
	}
        fattr3 fa = *reinterpret_cast<const fattr3 *> (ac.attr_lookup (file));
        check_cache (file, fa, nc->getaid());
	if (!e->is_idle()) {
          warn_debug << "RPC " << nc->proc () << " blocked due to open\n";
          e->rpcs.push_back(nc);
	  return true;
	}
      }
    }

    // fetch mode: block WRITEs and SETATTRs. block READ RPCs that
    // cannot be answered.
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
	  // *16 forces reading 16 blocks before starting at the
	  // beginning again
	  e->want(offset, size*16, rtpref);
      }
      else {
	warn_debug << "RPC " << nc->proc () << " blocked\n";
      }
      e->rpcs.push_back(nc);
      return true;
    }

    if (e->is_error()) {
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

  switch (nc->proc ()) {
  case NFSPROC3_ACCESS:
    {
      access3args *a = nc->template getarg<access3args> ();
      int32_t perm = ac.access_lookup (a->object, nc->getaid (), a->access);
      if (perm > 0) {
        fattr3 fa =
	  *reinterpret_cast<const fattr3 *> (ac.attr_lookup (a->object));
        if (fa.type == NF3REG) {
          file_cache *e = file_cache_lookup(a->object);
          if (!e) {
            file_cache_insert (a->object);
            e = file_cache_lookup(a->object);
            assert(e);
	    e->fa.mtime.seconds = 0;
	    e->fa.mtime.nseconds = 0;
            e->open();
          }
          else if (e->is_idle())
            e->open();
        }
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

  case NFSPROC3_READ:
    {
      read3args *a = nc->template getarg<read3args> ();
      file_cache *e = file_cache_lookup(a->file);
      if (e) {
        read_from_cache (nc, e);
        return;
      }
      else
        warn << "dangling read: " << a->file << "\n";
      break;
    }

  case cl_NFSPROC3_CLOSE:
  case NFSPROC3_COMMIT:
    {
      nfs_fh3 *a = nc->template getarg<nfs_fh3> ();
      file_cache *e = file_cache_lookup(*a);
      assert(!e->is_flush());

      if (e && e->is_dirty()) {
        flush_cache (nc, e);
        return;
      }

      if (!e)
        warn << "dangling close: " << *a << "\n";

      if (nc->proc () == cl_NFSPROC3_CLOSE && e &&
	  e->afh != 0 && !e->is_fetch ()) {
	e->afh->close (wrap (&server::file_closed));
	e->afh = 0;
      }

      if (nc->proc () == NFSPROC3_COMMIT) {
        commit3res res (NFS3_OK);
        res.resok->verf = verf3;
        nc->reply (&res);
      }
      else
        nc->error (NFS3_OK);
      return;
    }

  case NFSPROC3_WRITE:
    {
      write3args *a = nc->template getarg<write3args> ();
      file_cache *e = file_cache_lookup(a->file);
      if (e) {
        write_to_cache (nc, e);
        return;
      }
      else
        warn << "dangling write: " << a->file << "\n";
      break;
    }

  case NFSPROC3_SETATTR:
    {
      // for a set-size SETATTR: truncate cache file to the appropriate
      // size. set size attribute on the cache file, but not mtime. do
      // wcc checking. don't mark the file dirty: if it wasn't dirty
      // before, and will not be modified, we cannot flush file content
      // to server because SETATTR may not always follow ACCESS, so file
      // content may not be up to date.
      setattr3args *a = nc->template getarg<setattr3args> ();
      file_cache *e = file_cache_lookup(a->object);
      if (a->new_attributes.size.set && e) {
        truncate_cache (nc, e, *(a->new_attributes.size.val));
        return;
      }
      break;
    }

  case NFSPROC3_LOOKUP:
    {
      diropargs3 *a = nc->template getarg<diropargs3> ();
      dir_lc **dp = lc[a->dir];
      const ex_fattr3 *f = ac.attr_lookup (a->dir);

      if (dp && f && (*dp)->attr.mtime < f->mtime) // directory is newer
        lc_clear(a->dir);
      else if (dp) {
        bool has_negative = nlc_lookup(a->dir, a->name);
        if (has_negative) {
          lookup3res res(NFS3ERR_NOENT);
	  res.resfail->set_present (false);
	  if (f) {
            res.resfail->set_present (true);
            *res.resfail->attributes = *reinterpret_cast<const fattr3 *> (f);
	  }
          nc->reply (&res);
          return;
        }
        else if (f) {
	  nfs_fh3 fh;
          bool hit = lc_lookup(a->dir, a->name, fh);
	  if (hit) {
	    const ex_fattr3 *objf = ac.attr_lookup (fh);
	    if (objf) {
              lookup3res res(NFS3_OK);
	      res.resok->object = fh;
              res.resok->dir_attributes.set_present (true);
              *res.resok->dir_attributes.attributes
	        = *reinterpret_cast<const fattr3 *> (f);
              res.resok->obj_attributes.set_present (true);
              *res.resok->obj_attributes.attributes
	        = *reinterpret_cast<const fattr3 *> (objf);
              nc->reply (&res);
              return;
	    }
	  }
        }
      }
      break;
    }

  default:
    break;
  }

  dispatch_to_server (nc);
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
server::dir_lc_gc_remove (dir_lc *d)
{
  delete d;
}

