/*
 *
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

#include "sfslbcd.h"
#include "lbfs_prot.h"
#include "fingerprint.h"
  
typedef callback<void, ptr<aiobuf>, ssize_t, int>::ref aiofh_cbrw;

struct write_obj {
  static const unsigned int PARALLEL_WRITES = 16;
  typedef callback<void,fattr3,bool>::ref cb_t;

  cb_t cb;
  ref<server> srv;
  nfs_fh3 fh;
  fattr3 fa;
  file_cache *fe;
  AUTH *auth;
  uint64 size;
  uint64 written;
  unsigned int outstanding_writes;
  bool callback;
  bool commit;

  unsigned tmpfd;
  unsigned chunkv_sz;
  Chunker chunker;
  
  void
  aborttmp_reply(void *res, clnt_stat err) {
    auto_xdr_delete axd (lbfs_program_3.tbl[lbfs_ABORTTMP].xdr_res, res);
    delete this;
  }
  
  void
  committmp_reply(ref<ex_commit3res> res, clnt_stat err) {
    outstanding_writes--;
    if (!callback && !err && res->status == NFS3_OK) {
      ok();
      return;
    }
    fail();
  }
  
  void
  commit_reply(time_t rqtime, ref<commit3args> arg,
               ref<ex_commit3res> res, clnt_stat err) {
    outstanding_writes--;
    if (!err) {
      srv->getxattr (rqtime, NFSPROC3_COMMIT, 0, arg, res);
      if (res->resok->file_wcc.before.present &&
	  res->resok->file_wcc.after.present) {
        if ((res->resok->file_wcc.before.attributes)->size == fa.size &&
	    (res->resok->file_wcc.before.attributes)->mtime == fa.mtime) {
	  ex_fattr3 *f = res->resok->file_wcc.after.attributes;
	  fa = *reinterpret_cast<fattr3 *> (f);
	}
	else {
	  warn << "wcc check failed: reply out of order, or conflict\n";
          warn << "commit wcc failed: "
	       << fa.size << ":" << fa.mtime.seconds << ":"
	       << fa.ctime.seconds << " -- "
	       << (res->resok->file_wcc.before.attributes)->size << ":"
	       << (res->resok->file_wcc.before.attributes)->mtime.seconds << ":"
	       << "\n";
	}
      }
    }
    if (!callback && !err && res->status == NFS3_OK) {
      ok();
      return;
    }
    warn << "final commit failed\n";
    fail();
  }

  void
  write_reply(time_t rqtime, ref<write3args> arg,
              ref<ex_write3res> res, clnt_stat err) {
    outstanding_writes--;
    if (!err) {
      srv->getxattr (rqtime, NFSPROC3_WRITE, 0, arg, res);
      if (res->resok->file_wcc.before.present &&
	  res->resok->file_wcc.after.present) {
        if ((res->resok->file_wcc.before.attributes)->size == fa.size &&
	    (res->resok->file_wcc.before.attributes)->mtime == fa.mtime) {
	  ex_fattr3 *f = res->resok->file_wcc.after.attributes;
	  fa = *reinterpret_cast<fattr3 *> (f);
	}
	else {
	  warn << "wcc check failed: reply out of order, or conflict\n";
          warn << "write wcc failed: "
	       << fa.size << ":" << fa.mtime.seconds << ":"
	       << fa.ctime.seconds << " -- "
	       << (res->resok->file_wcc.before.attributes)->size << ":"
	       << (res->resok->file_wcc.before.attributes)->mtime.seconds << ":"
	       << "\n";
	}
      }
    }
    if (!callback && !err && res->status == NFS3_OK) {
      do_write();
      ok();
      return;
    }
    warn << "write failed\n";
    fail();
  }

  void tmpwrite_reply (ref<ex_write3res> res, clnt_stat err) {
    outstanding_writes--;
    if (!callback && !err && res->status == NFS3_OK) {
      do_write();
      ok();
      return;
    }
    fail();
  }

  void condwrite_reply (uint64 off, uint32 cnt, ref<ex_write3res> res,
                        clnt_stat err)
  {
    if (!callback && !err && res->status == NFS3ERR_FPRINTNOTFOUND) {
      warn << "hash not found\n";
      while (cnt > 0) {
        unsigned s = cnt;
        s = s > srv->wtpref ? srv->wtpref : s;
        nfs3_read (off, s, wrap (this, &write_obj::lbfs_tmpwrite, off, s));
        off += s;
	cnt -= s;
      }
      outstanding_writes--;
      return;
    }

    outstanding_writes--;
    if (!callback && !err && res->status == NFS3_OK) {
      do_write();
      ok();
      return;
    }

    warn << "condwrite_reply " << err << ", " << res->status << "\n";
    fail();
  }
  
  void nfs3_read (uint64 off, uint32 cnt, aiofh_cbrw cb)
  {
    outstanding_writes++;
    ptr<aiobuf> buf = file_cache::a->bufalloc (cnt);
    if (!buf) {
      file_cache::a->bufwait
        (wrap (this, &write_obj::nfs3_read_again, off, cnt, cb));
      return;
    }
    fe->afh->read (off, buf, cb);
  }

  void nfs3_read_again (uint64 off, uint32 cnt, aiofh_cbrw cb)
  {
    if (callback) {
      outstanding_writes--;
      fail ();
      return;
    }
    ptr<aiobuf> buf = file_cache::a->bufalloc (cnt);
    if (!buf) {
      file_cache::a->bufwait
        (wrap (this, &write_obj::nfs3_read_again, off, cnt, cb));
      return;
    }
    fe->afh->read (off, buf, cb);
  }

  void lbfs_condwrite (uint64 off, uint32 cnt,
                       ptr<aiobuf> buf, ssize_t sz, int err)
  {
    if (err || (unsigned)sz != cnt) {
      outstanding_writes--;
      if (err)
        warn << "lbfs_write: read failed: " << err << "\n";
      else
        warn << "lbfs_write: short read: got "
             << sz << " wanted " << cnt << "\n";
      fail ();
      return;
    }

    if (callback) {
      outstanding_writes--;
      fail ();
      return;
    }

    chunker.chunk_data ((unsigned char*) buf->base (), off, (unsigned)sz);
    const vec<chunk *>& cv = chunker.chunk_vector ();
    if (chunkv_sz < cv.size ()) {
      for (unsigned i=chunkv_sz; i < cv.size (); i++) {
        chunk *c = cv[i];
        uint64 off = c->location ().pos ();
        uint64 cnt = c->location ().count ();
        // warn << c->hashidx () << ": " << off << "+" << cnt << "\n";

	lbfs_condwrite3args arg;
        arg.commit_to = fh;
        arg.fd = tmpfd;
	arg.offset = off;
	arg.count = cnt;
	arg.hash = c->hash ();
        ref<ex_write3res> res = New refcounted <ex_write3res>;
        outstanding_writes++;
        srv->nfsc->call (lbfs_CONDWRITE, &arg, res,
	                 wrap (this, &write_obj::condwrite_reply,
			       off, cnt, res), auth);
      }
      chunkv_sz = cv.size ();
    }
    outstanding_writes--;
  }

  void lbfs_tmpwrite (uint64 off, uint32 cnt,
                      ptr<aiobuf> buf, ssize_t sz, int err)
  {
    if (err || (unsigned)sz != cnt) {
      outstanding_writes--;
      if (err)
        warn << "lbfs_write: read failed: " << err << "\n";
      else
        warn << "lbfs_write: short read: got "
             << sz << " wanted " << cnt << "\n";
      fail ();
      return;
    }

    if (callback) {
      outstanding_writes--;
      fail ();
      return;
    }

    lbfs_tmpwrite3args arg;
    arg.commit_to = fh;
    arg.fd = tmpfd;
    arg.offset = off;
    arg.count = sz;
    arg.stable = UNSTABLE;
    arg.data.setsize(sz);
    memmove (arg.data.base (), buf->base (), sz);
    
    ref<ex_write3res> res = New refcounted <ex_write3res>;
    srv->nfsc->call (lbfs_TMPWRITE, &arg, res,
	             wrap (this, &write_obj::tmpwrite_reply, res), auth);
  }

  void nfs3_write (uint64 off, uint32 cnt,
                   ptr<aiobuf> buf, ssize_t sz, int err)
  {
    if (err || (unsigned)sz != cnt) {
      outstanding_writes--;
      if (err)
        warn << "lbfs_write: read failed: " << err << "\n";
      else
        warn << "lbfs_write: short read: got "
             << sz << " wanted " << cnt << "\n";
      fail ();
      return;
    }

    if (callback) {
      outstanding_writes--;
      fail ();
      return;
    }

    ref<write3args> a = New refcounted<write3args>;
    a->file = fh;
    a->offset = off;
    a->count = sz;
    a->stable = UNSTABLE;
    a->data.setsize(sz);
    memmove (a->data.base (), buf->base (), sz);
    
    ref<ex_write3res> res = New refcounted <ex_write3res>;
    srv->nfsc->call (lbfs_NFSPROC3_WRITE, a, res,
	             wrap (this, &write_obj::write_reply, timenow, a, res),
		     auth);
  }

  void nfs3_commit () {
    ref<commit3args> a = New refcounted<commit3args>;
    a->file = fh;
    a->offset = 0;
    a->count = 0;
    outstanding_writes++;
    ref<ex_commit3res> res = New refcounted <ex_commit3res>;
    srv->nfsc->call (lbfs_NFSPROC3_COMMIT, a, res,
	             wrap (this, &write_obj::commit_reply, timenow, a, res),
		     auth);
  }

  void do_write() {
    if (outstanding_writes >= PARALLEL_WRITES)
      return;
    if (written < size && !callback) {
      unsigned s = size-written;
      s = s > srv->wtpref ? srv->wtpref : s;
      if (srv->use_lbfs ())
	nfs3_read (written, s,
	           wrap (this, &write_obj::lbfs_condwrite, written, s));
      else
	nfs3_read (written, s,
	           wrap (this, &write_obj::nfs3_write, written, s));
      written += s;
    }
    if (!srv->use_lbfs () && written == size && !commit) {
      commit = true;
      nfs3_commit ();
    }
  }

  static void file_closed (int) { }

  void fail () {
    if (!callback) {
      fe->afh->close (wrap (&write_obj::file_closed));
      fe->afh = 0;
      callback = true;
      cb(fa, false);
    }
    if (outstanding_writes == 0) {
      warn << "in fail(), outstanding write = 0\n";
      if (srv->use_lbfs ()) {
        lbfs_committmp3args arg;
        arg.commit_to = fh;
        arg.fd = tmpfd;
        void *res = lbfs_program_3.tbl[lbfs_ABORTTMP].alloc_res ();
        srv->nfsc->call (lbfs_ABORTTMP, &arg, res,
	                 wrap (this, &write_obj::aborttmp_reply, res), auth);
      }
      else {
	warn << "delete this\n";
	delete this;
      }
    }
  }

  void ok () {
    if (outstanding_writes == 0) {
      if (srv->use_lbfs () && !commit) {
        commit = true;
        outstanding_writes++;
    
	lbfs_committmp3args arg;
        arg.commit_to = fh;
        arg.fd = tmpfd;
        ref<ex_commit3res> res = New refcounted <ex_commit3res>;
        srv->nfsc->call (lbfs_COMMITTMP, &arg, res,
	                 wrap (this, &write_obj::committmp_reply, res), auth);
	return;
      }

      if (!callback) {
	warn << "close after flush\n";
        fe->afh->close (wrap (&write_obj::file_closed));
        fe->afh = 0;
        callback = true;
	cb(fa, true);
      }

      warn << "in ok(), delete this\n";
      delete this;
    }
  }

  void start_write () {
    for (unsigned i=0; i<PARALLEL_WRITES/4; i++)
      do_write();
    if (!outstanding_writes) // nothing to do
      ok();
  }

  void mktmpfile_reply (ref<ex_diropres3> res, clnt_stat err) {
    if (err || res->status != NFS3_OK)
      fail ();
  }

  write_obj (str fn, file_cache *fe, nfs_fh3 fh,
             uint64 size, fattr3 fa, ref<server> srv,
             AUTH *a, write_obj::cb_t cb)
    : cb(cb), srv(srv), fh(fh), fa(fa), fe(fe), auth(a),
      size(size), written(0), outstanding_writes(0),
      callback(false), commit(false)
  {
    assert (fe->afh);

    if (srv->use_lbfs ()) {
      lbfs_mktmpfile3args arg;
      arg.commit_to = fh;
      tmpfd = server::tmpfd;
      server::tmpfd ++;
      arg.fd = tmpfd;
      arg.obj_attributes.mode.set_set (true);
      *(arg.obj_attributes.mode.val) = fa.mode;
      arg.obj_attributes.uid.set_set (true);
      *(arg.obj_attributes.uid.val) = fa.uid;
      arg.obj_attributes.gid.set_set (true);
      *(arg.obj_attributes.gid.val) = fa.gid;
      arg.obj_attributes.size.set_set (true);
      *(arg.obj_attributes.size.val) = size; // assume this is size of file?
      arg.obj_attributes.atime.set_set (SET_TO_CLIENT_TIME);
      arg.obj_attributes.atime.time->seconds = fa.atime.seconds;
      arg.obj_attributes.atime.time->nseconds = fa.atime.nseconds;
      arg.obj_attributes.mtime.set_set (SET_TO_CLIENT_TIME);
      arg.obj_attributes.mtime.time->seconds = fa.mtime.seconds;
      arg.obj_attributes.mtime.time->nseconds = fa.mtime.nseconds;

      ref<ex_diropres3> res = New refcounted <ex_diropres3>;
      srv->nfsc->call (lbfs_MKTMPFILE, &arg, res,
	               wrap (this, &write_obj::mktmpfile_reply, res),
		       auth);

      chunkv_sz = 0;
    }

    start_write ();
  }

  ~write_obj() {}
};

void
lbfs_write (str fn, file_cache *fe, nfs_fh3 fh,
            uint64 size, fattr3 fa, ref<server> srv,
            AUTH *a, write_obj::cb_t cb)
{
  vNew write_obj (fn, fe, fh, size, fa, srv, a, cb);
}

