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

#include "ranges.h"
#include "sfslbcd.h"
#include "lbfs_prot.h"

struct read_state {
  time_t rqtime;
  uint64 off;
  uint64 cnt;
};

struct read_obj {
  static const int PARALLEL_READS = 4;
  static const int LBFS_MAXDATA = 65536;
  typedef callback<void,bool,bool>::ref cb_t;

  cb_t cb;
  ref<server> srv;
  nfs_fh3 fh;
  AUTH *auth;
  uint64 size;
  unsigned int outstanding_reads;
  bool errorcb;
  file_cache *fe;

  void
  read_reply(ref<read_state> rs, ref<read3args> arg,
             ref<ex_read3res> res, clnt_stat err) 
  {
    if (errorcb || err || res->status != NFS3_OK) {
      outstanding_reads--;
      fail ();
      return;
    }

    if (res->resok->count > 0) {
      ptr<aiobuf> buf = file_cache::a->bufalloc (res->resok->count);
      if (!buf) {
        file_cache::a->bufwait
          (wrap (this, &read_obj::read_reply, rs, arg, res, err));
        return;
      }
    
      if (!err)
	srv->getxattr (rs->rqtime, NFSPROC3_READ, 0, arg, res);

      memmove(buf->base (), res->resok->data.base (), res->resok->count);
      fe->afh->write (rs->off, buf, wrap (this, &read_obj::read_reply_write,
                                          rs->off, rs->cnt));
    }
    else
      read_reply_write (rs->off, rs->cnt, 0, 0, 0);
  }

  void
  read_reply_write (uint64 off, uint64 cnt, ptr<aiobuf> buf,
                    ssize_t sz, int err)
  {
    outstanding_reads--;

    if (err) {
      warn << "fill_cache: write failed: " << err << "\n";
      fail ();
      return;
    }

    fe->rcv->add (off, cnt);
    do_read ();
    ok ();
  }

  void nfs3_read (uint64 off, uint32 cnt) 
  {
    fe->req->add(off, cnt);
    ref<read3args> a = New refcounted<read3args>;
    a->file = fh;
    a->offset = off;
    a->count = cnt;
    outstanding_reads++;
    ref<ex_read3res> res = New refcounted <ex_read3res>;
    ref<read_state> rs = New refcounted <read_state>;
    rs->rqtime = timenow;
    rs->off = off;
    rs->cnt = cnt;
    srv->nfsc->call (lbfs_NFSPROC3_READ, a, res,
	             wrap (this, &read_obj::read_reply, rs, a, res),
		     auth);
  }

  bool do_read() 
  {
    while (fe->pri.size()) {
      uint64 off = fe->pri.pop_front();
      if (off < size) {
        unsigned s = size-off;
        s = s > srv->rtpref ? srv->rtpref : s;
	if (!fe->req->filled(off, s)) {
          nfs3_read (off, s);
	  return false;
	}
      }
    }
    uint64 off;
    uint64 cnt;
    if (fe->req->has_next_gap(0, off, cnt)) {
      assert(off < size);
      cnt = cnt > srv->rtpref ? srv->rtpref : cnt;
      if (!fe->req->filled(off, cnt)) {
	nfs3_read (off, cnt);
	return false;
      }
    }
    return true;
  }

  static void file_closed (int) {}

  void fail () 
  {
    if (!errorcb) {
      fe->afh->close (wrap (&read_obj::file_closed));
      fe->afh = 0;
      errorcb = true;
      cb (true,false);
    }
    if (outstanding_reads == 0)
      delete this;
  }

  void ok() 
  {
    if (!errorcb)
      cb (outstanding_reads == 0,true);
    if (outstanding_reads == 0)
      delete this;
  }

  void compose (uint64 offset, ref<lbfs_getfp3res> res)
  {
    for (unsigned i=0; i<res->resok->fprints.size(); i++) {
      warn << "get_fp +" << res->resok->fprints[i].count << "\n";
    }
  }

  void getfp_reply (uint64 offset, ref<lbfs_getfp3res> res, clnt_stat err) 
  {
    outstanding_reads--;
    if (!err && res->status == NFS3_OK) {
      bool eof = res->resok->eof;
      uint64 next_offset = offset;
      if (!eof) {
	for (uint i=0; i < res->resok->fprints.size (); i++)
	  next_offset += res->resok->fprints[i].count;
        lbfs_getfp3args arg;
        arg.file = fh;
        arg.offset = next_offset;
        arg.count = LBFS_MAXDATA;
        ref<lbfs_getfp3res> new_res = New refcounted <lbfs_getfp3res>;
	outstanding_reads++;
        srv->nfsc->call (lbfs_GETFP, &arg, new_res,
                         wrap (this, &read_obj::getfp_reply,
			       next_offset, new_res), auth);
      }
      compose (offset, res);
      if (offset == 0)
	start_read ();
    }
    else
      start_read ();
    ok ();
  }

  void file_open (ptr<aiofh> afh, int err) 
  {
    if (err) {
      warn << "fill_cache: open failed: " << err << "\n";
      fail ();
      return;
    }
    fe->afh = afh;

    if (srv->use_lbfs ()) {
      lbfs_getfp3args arg;
      arg.file = fh;
      arg.offset = 0;
      arg.count = LBFS_MAXDATA;
      ref<lbfs_getfp3res> res = New refcounted <lbfs_getfp3res>;
      outstanding_reads++;
      srv->nfsc->call (lbfs_GETFP, &arg, res,
	               wrap (this, &read_obj::getfp_reply, 0, res),
		       auth);
    }
    else
      start_read ();
  }

  void start_read () 
  {
    bool eof = false;
    for (int i=0; i<PARALLEL_READS && !eof; i++)
      eof = do_read();
    if (!outstanding_reads) // nothing to do
      ok();
  }

  read_obj (str fn, nfs_fh3 fh, uint64 size, ref<server> srv,
            AUTH *a, read_obj::cb_t cb)
    : cb(cb), srv(srv), fh(fh), auth(a), size(size),
      outstanding_reads(0), errorcb(false)
  {
    fe = srv->file_cache_lookup(fh);
    assert(fe);

    if (fe->afh == 0) {
      file_cache::a->open (fn, O_CREAT | O_RDWR, 0666,
                           wrap (this, &read_obj::file_open));
      return;
    }
    else
      file_open (fe->afh, 0);
  }


  ~read_obj() {}
};

void
lbfs_read (str fn, nfs_fh3 fh, uint64 size, ref<server> srv,
           AUTH *a, read_obj::cb_t cb)
{
  vNew read_obj (fn, fh, size, srv, a, cb);
}

