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

typedef callback<void, ptr<aiobuf>, ssize_t, int>::ref aiofh_cbrw;

struct read_state {
  time_t rqtime;
  uint64 off;
  uint64 cnt;
};

struct read_obj {
  static const unsigned PARALLEL_READS = 8;
  static const unsigned LBFS_MAXDATA = 65536;
  static const unsigned LBFS_MIN_BYTES_FOR_GETFP = 16384;
  typedef callback<void,bool,bool>::ref cb_t;

  cb_t cb;
  ref<server> srv;
  file_cache *fe;
  nfs_fh3 fh;
  AUTH *auth;
  uint64 size;
  unsigned int outstanding_reads;
  bool errorcb;
  bool use_lbfs;
  
  uint64 bytes_read;

  vec<uint64> rq_off;
  vec<uint64> rq_cnt;
  
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
    bytes_read += cnt;
    srv->nfsc->call (lbfs_NFSPROC3_READ, a, res,
	             wrap (this, &read_obj::read_reply, rs, a, res),
		     auth);
  }

  void do_read () 
  {
    if (outstanding_reads >= PARALLEL_READS)
      return;

    if (!use_lbfs) { // NFS read
      while (fe->pri.size()) {
        uint64 off = fe->pri.pop_front();
        if (off < size) {
          unsigned s = size-off;
          s = s > srv->rtpref ? srv->rtpref : s;
	  if (!fe->req->filled(off, s)) {
            nfs3_read (off, s);
	    return;
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
	  return;
        }
      }
    }
    else { // LBFS read
      while (rq_off.size () > 0 && outstanding_reads < PARALLEL_READS) {
	uint64 cnt = rq_cnt [0];
	uint64 off;
	if (cnt > srv->rtpref) {
          off = rq_off [0];
	  cnt = srv->rtpref;
	  rq_off [0] = rq_off [0] + srv->rtpref;
	  rq_cnt [0] = rq_cnt [0] - srv->rtpref;
	}
	else {
          off = rq_off.pop_front ();
          cnt = rq_cnt.pop_front ();
	}
	if (!fe->req->filled (off, cnt))
          nfs3_read (off, cnt);
      }
    }
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
    if (outstanding_reads == 0) {
      fe->afh->fsync (wrap (&read_obj::file_closed));
      str pfn = fe->prevfn;
      fe->prevfn = fe->fn;
      // warn << "remove " << pfn << "\n";
      file_cache::a->unlink(pfn.cstr(), wrap(&read_obj::file_closed));
      delete this;
    }
  }

  void aiod_read (chunk_location *c, ptr<aiofh> afh, aiofh_cbrw cb)
  {
    ptr<aiobuf> buf = file_cache::a->bufalloc (c->count ());
    if (!buf) {
      file_cache::a->bufwait
        (wrap (this, &read_obj::aiod_read, c, afh, cb));
      return;
    }
    afh->read (c->pos (), buf, cb);
  }

  struct rdstate {
    fp_db::iterator *ci;
    uint64 offset;
    uint64 cnt;
    sfs_hash hash;
  };

  void
  check_chunk_read (rdstate *rds, chunk_location *c, ptr<aiofh> afh,
		    ptr<aiobuf> buf, ssize_t sz, int err)
  {
    afh->close (wrap (&read_obj::file_closed));

    if (!err) {
      Chunker chunker;
      chunker.chunk_data ((unsigned char *)buf->base (), sz);
      chunker.stop ();
      const vec<chunk *>& cv = chunker.chunk_vector();
      if (cv.size () == 1 && cv[0]->hash_eq (rds->hash) &&
	  (unsigned)sz == rds->cnt) {
        // got a matching chunk
	// warn << "matching chunk found\n";
        fe->afh->write (rds->offset, buf,
	                wrap (this, &read_obj::read_reply_write,
			      rds->offset, rds->cnt));
	delete c;
	delete rds->ci;
	delete rds;
	return;
      }
      else 
	warn << "got data, but no match\n";
    }

    outstanding_reads--;
    delete c;
    rds->ci->del ();
    if (!next_chunk (false, rds)) {
      warn << "no next chunk, queueing " << rds->cnt << "\n";
      rq_off.push_back (rds->offset);
      rq_cnt.push_back (rds->cnt);
      delete rds->ci;
      delete rds;
    }
    do_read ();
  }
    
  void
  check_chunk_open (rdstate *rds, chunk_location *c,
		    ptr<aiofh> afh, int err) 
  {
    if (!err) {
      aiod_read (c, afh,
	         wrap (this, &read_obj::check_chunk_read, rds, c, afh));
      return;
    }

    outstanding_reads--;
    delete c;
    rds->ci->del ();
    if (!next_chunk (false, rds)) {
      warn << "can't open file, queueing " << rds->cnt << "\n";
      rq_off.push_back (rds->offset);
      rq_cnt.push_back (rds->cnt);
      delete rds->ci;
      delete rds;
    }
    do_read ();
  }

  bool
  next_chunk (bool first, rdstate *rds)
  {
    chunk_location *c = New chunk_location;
    int r;
    if (first)
      r = rds->ci->get (c);
    else
      r = rds->ci->next (c);
    while (!r) {
      nfs_fh3 f;
      c->get_fh(f);
      file_cache *e = srv->file_cache_lookup (f);
      if (e && e->prevfn != "" && e->prevfn != fe->fn) {
	outstanding_reads++;
	file_cache::a->open
	  (e->prevfn, O_RDONLY, 0,
	   wrap (this, &read_obj::check_chunk_open, rds, c));
	return true;
      }
      if (!e)
	rds->ci->del ();
      r = rds->ci->next (c);
    }
    delete c;
    return false;
  }

  void compose (uint64 offset, ref<lbfs_getfp3res> res)
  {
    for (unsigned i=0; i<res->resok->fprints.size(); i++) {
      uint64 count = res->resok->fprints[i].count;
      // warn << "get_fp +" << count << "\n";
      bool checking = false;
      fp_db::iterator *ci = 0;
      u_int64_t index;
      memmove(&index, res->resok->fprints[i].hash.base(), sizeof(index));
      if (!server::fpdb.get_iterator (index, &ci)) {
	if (ci) {
          rdstate *rds = New rdstate;
          rds->ci = ci;
          rds->offset = offset;
          rds->cnt = count;
          rds->hash = res->resok->fprints[i].hash;
	  if (next_chunk(true, rds))
	    checking = true;
	  else {
	    delete rds;
	    delete ci;
	  }
	}
      }
      if (!checking) { // chunk not found locally
        // warn << index << ": nothing in db, queueing " << count << "\n";
        rq_off.push_back (offset);
	rq_cnt.push_back (count);
      }
      chunk c (offset, count, res->resok->fprints[i].hash);
      c.location ().set_fh (fh);
      server::fpdb.add_entry
	(c.hashidx (), &(c.location ()), c.location ().size ());
      offset += res->resok->fprints[i].count;
    }
    do_read ();
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
    }
    else if (offset == 0)
      start_nfs_read ();
    if (outstanding_reads == 0)
      ok ();
  }

  void file_open (str fn, ptr<aiofh> afh, int err) 
  {
    if (err) {
      warn << "fill_cache: open failed: " << err << "\n";
      fail ();
      return;
    }
    if (fe->afh)
      fe->afh->close (wrap (&read_obj::file_closed));
    fe->fn = fn;
    fe->afh = afh;

    if (srv->use_lbfs () && size > LBFS_MIN_BYTES_FOR_GETFP)
      use_lbfs = true;
    else
      use_lbfs = false;

    if (use_lbfs) {
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
      start_nfs_read ();
  }

  void start_nfs_read () 
  {
    for (unsigned i=0; i<PARALLEL_READS; i++)
      do_read ();
    if (!outstanding_reads) // nothing to do
      ok();
  }

  read_obj (file_cache *fe, uint64 size, ref<server> srv,
            AUTH *a, read_obj::cb_t cb)
    : cb(cb), srv(srv), fe(fe), fh(fe->fh), auth(a), size(size),
      outstanding_reads(0), errorcb(false)
  {
    assert(fe);

    bytes_read = 0;
    str fn = srv->gen_fn_from_fh (fh);
    file_cache::a->open (fn, O_CREAT | O_TRUNC | O_RDWR, 0666,
	                 wrap (this, &read_obj::file_open, fn));
  }

  ~read_obj()
  {
    warn << "read_obj: read " << bytes_read << "/" << size << " bytes\n"; 
  }
};

void
lbfs_read (file_cache *fe, uint64 size, ref<server> srv,
           AUTH *a, read_obj::cb_t cb)
{
  vNew read_obj (fe, size, srv, a, cb);
}

