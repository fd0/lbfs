/*
 *
 * Copyright (C) 2002 Benjie Chen (benjie@lcs.mit.edu)
 * Copyright (C) 1998 David Mazieres (dm@uun.org)
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

#ifndef LBFSCD_H
#define LBFSCD_H

#include "arpc.h"
#include "sfscd_prot.h"
#include "nfstrans.h"
#include "sfsclient.h"
#include "qhash.h"
#include "itree.h"
#include "crypt.h"
#include "list.h"
#include "lrucache.h"
#include "ranges.h"
#include "aiod.h"
#include "attrcache.h"

inline bool
operator== (const nfs_fh3 &a, const nfs_fh3 &b)
{
  return a.data.size () == b.data.size ()
    && !memcmp (a.data.base (), b.data.base (), b.data.size ());
}

inline bool
operator!= (const nfs_fh3 &a, const nfs_fh3 &b)
{
  return !(a == b);
}

inline
const strbuf &
strbuf_cat(const strbuf &b, nfs_fh3 fh)
{
  str s = armor32(fh.data.base(), fh.data.size());
  b << s;
  return b;
}

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

class file_cache {
  friend class read_obj;
public:
  sfs_aid aid;
  nfs_fh3 fh;
  int status;
  fattr3 fa;
  uint64 osize;
  ptr<aiofh> afh;
  vec<nfscall *> rpcs;
  int outstanding_ops;
  uint64 mstart;
  uint64 mend;
  
  bool flush_scheduled;
  bool flush_wait;

private:
  static const int fcache_open  = 0;
  static const int fcache_fetch = 1;
  static const int fcache_idle  = 2;
  static const int fcache_dirty = 3;
  static const int fcache_flush = 4;
  static const int fcache_error = 5;

  vec<uint64> pri;
  ranges *rcv;
  ranges *req;
  
  void cr() {
    if (rcv) {
      delete rcv;
      delete req;
    }
    rcv = 0;
    req = 0;
  }
  void ar(uint64 size) {
    assert(!rcv && !req);
    rcv = New ranges(0, size);
    req = New ranges(0, size);
  }

public:
  file_cache(nfs_fh3 fh)
    : fh(fh), status(fcache_open), afh(0), outstanding_ops(0),
      mstart(0), mend(0), rcv(0), req(0)
  {
    flush_scheduled = flush_wait = false;
  }

  ~file_cache() { if (rcv) delete rcv; assert(rpcs.size() == 0); }

  bool is_idle()    const { return status == fcache_idle; }
  bool is_open()    const { return status == fcache_open; }
  bool is_fetch()   const { return status == fcache_fetch; }
  bool is_flush()   const { return status == fcache_flush; }
  bool is_dirty()   const { return status == fcache_dirty; }
  bool is_error()   const { return status == fcache_error; }

  void idle()    { cr(); status = fcache_idle; }
  void open()    { cr(); status = fcache_open; }
  void fetch(uint64 size) { ar(size); status = fcache_fetch; }
  void flush()	 { cr(); status = fcache_flush; }
  void dirty()   { cr(); status = fcache_dirty; }
  void error()   { cr(); status = fcache_error; }

  void outstanding_op () { outstanding_ops++; }
  void outstanding_op_done () { outstanding_ops--; }
  bool being_modified () const { return outstanding_ops > 0; }

  bool received(uint64 off, uint64 size) const {
    if (is_fetch() && (!rcv || !rcv->filled(off, size)))
      return false;
    return true;
  }
  void want(uint64 off, uint64 size, unsigned rtpref) {
    for(uint64 i=off; i<off+size; i+=rtpref)
      pri.push_back(i);
  }

  static aiod* a;
};

struct dir_lc {
  ex_fattr3 attr;
  qhash<filename3, nfs_fh3> lc;
  qhash<filename3, bool> nlc;
};

class server : public sfsserver_auth {
  friend class read_obj;
  friend class write_obj;
protected:
  str cdir;
  bool try_compress;
  bool do_lbfs;
  writeverf3 verf3;
  lbfs_attr_cache ac;
  lrucache<nfs_fh3, file_cache *> fc;
  lrucache<nfs_fh3, dir_lc *> lc; 

  void check_lbfs (void *res, clnt_stat err);
  void dispatch_dummy (svccb *sbp);
  void cbdispatch (svccb *sbp);
  void setfd (int fd);
  void getreply (time_t rqtime, nfscall *nc, void *res, clnt_stat err);
  void fixlc (nfscall *nc, void *res);
  bool dont_run_rpc (nfscall *nc);

  void delayed_flush (nfs_fh3 fh);
  void flush_cache (nfscall *nc, file_cache *e);

  void read_from_cache (nfscall *nc, file_cache *e);
  void read_from_cache_open (nfscall *nc, file_cache *e,
                             ptr<aiofh> afh, int err);
  void read_from_cache_read (nfscall *sbp, file_cache *e,
                             ptr<aiobuf> buf, ssize_t sz, int err);

  void write_to_cache (nfscall *nc, file_cache *e);
  void write_to_cache_open (nfscall *sbp, file_cache *e,
                            ptr<aiofh> afh, int err);
  void write_to_cache_write (nfscall *sbp, file_cache *e,
                             ptr<aiobuf> buf, ssize_t sz, int err);

  void truncate_cache (nfscall *sbp, file_cache *e, uint64 size);
  void truncate_cache_open (nfscall *sbp, file_cache *e, uint64 size,
                            ptr<aiofh> afh, int err);
  void truncate_cache_truncate (nfscall *sbp, file_cache *e, uint64 size,
                                int err);

  void check_cache (nfs_fh3 obj, fattr3 fa, sfs_aid aid);

  void access_reply (time_t rqtime, nfscall *nc, void *res, clnt_stat err);
  void fetch_done (file_cache *e, bool done, bool ok);
  void flush_done (nfscall *nc, nfs_fh3 fh, fattr3 fa, bool ok);

  void run_rpcs (file_cache *e);
  void dispatch_to_server (nfscall *nc);

  static void file_closed (int) {}

  str fh2fn(nfs_fh3 fh) {
    strbuf n;
    unsigned char x = 0;
    char *c = fh.data.base();
    for (unsigned i=0; i<fh.data.size(); i++)
      x = x ^ ((unsigned char)(*(c+i)));
    n << cdir << "/" << (((unsigned int)x)%254) << "/"
      << armor32(fh.data.base(), fh.data.size());
    return n;
  }

  void nlc_insert (nfs_fh3 dir, filename3 name) {
    dir_lc *d;
    dir_lc **dp = lc[dir];
    if (!dp)
      return;
    d = *dp;
    if (d->nlc[name])
      d->nlc.remove(name);
    d->nlc.insert(name, true);
  }

  bool nlc_lookup (nfs_fh3 dir, filename3 name) {
    dir_lc **dp = lc[dir];
    if (dp) {
      if ((*dp)->nlc[name])
        return true;
    }
    return false;
  }

  void nlc_remove (nfs_fh3 dir, filename3 name) {
    dir_lc **dp = lc[dir];
    if (dp)
      (*dp)->nlc.remove(name);
  }

  void nlc_remove (nfs_fh3 dir) {
    dir_lc **dp = lc[dir];
    if (dp)
      (*dp)->nlc.clear();
  }

  void lc_insert (nfs_fh3 dir, filename3 name, nfs_fh3 fh) {
    dir_lc *d;
    dir_lc **dp = lc[dir];
    if (!dp)
      return;
    d = *dp;
    if (d->lc[name])
      d->lc.remove(name);
    d->lc.insert(name, fh);
  }
  
  bool lc_lookup (nfs_fh3 dir, filename3 name, nfs_fh3 &fh) {
    dir_lc **dp = lc[dir];
    if (dp) {
      nfs_fh3 *f = (*dp)->lc[name];
      if (f) {
	fh = *f;
        return true;
      }
    }
    return false;
  }

  void lc_remove (nfs_fh3 dir, filename3 name) {
    dir_lc **dp = lc[dir];
    if (dp)
      (*dp)->lc.remove(name);
  }

  void lc_remove (nfs_fh3 dir) {
    dir_lc **dp = lc[dir];
    if (dp)
      (*dp)->lc.clear();
  }

  void lc_clear (nfs_fh3 dir) {
    lc_remove(dir);
    nlc_remove(dir);
    lc.remove(dir);
  }

  void file_cache_insert (nfs_fh3 fh) {
    file_cache *f = New file_cache(fh);
    fc.insert(fh,f);
  }

  file_cache *file_cache_lookup (nfs_fh3 fh) {
    file_cache **e = fc[fh];
    if (e) {
      assert(*e);
      return *e;
    }
    return 0;
  }

  void file_cache_gc_remove (file_cache *e);
  void dir_lc_gc_remove (dir_lc *d);

public:
  typedef sfsserver_auth super;
  ptr<aclnt> nfsc;
  ptr<asrv> nfscbs;
  unsigned rtpref;
  unsigned wtpref;

  server (const sfsserverargs &a);
  ~server () { warn << path << " deleted\n"; }
  bool use_lbfs () const { return do_lbfs; }

  void flushstate ();
  void authclear (sfs_aid aid);
  void setrootfh (const sfs_fsinfo *fsi, callback<void, bool>::ref err_c);
  void dispatch (nfscall *nc);
  void getxattr (time_t rqtime, unsigned int proc,
                 sfs_aid aid, void *arg, void *res);

  static unsigned tmpfd;
};

void lbfs_read (str fn, nfs_fh3 fh, uint64 size, ref<server> srv,
                AUTH *a, callback<void, bool, bool>::ref cb);
void lbfs_write (str fn, file_cache *fe,
                 nfs_fh3 fh, uint64 size, fattr3 fa,
		 ref<server> srv,
                 AUTH *a, callback<void, fattr3, bool>::ref cb);

#endif

