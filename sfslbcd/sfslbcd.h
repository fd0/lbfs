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

inline
const strbuf &
strbuf_cat(const strbuf &b, nfs_fh3 fh)
{
  str s = armor32(fh.data.base(), fh.data.size());
  b << s;
  return b;
}

class attr_cache {
  struct access_dat {
    u_int32_t mask;
    u_int32_t perm;

    access_dat (u_int32_t m, u_int32_t p) : mask (m), perm (p) {}
  };

public:
  struct attr_dat {
    attr_cache *const cache;
    const nfs_fh3 fh;
    ex_fattr3 attr;
    qhash<sfs_aid, access_dat> access;

    ihash_entry<attr_dat> fhlink;
    tailq_entry<attr_dat> lrulink;

    attr_dat (attr_cache *c, const nfs_fh3 &f, const ex_fattr3 *a);
    ~attr_dat ();
    void touch ();
    void set (const ex_fattr3 *a, const wcc_attr *w);
    bool valid () { return timenow < implicit_cast<time_t> (attr.expire); }
  };

private:
  friend class attr_dat;
  ihash<const nfs_fh3, attr_dat, &attr_dat::fh, &attr_dat::fhlink> attrs;

  static void remove_aid (sfs_aid aid, attr_dat *ad)
    { ad->access.remove (aid); }

public:
  ~attr_cache () { attrs.deleteall (); }
  void flush_attr () { attrs.deleteall (); }
  void flush_access (sfs_aid aid) { attrs.traverse (wrap (remove_aid, aid)); }
  void flush_access (const nfs_fh3 &fh, sfs_aid);

  void attr_enter (const nfs_fh3 &, const ex_fattr3 *, const wcc_attr *);
  const ex_fattr3 *attr_lookup (const nfs_fh3 &);

  void access_enter (const nfs_fh3 &, sfs_aid aid,
		     u_int32_t mask, u_int32_t perm);
  int32_t access_lookup (const nfs_fh3 &, sfs_aid, u_int32_t mask);
};

class server : public sfsserver_auth {
  str cdir;
  struct fcache {
    nfs_fh3 fh;
    fattr3 fa;
    bool dirty;
    int fd;
    fcache(nfs_fh3 fh, fattr3 fa) : fh(fh), fa(fa), dirty(false), fd(-1) {}
  };

  attr_cache ac;
  lrucache<nfs_fh3, fcache> fc;

  void dispatch_dummy (svccb *sbp);
  void cbdispatch (svccb *sbp);
  void setfd (int fd);
  
  void getreply (time_t rqtime, nfscall *nc, void *res, clnt_stat err);
  void access_reply (time_t rqtime, nfscall *nc, void *res, clnt_stat err);

  void flush_cache (nfscall *nc, fcache *e);
  void read_from_cache (nfscall *nc, fcache *e);
  void write_to_cache (nfscall *nc, fcache *e);
  int truncate_cache (uint64 size, fcache *e);

  void close_reply (nfscall *nc, bool ok);
  void cache_file_reply (time_t rqtime, nfscall *nc, void *res, bool ok);
  void access_reply_cached (nfscall *nc, int32_t perm, fattr3 fa,
                            bool update_fa, bool ok);

  void remove_cache (fcache e);
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

  void fcache_insert (nfs_fh3 fh, fattr3 fa) {
    struct fcache f(fh, fa);
    fc.insert(fh,f);
  }

public:
  typedef sfsserver_auth super;
  ptr<aclnt> nfsc;
  ptr<asrv> nfscbs;

  server (const sfsserverargs &a);
  ~server () { warn << path << " deleted\n"; }
  void flushstate ();
  void authclear (sfs_aid aid);
  void setrootfh (const sfs_fsinfo *fsi, callback<void, bool>::ref err_c);
  void dispatch (nfscall *nc);
  void getattr (time_t rqtime, unsigned int proc,
                sfs_aid aid, void *arg, void *res);
};

void lbfs_read (str fn, nfs_fh3 fh, size_t size, ref<server> srv,
                AUTH *a, callback<void, bool>::ref cb);
void lbfs_write (str fn, nfs_fh3 fh, size_t size, ref<server> srv,
                 AUTH *a, callback<void, bool>::ref cb);

#endif

