// -*- c++ -*-
/* $Id$ */

/*
 *
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

#include "qhash.h"
#include "arpc.h"
#include "vec.h"
#include "getfh3.h"
#include "sfscrypt.h"
#include "sfsmisc.h"
#include "crypt.h"
#include "rabin.h"
#include "seqno.h"
#include "nfstrans.h"
#include "sfsserv.h"
#include "axprt_compress.h"

#define FATTR3 fattr3exp

typedef callback<void, bool>::ref cbb;

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

struct hashfh3 {
  hashfh3 () {}
  hash_t operator() (const nfs_fh3 &fh) const {
    const u_int32_t *s = reinterpret_cast<const u_int32_t *> (fh.data.base ());
    const u_int32_t *e = s + (fh.data.size () >> 2);
    u_int32_t val = 0;
    while (s < e)
      val ^= *s++;
    return val;
  }
};

struct filesys {
  str host;
  ptr<aclnt> c;

  filesys *parent;
  str path_root;		// Local path corresponding to root
  str path_mntpt;		// Mountpoint relative to exported namespace
  nfs_fh3 fh_root;		// NFS File handle of root
  nfs_fh3 fh_mntpt;		// NFS File handle of mount point
  u_int64_t fsid;		// fsid of root
  u_int64_t fileid_root;	// fileid of root
  u_int64_t fileid_root_dd;	// fileid of root/..
  u_int64_t fileid_mntpt;	// fileid of mntpt
  u_int64_t fileid_mntpt_dd;	// fileid of mntpt/..
  enum {
    ANON_READ = 1,
    ANON_READWRITE = 3,
  };
  u_int options;		// Any of the above options
  ihash_entry<filesys> mphl;

  typedef qhash<u_int64_t, u_int64_t> inotab_t;
  typedef ihash<nfs_fh3, filesys, &filesys::fh_mntpt,
	        &filesys::mphl, hashfh3> mp3tab_t;

  mp3tab_t &mp3tab;
  inotab_t *inotab;

  filesys () : mp3tab (*New mp3tab_t), inotab (New inotab_t) {}
};

class erraccum;
struct synctab;
class filesrv {
public:
  struct reqstate {
    ptr<aclnt> c;
    u_int32_t fsno;
    bool rootfh;
  };

  ptr<sfs_servinfo_w> siw;
  sfs_servinfo servinfo;
  sfs_hash hostid;
  ptr<sfspriv> privkey;

  ptr<axprt_stream> authxprt;
  ptr<aclnt> authclnt;

  vec<filesys> fstab;

  blowfish fhkey;
  sfs_fsinfo fsinfo;
  u_int leasetime;

private:
  typedef callback<void, bool>::ref cb_t;
  cb_t::ptr cb;
  struct getattr_state {
    int nleft;
    bool ok;
  };

  PRIVDEST ~filesrv ();		// No deleting

  /* Initialization functions */
  int path2fsidx (str path, size_t nfs);

  void gotroot (ref<erraccum> ea, int i, ptr<nfsinfo> ni, str err);
  void gotroots (bool ok);

  void gotrootattr (ref<erraccum> ea, int i,
		    const nfs_fh3 *fhp, const FATTR3 *attr, str err);
  void gotmp (ref<erraccum> ea, int i,
	      const nfs_fh3 *fhp, const FATTR3 *attr, str err);
  void gotmps (bool ok);

  void gotrdres (ref<erraccum>, ref<readdir3res> res,
		 int i, bool mp, clnt_stat stat);
  void gotdds (bool ok);

  void fixrdres (void *res, filesys *fsp, bool rootfh);
  void fixrdplusres (void *res, filesys *fsp, bool rootfh);

  int fhsubst (bool *substp, filesys *pfsp, nfs_fh3 *fhp, u_int32_t *fsnop);
  size_t getfsno (const filesys *fsp) const {
#ifdef CHECK_BOUNDS
    assert (fstab.base () <= fsp && fsp < fstab.lim ());
#endif /*CHECK_BOUNDS*/ 
    return fsp - fstab.base ();
  }

public:
  synctab *const st;

  void init (cb_t cb);

  bool fixarg (svccb *sbp, reqstate *fsnop);
  bool fixres (svccb *sbp, void *res, reqstate *fsnop);

  bool getauthclnt ();

  filesrv ();
};

extern int sfssfd;

class client : public virtual refcount, public sfsserv {
  filesrv *fsrv;

  ptr<asrv> nfssrv;

  static u_int64_t nextgen ();

  void fail ();

  void nfs3dispatch (svccb *);
  void nfs3reply (svccb *sbp, void *res, filesrv::reqstate rqs, clnt_stat err);
  void renamecb_1 (svccb *sbp, void *res, filesrv::reqstate rqs,
		   clnt_stat err);
  void renamecb_2 (svccb *sbp, rename3res *rres, filesrv::reqstate rqs,
		   lookup3res *ares, clnt_stat err);
  void removecb_1 (svccb *sbp, lookup3res *ares, filesrv::reqstate rqs,
		   clnt_stat err);
  void removecb_2 (svccb *sbp, void *res, filesrv::reqstate rqs,
		   lookup3res *ares, clnt_stat err);
  void removecb_3 (svccb *sbp, getattr3res *gres, filesrv::reqstate rqs,
		   nfs_fh3 fh, void *res, clnt_stat err);

  bool try_compress;

protected:
  client (ref<axprt_zcrypt> x);
  ~client ();
  ptr<sfspriv> doconnect (const sfs_connectarg *, sfs_servinfo *);

public:
  ptr<aclnt> nfscbc;
  const u_int64_t generation;
  ihash_entry<client> glink;

  void sfs_getfsinfo (svccb *sbp);

  static void launch (ref<axprt_zcrypt> x);
  filesrv *getfilesrv () const { return fsrv; }
};

extern ihash<const u_int64_t, client,
  &client::generation, &client::glink> clienttab;

synctab *synctab_alloc ();
void synctab_free (synctab *st);
void dolease (filesrv *fsrv, u_int64_t cgen, u_int32_t fsno, xattr *xp);
void doleases (filesrv *fsrv, u_int64_t cgen, u_int32_t fsno,
	       svccb *sbp, void *res);

bool fh3tosfs (nfs_fh3 *);
bool fh3tonfs (nfs_fh3 *);

ptr<axprt_crypt> client_accept (ptr<axprt_crypt> x);

extern filesrv *defsrv;

template<class T> inline str
stat2str (T xstat, clnt_stat stat)
{
  if (stat)
    return strbuf () << stat;
  else if (xstat)
    return strbuf () << xstat;
  else
    return NULL;
}

