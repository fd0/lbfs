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

#ifndef SFSRWSD_H
#define SFSRWSD_H

#include "qhash.h"
#include "arpc.h"
#include "vec.h"
#include "sfsmisc.h"
#include "mount_prot.h"
#include "crypt.h"
#include "rabin.h"
#include "seqno.h"
#include "nfstrans.h"
#include "sfsserv.h"
#include "lbfs_prot.h"
#include "lbfsdb.h"
#include "fingerprint.h"

#define FATTR3 fattr3exp

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

//
// tmp fh to name translation
//
struct tmpfh_record {
#define TMPFN_MAX 1024
  nfs_fh3 fh;
  char name[TMPFN_MAX];
  int len;
  ihash_entry<tmpfh_record> hlink;

  tmpfh_record (const nfs_fh3 &f, const char *s, unsigned l);
  ~tmpfh_record ();
};

struct tmpfh_table {
  ihash<const nfs_fh3, tmpfh_record, 
        &tmpfh_record::fh, &tmpfh_record::hlink> tab;
};

struct filesys {
  typedef qhash<u_int64_t, u_int64_t> inotab_t;

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
  ihash_entry<filesys> rhl;
  ihash_entry<filesys> mphl;
  inotab_t *inotab;
};

class erraccum;
struct synctab;
class filesrv {
public:
  struct reqstate {
    u_int32_t fsno;
    bool rootfh;
  };

  str host;
  ptr<aclnt> c;
  ptr<aclnt> mountc;
  
  sfs_servinfo servinfo;
  sfs_hash hostid;
  ptr<rabin_priv> sk;

  ptr<axprt_stream> authxprt;
  ptr<aclnt> authclnt;

  vec<filesys> fstab;
  vec<nfs_fh3> sfs_trash_fhs;
  ihash<nfs_fh3, filesys, &filesys::fh_root, &filesys::rhl, hashfh3> root3tab;
  ihash<nfs_fh3, filesys, &filesys::fh_mntpt, &filesys::mphl, hashfh3> mp3tab;

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
  void getnfsc (ptr<aclnt> c, clnt_stat stat);
  void getmountc (ptr<aclnt> c, clnt_stat stat);

  void gotroot (ref<erraccum> ea, int i, const nfs_fh3 *fhp, str err);
  void gotroots (bool ok);

  void gottrashdir (ref<erraccum> ea, int i,
		    const nfs_fh3 *fhp, str err);
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

  int fhsubst (bool *substp, nfs_fh3 *fhp, u_int32_t *fsnop);
  size_t getfsno (const filesys *fsp) const {
#ifdef CHECK_BOUNDS
    assert (fstab.base () <= fsp && fsp < fstab.lim ());
#endif CHECK_BOUNDS
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

extern AUTH *auth_root;
extern AUTH *auth_default;

const strbuf &strbuf_cat (const strbuf &, mountstat3);
void getfh3 (const char *host, str path,
	     callback<void, const nfs_fh3 *, str>::ref);
void getfh3 (ref<aclnt> c, str path,
	     callback<void, const nfs_fh3 *, str>::ref);
void lookupfh3 (ref<aclnt> c, const nfs_fh3 &start, str path,
		callback<void, const nfs_fh3 *, const FATTR3 *, str>::ref cb);

class client : public virtual refcount, public sfsserv {
  filesrv *fsrv;

  ptr<asrv> nfssrv;

  lbfs_db lbfsdb;
  tmpfh_table fhtab;

  static u_int64_t nextgen ();

  void fail ();

  void nfs3dispatch (svccb *);
  void nfs3reply (svccb *sbp, void *res, filesrv::reqstate rqs, clnt_stat err);
  void renamecb_1 (svccb *sbp, void *res, filesrv::reqstate rqs,
		   clnt_stat err);
  void renamecb_2 (svccb *sbp, rename3res *rres, filesrv::reqstate rqs,
		   lookup3res *ares, clnt_stat err);

  void condwrite_write_cb (svccb *sbp, filesrv::reqstate rqs, size_t count,
                           write3res *, str err);
  void condwrite_got_chunk (svccb *sbp, filesrv::reqstate rqs,
		            lbfs_db::chunk_iterator * iter, 
			    unsigned char *data, 
			    size_t count, read3res *, str err);
  void condwrite_read_cb (unsigned char *, off_t, 
                          const unsigned char *, size_t, off_t);
  void condwrite (svccb *sbp, filesrv::reqstate rqs);

  void mktmpfile_cb (svccb *sbp, filesrv::reqstate rqs, char *path,
                     void *_cres, clnt_stat err);
  void mktmpfile (svccb *sbp, filesrv::reqstate rqs);
  
  void chunk_data (Chunker *, const unsigned char *data, 
                   size_t count, off_t pos);
  void removetmp_cb (wccstat3 *, clnt_stat err);
  void committmp_cb (svccb *sbp, filesrv::reqstate rqs, Chunker *,
                     const FATTR3 *attr, commit3res *res, str err);
  void committmp (svccb *sbp, filesrv::reqstate rqs);
 
  void getfp_cb (svccb *sbp, filesrv::reqstate rqs, Chunker *, 
                 size_t count, read3res *, str err);
  void getfp (svccb *sbp, filesrv::reqstate rqs);

protected:
  client (ref<axprt_crypt> x);
  ~client ();
  ptr<rabin_priv> doconnect (const sfs_connectarg *, sfs_servinfo *);

public:
  ptr<aclnt> nfscbc;
  const u_int64_t generation;
  ihash_entry<client> glink;

  void sfs_getfsinfo (svccb *sbp);

  static void launch (ref<axprt_crypt> x) { vNew refcounted<client> (x); }
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

void client_accept (ptr<axprt_crypt> x);

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

#endif

