/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@mit.edu)
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

#ifndef _SFSROSD_H_
#define _SFSROSD_H_

#include "sfsmisc.h"
#include "nfs3_prot.h"
#include "nfstrans.h"
#include "lbfs_prot.h"
#include "arpc.h"
#include "crypt.h"
#include "sfsserv.h"

class filesrv;

class fh_entry {
private:
  int fd;

public:
  str path;
  nfs_fh3 fh;
  ex_fattr3 fa;
  filesrv *fsrv;
  time_t lastused;

  ihash_entry<fh_entry> fhlink;
  tailq_entry<fh_entry> timeoutlink;
 
  fh_entry (str p, nfs_fh3 f, ex_fattr3 *a, filesrv *fs);
  ~fh_entry ();
  int closefd (void);
  void setfd (int f) { fd = f;}
  void update_attr (int fd);
  void update_attr (str p);
  void print (void);
};


class filesrv {
private:
  static const int fhe_timer = 5;      // seconds
  static const int fhe_expire = 3600;  // expire time in seconds for fh
  static const int fd_expire = 30;     // expire time in seconds for fd
  static const int fhe_max = 10000;    // max number of file handles
  static const int fd_max = 60;        // max number of file descriptors
  int fhe_n;                           // number of file handles in use
  int fd_n;                            // number of file descriptors in use

  ihash<nfs_fh3, fh_entry, &fh_entry::fh, &fh_entry::fhlink> entries;
  tailq<fh_entry, &fh_entry::timeoutlink> timeoutlist;
  timecb_t *fhetmo;

public:
  sfs_servinfo servinfo;
  sfs_hash hostid;
  ptr<rabin_priv> sk;
  fh_entry *root;

  filesrv();
  void fhetimeout (void);
  int lookup_attr (str p, ex_fattr3 *fa);
  void mk_fh (nfs_fh3 *fh, ex_fattr3 *fa);
  int closefd (fh_entry *fh);
  int getfd (fh_entry *fhe, int flags);
  int getfd (str p, int flags, mode_t mode);
  fh_entry *lookup_add (str p);
  fh_entry *lookup (nfs_fh3 *fh);
  void remove (fh_entry *fhe);
  int checkfhe (void);
  void purgefd (int force);
  void purgefhe (void);
  void printfhe (void);
  int checkfd (void);

};

class client : public sfsserv {
  filesrv *fsrv;

  ptr<axprt_crypt> x;
  ptr<asrv> rwsrv;
  //  ptr<asrv> sfssrv;

  bool unixauth;
  uid_t uid;

  bool authid_valid;
  sfs_hash authid;

  void nfs3dispatch (svccb *sbp);
  void nfs3_getattr (svccb *sbp);
  void nfs3_access (svccb *sbp);
  void nfs3_fsinfo (svccb *sbp);
  void nfs3_fsstat (svccb *sbp);
  void nfs3_lookup (svccb *sbp);
  void nfs3_readdir (svccb *sbp);
  void nfs3_read (svccb *sbp);
  void nfs3_create (svccb *sbp);
  void nfs3_condwrite (svccb *sbp);
  void nfs3_write (svccb *sbp);
  void nfs3_commit (svccb *sbp);
  void nfs3_remove (svccb *sbp);
  void nfs3_rmdir (svccb *sbp);
  void nfs3_rename (svccb *sbp);
  void nfs3_link (svccb *sbp);
  void nfs3_mkdir (svccb *sbp);
  void nfs3_symlink (svccb *sbp);
  void nfs3_readlink (svccb *sbp);
  void nfs3_setattr (svccb *sbp);
  uint32 access_check(ex_fattr3 *fa, uint32 access_req);
  bool dirlookup (str dir, filename3 *name);

public:
  client (ref<axprt_crypt> x);
  void sfs_getfsinfo (svccb *sbp);

protected:
  ptr<rabin_priv> doconnect (const sfs_connectarg *, sfs_servinfo *);
};

extern filesrv *defsrv;

#endif _SFSROSD_H_
