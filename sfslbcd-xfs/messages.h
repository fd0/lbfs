/*
 *
 * Copyright (C) 2000 Athicha Muthitacharoen (athicha@mit.edu)
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



#ifndef _MESSAGES_H_
#define _MESSAGES_H_

#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif

#include <xfs/xfs_message.h>
#include "sfslbcd.h"
#include "xfs-sfs.h"
#include "xfs-nfs.h"
#include "fh_map.h"
#include "xfs.h"
#include "fingerprint.h"
#include "sha1.h"

#define NFS_MAXDATA 8192
#define LBFS_MAXDATA 262144

class condwrite3args {
public:
  condwrite3args(int filedesc, struct xfs_message_putdata* msg, nfs_fh3 tfh) :
    fd(filedesc), h(msg), tmpfh(tfh), blocks_written(0), total_blocks(0), 
    done(false), chunker(0) { 
    fname = New char[MAXPATHLEN];
  }
  ~condwrite3args() { 
    delete[] fname;
    delete chunker; 
  }
  
  int fd;
  struct xfs_message_putdata* h;
  nfs_fh3 tmpfh;
  //int rfd;
  char *fname;
  uint blocks_written;
  uint total_blocks;
  bool done;
  Chunker *chunker;
};

class getfp_args {
 public:
  getfp_args(int f, struct xfs_message_getdata *header) : 
    fd(f), h(header), offset(0), blocks_written(0), total_blocks(0), eof(0) {
    out_fname = New char[MAXPATHLEN];
  }
  ~getfp_args() {
    delete[] out_fname;
  }

 int fd;
 struct xfs_message_getdata *h;
 uint64 offset; 
 //int cfd;
 struct xfs_message_installdata msg;
 char *out_fname;
 uint blocks_written;
 uint total_blocks;
 uint eof;
};

class rename_args {
 public: 
  rename_args(int f, struct xfs_message_rename *header) :
    fd(f), h(header), 
    lres(New refcounted<ex_lookup3res>), 
    rnres(New refcounted<ex_rename3res>), 
    gares(New refcounted<ex_getattr3res>) {} 
#if 0
  ~rename_args() {
    if (lres) delete lres;
    if (rnres) delete rnres;
    if (gares) delete gares;
  }
#endif
  int fd;
  struct xfs_message_rename *h;
  ref<ex_lookup3res> lres;
  time_t rqtime1; //first attr time
  ref<ex_rename3res> rnres;
  ref<ex_getattr3res> gares;
};

// returns 0 if sha1 hash of data is equals to the given hash
static inline int
compare_sha1_hash(unsigned char *data, size_t count, sfs_hash &hash)
{
  char h[sha1::hashsize];
  sha1_hash(h, data, count);
  warn << "f(h) = " << fingerprint(data, count) << "\n";
  warn << "h = " << armor32(h, sha1::hashsize) << "\n";
  warn << "hash = " << armor32(hash.base(), sha1::hashsize) << "\n";
  return strncmp(h, hash.base(), sha1::hashsize);
}

#if 0
class xfs_message {
 public: xfs_message
}
#endif

int xfs_message_getroot (int, struct xfs_message_getroot*, u_int);

int xfs_message_getnode (int, struct xfs_message_getnode*, u_int);

int xfs_message_getdata (int, struct xfs_message_getdata*, u_int);

int xfs_message_getattr (int, struct xfs_message_getattr*, u_int);

int xfs_message_inactivenode (int,struct xfs_message_inactivenode*,u_int);

int xfs_message_putdata (int fd, struct xfs_message_putdata *h, u_int size);

int xfs_message_putattr (int fd, struct xfs_message_putattr *h, u_int size);

int xfs_message_create (int fd, struct xfs_message_create *h, u_int size);

int xfs_message_mkdir (int fd, struct xfs_message_mkdir *h, u_int size);

int xfs_message_link (int fd, struct xfs_message_link *h, u_int size);

int xfs_message_symlink (int fd, struct xfs_message_symlink *h, u_int size);

int xfs_message_remove (int fd, struct xfs_message_remove *h, u_int size);

int xfs_message_rmdir (int fd, struct xfs_message_rmdir *h, u_int size);

int xfs_message_rename (int fd, struct xfs_message_rename *h, u_int size);

int xfs_message_pioctl (int fd, struct xfs_message_pioctl *h, u_int size) ;

void cbdispatch(svccb *sbp);

#endif /* _MESSAGES_H_ */
