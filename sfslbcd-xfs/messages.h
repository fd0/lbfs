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
#define LBFS_MAXDATA 65536

class condwrite3args {
public:
  condwrite3args(int filedesc, ref<struct xfs_message_putdata> msg, 
                 nfs_fh3 tfh) 
    : fd(filedesc), h(msg), tmpfh(tfh), 
      blocks_written(0), total_blocks(0), chunker(0), eof(false) {
    fname = New char[MAXPATHLEN];
    retries = 0;
    commited = false;
    cur_pos = 0;
  }
  ~condwrite3args() { 
    delete[] fname;
    delete chunker; 
  }
  
  int fd;
  ref<struct xfs_message_putdata> h;
  nfs_fh3 tmpfh;
  char *fname;
  uint blocks_written;
  uint total_blocks;
  Chunker *chunker;
  bool eof;
  int retries;
  bool commited;
  off_t cur_pos;
};

class getfp_args {
 public:
  getfp_args(int f, ref<struct xfs_message_open> header) : 
    fd(f), h(header), offset(0), blocks_written(0), total_blocks(0), eof(0) {
    out_fname = New char[MAXPATHLEN];
    retries = 0;
  }
  ~getfp_args() {
    delete[] out_fname;
  }

 int fd;
 ref<struct xfs_message_open> h;
 uint64 offset; 
 struct xfs_message_installdata msg;
 char *out_fname;
 uint blocks_written;
 uint total_blocks;
 uint eof;
 int retries;
};

class rename_args {
 public: 
  rename_args(int f, ref<struct xfs_message_rename> header) :
    fd(f), h(header), 
    lres(New refcounted<ex_lookup3res>), 
    rnres(New refcounted<ex_rename3res>), 
    gares(New refcounted<ex_getattr3res>) {} 
  ~rename_args() {
  }
  int fd;
  ref<struct xfs_message_rename> h;
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
#if DEBUG > 0
  warn << "f(h) = " << fingerprint(data, count) << "\n";
  warn << "h = " << armor32(h, sha1::hashsize) << "\n";
  warn << "hash = " << armor32(hash.base(), sha1::hashsize) << "\n";
#endif
  return strncmp(h, hash.base(), sha1::hashsize);
}

int xfs_wakeup (xfscall *xfsc);
int xfs_getroot (xfscall *xfsc);
int xfs_getnode (xfscall *xfsc);
int xfs_getdata (xfscall *xfsc);
int xfs_open (xfscall *xfsc);
int xfs_getattr (xfscall *xfsc);
int xfs_inactivenode (xfscall *xfsc);
int xfs_putdata (xfscall *xfsc);
int xfs_putattr (xfscall *xfsc);
int xfs_create (xfscall *xfsc);
int xfs_mkdir (xfscall *xfsc);
int xfs_link (xfscall *xfsc);
int xfs_symlink (xfscall *xfsc);
int xfs_remove (xfscall *xfsc);
int xfs_rmdir (xfscall *xfsc);
int xfs_rename (xfscall *xfsc);
int xfs_pioctl (xfscall *xfsc);

void cbdispatch(svccb *sbp);

class xfs_wakeup_args {
 public:
  int fd;
  struct xfs_message_wakeup *h;

  xfs_wakeup_args (int file_des, struct xfs_message_wakeup *header) :
    fd(file_des), h(header) { }
  ~xfs_wakeup_args () {
    delete h;
  }

};

struct xfs_getroot_args {
  struct xfs_message_getroot *h;
  sfs_fsinfo *fsi;
  ex_getattr3res *attr_res;

  xfs_getroot_args (struct xfs_message_getroot *header) :
    h(header), fsi(NULL), attr_res(NULL) { }

  ~xfs_getroot_args () {
    delete h;
    if (fsi) 
      delete fsi;
    if (attr_res) 
      delete attr_res;
  }
};

class xfs_getnode_args {
 public:
  int fd;
  struct xfs_message_getnode *h;
  
};

class xfs_getdata_args {

};

class xfs_open_args {

};

class xfs_getattr_args {

};

class xfs_inactivenode_args {

};

class xfs_putdata_args {

};

class xfs_putattr_args {

};

class xfs_create_args {

};

class xfs_mkdir_args {

};

class xfs_link_args {

};

class xfs_symlink_args {

};

class xfs_remove_args {

};

class xfs_rmdir_args {

};

class xfs_rename_args {

};

class xfs_pioctl_args {

};

#endif /* _MESSAGES_H_ */
