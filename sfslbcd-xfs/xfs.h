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

#ifndef __XFS_H_V
#define __XFS_H_V 1

#include <stdarg.h>
#include <xfs/xfs_message.h>
#include "arpc.h"
#include "lbfs_prot.h"

void xfs_message_init (void);
int  xfs_message_send (int fd, struct xfs_message_header *h, u_int size);
int  xfs_message_receive (int fd, struct xfs_message_header *h, u_int size);
int  xfs_send_message_wakeup (int fd, u_int seqnum, int error);
int  xfs_send_message_wakeup_multiple (int fd, u_int seqnum, int error, ...);
int  xfs_send_message_wakeup_vmultiple (int fd,	u_int seqnum, int error, 
					va_list args);
struct xfscall {

  u_int32_t opcode;
  int instance;
  int fd;
  void *const argp;
  void *resp;

  xfscall (u_int32_t oc, int file_des, void *const ap, void *rp) : 
    opcode(oc), instance(0), fd(file_des), argp(ap), resp(rp) { }
  ~xfscall () {
  }
  void *getvoidarg () { return argp; }
  void *getvoidres () { return resp; }
};
  
typedef int (*xfs_message_function) (xfscall *);
extern xfs_message_function rcvfuncs[XFS_MSG_COUNT];

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
  sfs_fsinfo *fsi;
  ex_getattr3res *attr_res;

  xfs_getroot_args () : fsi(NULL), attr_res(NULL) { }

  ~xfs_getroot_args () {
    if (fsi) 
      delete fsi;
    if (attr_res) 
      delete attr_res;
  }
};

#endif /* __XFS_H_V */
