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

void xfs_message_init (void);
int  xfs_message_send (int fd, struct xfs_message_header *h, u_int size);
int  xfs_message_receive (int fd, struct xfs_message_header *h, u_int size);
int  xfs_send_message_wakeup (int fd, u_int seqnum, int error);
int  xfs_send_message_wakeup_multiple (int fd, u_int seqnum, int error, ...);
int  xfs_send_message_wakeup_vmultiple (int fd,	u_int seqnum, int error, 
					va_list args);
class xfscall {
  void *argp;
 public:
  u_int32_t opcode;
  int fd;

  xfscall (u_int32_t oc, int file_des) : argp(NULL), opcode(oc), fd(file_des) 
    { }
  ~xfscall () {
  }
  void *getvoidarg () { return argp; }
};
  
typedef int (*xfs_message_function) (xfscall *);
extern xfs_message_function rcvfuncs[XFS_MSG_COUNT];

#endif /* __XFS_H_V */
