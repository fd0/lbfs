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

#define NFS_MAXDATA 8192

extern fh_map fht;

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

#if 0

int xfs_message_rename (int fd, struct xfs_message_rename *h, u_int size);

int xfs_message_pioctl (int fd, struct xfs_message_pioctl *h, u_int size) ;

#endif /* if 0 */

#endif /* _MESSAGES_H_ */
