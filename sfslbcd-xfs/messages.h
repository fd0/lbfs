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
//static char iobuf[NFS_MAXDATA];

extern fh_map fht;

#endif /* _MESSAGES_H_ */
