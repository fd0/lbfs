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
#ifndef _SFSLBCD_H_
#define _SFSLBCD_H_

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <sys/time.h>

#define export _export		/* C++ keyword gets used in C */

#if defined(HAVE_DIRENT_H)
#include <dirent.h>
#endif
#if DIRENT_AND_SYS_DIR_H
#include <sys/dir.h>
#endif
//#elif defined(HAVE_SYS_DIR_H)
//#include <sys/dir.h>
//#endif

#ifndef MOUNT_XFS
#define MOUNT_XFS       "xfs"           /* xfs */
#endif

#include <unistd.h>
#include <sys/types.h>
#ifdef HAVE_LINUX_TYPES_H
#include <linux/types.h>
#endif
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/uio.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#ifdef HAVE_SYS_IOCCOM_H
#include <sys/ioccom.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#include <sys/mount.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <pwd.h>

#undef export

#endif /* _SFSLBCD_H_ */
