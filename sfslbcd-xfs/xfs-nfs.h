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

#ifndef _XFS_NFS_H
#define _XFS_NFS_H

#include <sys/types.h>
#include "nfs3exp_prot.h"
#include <xfs/xfs_message.h>
#include "cache.h"

//#if defined(HAVE_DIRENT_H)
#include <dirent.h>          /* Should work for both Open and Free BSDs */
//#endif
#if DIRENT_AND_SYS_DIR_H
#include <sys/dir.h>
#endif

#ifdef __linux__
#include <xfs/xfs_dirent.h>
#else
#define XFS_DIRENT_BLOCKSIZE 1024
#define xfs_dirent dirent
#endif

struct write_dirent_args {
    int fd;
#ifdef HAVE_OFF64_T
    off64_t off;
#else
    off_t off;
#endif
    char buf[XFS_DIRENT_BLOCKSIZE];
    char *ptr;
    void *last;
};

u_char nfs_rights2xfs_rights(u_int32_t, ftype3, u_int32_t);
void nfsobj2xfsnode(xfs_cred, cache_entry *, xfs_msg_node *);
int flushbuf(write_dirent_args *);
int nfsdir2xfsfile(ex_readdir3res *, write_dirent_args *);
int conv_dir (int fd, ex_readdir3res *res);
int nfsdirent2xfsfile(int, const char*, uint64);
int xfsfile_rm_dirent(int fd1, int fd2, const char* fname);
int dir_remove_name(int, const char *);
int xfsattr2nfsattr(uint32 opcode, xfs_attr, sattr3 *);
int fattr2sattr(ex_fattr3, sattr3 *);
void xfs_reply_err (int fd, u_int seqnum, int err);

#endif /* _XFS_NFS_H */




