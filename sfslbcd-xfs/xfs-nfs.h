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
#include <sys/dir.h>
#include "nfs3exp_prot.h"
#include <xfs/xfs_message.h>
#include "fh_map.h"

#ifdef __linux__
#include <xfs/xfs_dirent.h>
#else
#define XFS_DIRENT_BLOCKSIZE 1024
#define xfs_dirent dirent
#endif

extern fh_map fht;

struct write_dirent_args {
    int fd;
#ifdef HAVE_OFF64_T
    off64_t off;
#else
    off_t off;
#endif
    char *buf;
    char *ptr;
    void *last;
};

bool xfs_fheq(xfs_handle, xfs_handle);
bool nfs_fheq(nfs_fh3, nfs_fh3);
u_char nfs_rights2xfs_rights(u_int32_t, ftype3, u_int32_t);
void nfsobj2xfsnode(xfs_cred, nfs_fh3, ex_fattr3, xfs_msg_node *);
int flushbuf(write_dirent_args *);
int nfsdir2xfsfile(ex_readdir3res *, write_dirent_args *);
int nfsdirent2xfsfile(int, const char*, uint64);
int xfsfile_rm_dirent(int, const char* fname);
int xfsattr2nfsattr(xfs_attr, sattr3 *);

#endif /* _XFS_NFS_H */
