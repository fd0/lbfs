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

#include "xfs-nfs.h"

fh_map fht = fh_map();

bool xfs_fheq(xfs_handle x1, xfs_handle x2) {
  if (xfs_handle_eq(&x1, &x2)) 
    return true;
  else return false;
}

bool nfs_fheq(nfs_fh3 n1, nfs_fh3 n2) {
  if (memcmp(n1.data.base(), n2.data.base(), 
	     n1.data.size()) == 0)
    return true;
  else return false;
}

u_char nfs_rights2xfs_rights(u_int32_t access, ftype3 ftype, u_int32_t mode) {
  u_char ret = 0;

  if (ftype == NF3DIR) {
    if (access & (ACCESS3_READ | ACCESS3_LOOKUP | ACCESS3_EXECUTE))
      ret |= XFS_RIGHT_R | XFS_RIGHT_X;
    if (access & (ACCESS3_MODIFY | ACCESS3_EXTEND | ACCESS3_DELETE))
      ret |= XFS_RIGHT_W;
  } else {
    if ((ftype == NF3LNK) && (access & 
			       (ACCESS3_READ | ACCESS3_LOOKUP | ACCESS3_EXECUTE)))
      ret |= XFS_RIGHT_R;
    if ((access & (ACCESS3_READ | ACCESS3_LOOKUP)) && (mode & S_IRUSR)) 
      ret |= XFS_RIGHT_R;
    if ((access & (ACCESS3_MODIFY | ACCESS3_EXTEND | ACCESS3_DELETE)) && 
	(mode & S_IWUSR)) 
      ret |= XFS_RIGHT_W;
    if ((access & ACCESS3_EXECUTE) && (mode & S_IXUSR)) 
      ret |= XFS_RIGHT_X;    
  }
    
  return ret;
}

void nfsobj2xfsnode(xfs_cred cred, nfs_fh3 obj, ex_fattr3 attr, time_t rqtime,
		   xfs_msg_node *node) {

  //change expire to rpc_time + expire
  attr.expire += rqtime;
  node->handle = fht.gethandle(obj, attr);
  warn << "nfsfh becomes node.handle (" 
       << (int)node->handle.a << ","
       << (int)node->handle.b << ","
       << (int)node->handle.c << ","
       << (int)node->handle.d << ")\n";

  node->anonrights = XFS_RIGHT_R | XFS_RIGHT_W | XFS_RIGHT_X;
  node->tokens = XFS_ATTR_R; // | ~XFS_DATA_MASK;

  /* node->attr */
  node->attr.valid = XA_V_NONE;
  if (attr.type == NF3REG) {
    XA_SET_MODE(&node->attr, S_IFREG);
    XA_SET_TYPE(&node->attr, XFS_FILE_REG);
    XA_SET_NLINK(&node->attr, attr.nlink);
  } else 
    if (attr.type == NF3DIR) {
      XA_SET_MODE(&node->attr, S_IFDIR);
      XA_SET_TYPE(&node->attr, XFS_FILE_DIR);
      XA_SET_NLINK(&node->attr, attr.nlink);
    } else
      if (attr.type == NF3LNK) {
	XA_SET_MODE(&node->attr, S_IFLNK);
	XA_SET_TYPE(&node->attr, XFS_FILE_LNK);
	XA_SET_NLINK(&node->attr, attr.nlink);
      } else {
	warn << "nfsattr2xfs_attr: default\n";
	abort ();
      }
  XA_SET_SIZE(&node->attr, attr.size);
  XA_SET_UID(&node->attr,attr.uid);
  XA_SET_GID(&node->attr, attr.gid);
  node->attr.xa_mode  |= attr.mode;
  XA_SET_ATIME(&node->attr, attr.atime.seconds);
  XA_SET_MTIME(&node->attr, attr.mtime.seconds);
  XA_SET_CTIME(&node->attr, attr.ctime.seconds);
  XA_SET_FILEID(&node->attr, attr.fileid);

  //HARD CODE ACCESS FOR NOW!! use nfs3_access later
  node->anonrights = nfs_rights2xfs_rights(ACCESS3_READ  | 
					   ACCESS3_LOOKUP | 
					   ACCESS3_EXECUTE |
					   ACCESS3_MODIFY | 
					   ACCESS3_EXTEND | 
					   ACCESS3_DELETE,
					   attr.type, 
					   attr.mode);

  for (int i=0; i<MAXRIGHTS; i++) {
    node->id[i] = cred.pag;
    node->rights[i] = nfs_rights2xfs_rights(ACCESS3_READ  | 
					    ACCESS3_LOOKUP |
					    ACCESS3_EXECUTE | 
					    ACCESS3_MODIFY | 
					    ACCESS3_EXTEND | 
					    ACCESS3_DELETE,
					    attr.type, 
					    attr.mode);  
  }
}

static long blocksize = XFS_DIRENT_BLOCKSIZE;

int flushbuf(write_dirent_args *args) {
  unsigned inc = blocksize - (args->ptr - args->buf);
  xfs_dirent *last = (xfs_dirent *)args->last;

  last->d_reclen += inc;
  if (write (args->fd, args->buf, blocksize) != blocksize) {
    warn << "(" << errno << "):write\n";
    return -1;
  }
  args->ptr = args->buf;
  args->last = NULL;
  return 0;
}

int nfsdir2xfsfile(ex_readdir3res *res, write_dirent_args *args) {

  args->off = 0;
  args->buf = (char *)malloc (blocksize);
  if (args->buf == NULL) {
    warn << "nfsdir2xfsfile: malloc error\n";
    return -1;
  }
  args->ptr = args->buf;
  args->last = NULL;
  
  assert(res->status == NFS3_OK);
  entry3 *nfs_dirent = res->resok->reply.entries;
  xfs_dirent *xde = NULL;
  int reclen = sizeof(*xde);

  while (nfs_dirent != NULL) {
    if (args->ptr + reclen > args->buf + blocksize) {
      if (flushbuf (args) < 0) 
	return -1;
    }
    xde = (struct xfs_dirent *)args->ptr;
    xde->d_namlen = nfs_dirent->name.len();
    warn << "xde->namlen = " << xde->d_namlen 
	 << " nfs_dirent_len = " << nfs_dirent->name.len() << "\n";
    xde->d_reclen = reclen;
#if defined(HAVE_STRUCT_DIRENT_D_TYPE) && !defined(__linux__)
    xde->d_type = DT_UNKNOWN;
#endif
    xde->d_fileno = nfs_dirent->fileid;
    strcpy(xde->d_name, nfs_dirent->name.cstr());
    warn << "xde->d_name = " << xde->d_name 
	 << " nfs_dirent_name = " << nfs_dirent->name.cstr() << "\n";
    args->ptr += xde->d_reclen;
    args->off += xde->d_reclen;
    args->last = xde;

    nfs_dirent = nfs_dirent->nextentry;
  }

  return 0;
}

int nfsdirent2xfsfile(int fd, const char* fname, uint64 fid) {

  xfs_dirent *xde = (xfs_dirent *)malloc(sizeof(*xde));
  xde->d_namlen = strlen(fname);
  strcpy(xde->d_name, fname);
  xde->d_reclen = sizeof(*xde);
#if defined(HAVE_STRUCT_DIRENT_D_TYPE) && !defined(__linux__)
  xde->d_type = DT_UNKNOWN;
#endif
  xde->d_fileno = fid;
  
  if (write(fd, xde, xde->d_reclen) != xde->d_reclen) {
    warn << "(" << errno << "):write\n";
    return -1;
  }
  return 0;
}

int xfsfile_rm_dirent(int fd, const char* fname) {

  
  return 0;
}

int xfsattr2nfsattr(xfs_attr xa, sattr3 *na) {

  na->mode.set_set(true);
  *na->mode.val = xa.xa_mode;

  na->uid.set_set(true);
  warn << "xfs_uid = " << xa.xa_uid << "\n";
  *na->uid.val = xa.xa_uid;

  na->gid.set_set(true);
  warn << "xfs_gid = " << xa.xa_gid << "\n";
  *na->gid.val = xa.xa_gid;

  na->size.set_set(true);
  warn << "xfs_size = " << xa.xa_size << "\n";
  *na->size.val = xa.xa_size;

  na->atime.set_set(SET_TO_SERVER_TIME);

  if (na->atime.set == SET_TO_CLIENT_TIME) {
    na->atime.time->seconds = xa.xa_atime;
    na->atime.time->nseconds = 0;
  }

  na->mtime.set_set(SET_TO_SERVER_TIME);

  if (na->mtime.set == SET_TO_CLIENT_TIME) {
    na->mtime.time->seconds = xa.xa_mtime;
    na->mtime.time->nseconds = 0;
  }

  return 0;
}

int fattr2sattr(ex_fattr3 fa, sattr3 *sa) {
  
  sa->mode.set_set(true);
  *sa->mode.val = fa.mode;

  sa->uid.set_set(true);
  *sa->uid.val = fa.uid;

  sa->gid.set_set(true);
  *sa->gid.val = fa.gid;

  sa->size.set_set(true);
  *sa->size.val = fa.size;

  sa->atime.set_set(SET_TO_SERVER_TIME);

  if (sa->atime.set == SET_TO_CLIENT_TIME) {
    sa->atime.time->seconds = fa.atime.seconds;
    sa->atime.time->nseconds = fa.atime.nseconds;
  }

  sa->mtime.set_set(SET_TO_SERVER_TIME);

  if (sa->mtime.set == SET_TO_CLIENT_TIME) {
    sa->mtime.time->seconds = fa.mtime.seconds;
    sa->mtime.time->nseconds = fa.mtime.nseconds;
  }

  return 0;  
}
