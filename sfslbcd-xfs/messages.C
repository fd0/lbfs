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

#include "messages.h"

#if 0
nfs_fh3 *curdir, *curfh;
ex_getattr3res curdir_attrres, curfh_attrres;
#endif

nfs_fh3 *fh;
diropargs3 *doa;
readdir3args *rda;
read3args *ra;

fh_map fht = fh_map();

static int 
xfs_message_getroot (int, struct xfs_message_getroot*, u_int);

static int 
xfs_message_getnode (int, struct xfs_message_getnode*, u_int);

static int 
xfs_message_getdata (int, struct xfs_message_getdata*, u_int);

static int 
xfs_message_getattr (int, struct xfs_message_getattr*, u_int);

#if 0

static int 
xfs_message_inactivenode (int,struct xfs_message_inactivenode*,u_int);

static int 
xfs_message_putdata (int fd, struct xfs_message_putdata *h, u_int size);

static int
xfs_message_putattr (int fd, struct xfs_message_putattr *h, u_int size);

static int
xfs_message_create (int fd, struct xfs_message_create *h, u_int size);

static int
xfs_message_mkdir (int fd, struct xfs_message_mkdir *h, u_int size);

static int
xfs_message_link (int fd, struct xfs_message_link *h, u_int size);

static int
xfs_message_symlink (int fd, struct xfs_message_symlink *h, u_int size);

static int
xfs_message_remove (int fd, struct xfs_message_remove *h, u_int size);

static int
xfs_message_rmdir (int fd, struct xfs_message_rmdir *h, u_int size);

static int
xfs_message_rename (int fd, struct xfs_message_rename *h, u_int size);

static int
xfs_message_pioctl (int fd, struct xfs_message_pioctl *h, u_int size) ;
#endif /* if 0 */

xfs_message_function rcvfuncs[] = {
NULL,						/* version */
NULL, //(xfs_message_function)xfs_message_wakeup,	/* wakeup */
(xfs_message_function)xfs_message_getroot,	/* getroot */
NULL,						/* installroot */
(xfs_message_function)xfs_message_getnode, 	/* getnode */
NULL,						/* installnode */
(xfs_message_function)xfs_message_getattr,	/* getattr */
NULL,						/* installattr */
(xfs_message_function)xfs_message_getdata,	/* getdata */
NULL,						/* installdata */
NULL, //(xfs_message_function)xfs_message_inactivenode,	/* inactivenode */
NULL,						/* invalidnode */ 
(xfs_message_function)xfs_message_getdata,	/* open */

#if 0
(xfs_message_function)xfs_message_putdata,      /* put_data */
(xfs_message_function)xfs_message_putattr,      /* put attr */
(xfs_message_function)xfs_message_create,       /* create */
(xfs_message_function)xfs_message_mkdir,	/* mkdir */
(xfs_message_function)xfs_message_link,		/* link */
(xfs_message_function)xfs_message_symlink,      /* symlink */
(xfs_message_function)xfs_message_remove,	/* remove */
(xfs_message_function)xfs_message_rmdir,	/* rmdir */
(xfs_message_function)xfs_message_rename,	/* rename */
(xfs_message_function)xfs_message_pioctl,	/* pioctl */
NULL,	                                        /* wakeup_data */
NULL,						/* updatefid */
NULL,						/* advlock */
#endif

NULL						/* gc nodes */

};

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
 
int sfsinfo2xfsnode(xfs_cred cred, sfs_fsinfo *fsi, 
		    ex_getattr3res *res, xfs_msg_node *node) {

  if (fsi->prog != ex_NFS_PROGRAM || fsi->nfs->vers != ex_NFS_V3)
    return -1;  

#if 0
  //MIGHT NOT USE!!!!!
  curdir = new nfs_fh3;
  curfh  = new nfs_fh3;
  *curdir = fsi->nfs->v3->root;
  *curfh  = fsi->nfs->v3->root;
  warn << "**********Assigned curdir to sfsroot****************\n";
  //!!!!!!!!!!!
#endif

  node->handle = fht.gethandle(fsi->nfs->v3->root, *res->attributes);

  warn << "sfsroot becomes node.handle (" 
       << (int)node->handle.a << ","
       << (int)node->handle.b << ","
       << (int)node->handle.c << ","
       << (int)node->handle.d << ")\n";
  
  node->tokens = XFS_ATTR_R; // | ~XFS_DATA_MASK;

  node->attr.valid = XA_V_NONE;
  if (res->attributes->type == NF3REG) {
    XA_SET_MODE(&node->attr, S_IFREG);
    XA_SET_TYPE(&node->attr, XFS_FILE_REG);
    XA_SET_NLINK(&node->attr, res->attributes->nlink);
  } else 
    if (res->attributes->type == NF3DIR) {
      XA_SET_MODE(&node->attr, S_IFDIR);
      XA_SET_TYPE(&node->attr, XFS_FILE_DIR);
      XA_SET_NLINK(&node->attr, res->attributes->nlink);
    } else
      if (res->attributes->type == NF3LNK) {
	XA_SET_MODE(&node->attr, S_IFLNK);
	XA_SET_TYPE(&node->attr, XFS_FILE_LNK);
	XA_SET_NLINK(&node->attr, res->attributes->nlink);
      } else {
	warn << "nfsattr2xfs_attr: default\n";
	abort ();
      }
  XA_SET_SIZE(&node->attr, res->attributes->size);
  XA_SET_UID(&node->attr,res->attributes->uid);
  XA_SET_GID(&node->attr, res->attributes->gid);
  node->attr.xa_mode  |= res->attributes->mode;
  XA_SET_ATIME(&node->attr, res->attributes->atime.seconds);
  XA_SET_MTIME(&node->attr, res->attributes->mtime.seconds);
  XA_SET_CTIME(&node->attr, res->attributes->ctime.seconds);
  XA_SET_FILEID(&node->attr, res->attributes->fileid);

  //HARD CODE ACCESS FOR NOW!! use nfs3_access later
  node->anonrights = nfs_rights2xfs_rights(ACCESS3_READ  | 
					   ACCESS3_LOOKUP | 
					   ACCESS3_EXECUTE, 
					   res->attributes->type, 
					   res->attributes->mode);
  for (int i=0; i<MAXRIGHTS; i++) {
    node->id[i] = cred.pag;
    node->rights[i] = nfs_rights2xfs_rights(ACCESS3_READ  | 
					    ACCESS3_LOOKUP |
					    ACCESS3_EXECUTE, 
					    res->attributes->type, 
					    res->attributes->mode);  
  }

  return 0;
}

int nfsobj2xfsnode(xfs_cred cred, nfs_fh3 obj, ex_getattr3res res, 
		   xfs_msg_node *node) {

  node->handle = fht.gethandle(obj, *res.attributes);
  warn << "nfsfh becomes node.handle (" 
       << (int)node->handle.a << ","
       << (int)node->handle.b << ","
       << (int)node->handle.c << ","
       << (int)node->handle.d << ")\n";

  node->anonrights = XFS_RIGHT_R | XFS_RIGHT_W | XFS_RIGHT_X;
  node->tokens = XFS_ATTR_R; // | ~XFS_DATA_MASK;

  /* node->attr */
  node->attr.valid = XA_V_NONE;
  if (res.attributes->type == NF3REG) {
    XA_SET_MODE(&node->attr, S_IFREG);
    XA_SET_TYPE(&node->attr, XFS_FILE_REG);
    XA_SET_NLINK(&node->attr, res.attributes->nlink);
  } else 
    if (res.attributes->type == NF3DIR) {
      XA_SET_MODE(&node->attr, S_IFDIR);
      XA_SET_TYPE(&node->attr, XFS_FILE_DIR);
      XA_SET_NLINK(&node->attr, res.attributes->nlink);
    } else
      if (res.attributes->type == NF3LNK) {
	XA_SET_MODE(&node->attr, S_IFLNK);
	XA_SET_TYPE(&node->attr, XFS_FILE_LNK);
	XA_SET_NLINK(&node->attr, res.attributes->nlink);
      } else {
	warn << "nfsattr2xfs_attr: default\n";
	abort ();
      }
  XA_SET_SIZE(&node->attr, res.attributes->size);
  XA_SET_UID(&node->attr,res.attributes->uid);
  XA_SET_GID(&node->attr, res.attributes->gid);
  node->attr.xa_mode  |= res.attributes->mode;
  XA_SET_ATIME(&node->attr, res.attributes->atime.seconds);
  XA_SET_MTIME(&node->attr, res.attributes->mtime.seconds);
  XA_SET_CTIME(&node->attr, res.attributes->ctime.seconds);
  XA_SET_FILEID(&node->attr, res.attributes->fileid);

  return 0;
}

int nfsobj2xfsnode(xfs_cred cred, nfs_fh3 obj, 
		   ex_post_op_attr res, xfs_msg_node *node) {

  node->handle = fht.gethandle(obj, *res.attributes);
  warn << "nfsfh becomes node.handle (" 
       << (int)node->handle.a << ","
       << (int)node->handle.b << ","
       << (int)node->handle.c << ","
       << (int)node->handle.d << ")\n";


  node->anonrights = XFS_RIGHT_R | XFS_RIGHT_W | XFS_RIGHT_X;
  node->tokens = XFS_ATTR_R; // | ~XFS_DATA_MASK;

  node->attr.valid = XA_V_NONE;
  if (res.attributes->type == NF3REG) {
    XA_SET_MODE(&node->attr, S_IFREG);
    XA_SET_TYPE(&node->attr, XFS_FILE_REG);
    XA_SET_NLINK(&node->attr, res.attributes->nlink);
  } else 
    if (res.attributes->type == NF3DIR) {
      XA_SET_MODE(&node->attr, S_IFDIR);
      XA_SET_TYPE(&node->attr, XFS_FILE_DIR);
      XA_SET_NLINK(&node->attr, res.attributes->nlink);
    } else
      if (res.attributes->type == NF3LNK) {
	XA_SET_MODE(&node->attr, S_IFLNK);
	XA_SET_TYPE(&node->attr, XFS_FILE_LNK);
	XA_SET_NLINK(&node->attr, res.attributes->nlink);
      } else {
	warn << "nfsattr2xfs_attr: default\n";
	abort ();
      }
  XA_SET_SIZE(&node->attr, res.attributes->size);
  XA_SET_UID(&node->attr,res.attributes->uid);
  XA_SET_GID(&node->attr, res.attributes->gid);
  node->attr.xa_mode  |= res.attributes->mode;
  XA_SET_ATIME(&node->attr, res.attributes->atime.seconds);
  XA_SET_MTIME(&node->attr, res.attributes->mtime.seconds);
  XA_SET_CTIME(&node->attr, res.attributes->ctime.seconds);
  XA_SET_FILEID(&node->attr, res.attributes->fileid);

  return 0;
}

void getrootattr(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, ex_getattr3res *res, clnt_stat err) {

  struct xfs_message_installroot msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  assert(res->status == NFS3_OK);

  if (sfsinfo2xfsnode(h->cred, fsi, res, &msg.node)) {
    warn << "getfsinfo: can't extract rootfh/attributes\n";
    return;
  }
  
#if 0
  //MIGHT NOT USE!!!!!
  curdir_attrres = *res;
  curfh_attrres = *res;
  //!!!!!!!!!!!
#endif

  msg.header.opcode = XFS_MSG_INSTALLROOT;
  h0 = (struct xfs_message_header *)&msg;
  h0_len = sizeof(msg);

  delete fsi;
  xfs_send_message_wakeup_multiple (fd,	h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);

}

void getfsinfo(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, clnt_stat err) {

  assert(fsi->prog == ex_NFS_PROGRAM && fsi->nfs->vers == ex_NFS_V3);

  ex_getattr3res *res = new ex_getattr3res;
  AUTH *auth_default = authunix_create_default ();

  nfsc->call(ex_NFSPROC3_GETATTR, &fsi->nfs->v3->root, res, 
	     wrap (&getrootattr, fd, h, fsi, res), 
	     auth_default);
}

static int
xfs_message_getroot (int fd, struct xfs_message_getroot *h, u_int size)
{
  warn << "get root!!\n";

  sfs_fsinfo *fsi = new sfs_fsinfo;
  sfsc->call(SFSPROC_GETFSINFO, NULL, fsi,
	  wrap (&getfsinfo, fd, h, fsi),
	  NULL, NULL /*, fsinfo_marshall ()*/);

  return 0;
}

void nfs3_lookup(int fd, struct xfs_message_getnode *h, 
	    ex_lookup3res *lres, clnt_stat err) {

  struct xfs_message_installnode msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  ex_post_op_attr a;
  
  if (lres->status != NFS3_OK) {
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    int error = lres->status;

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, error,
				      h0, h0_len, NULL, 0);
    return;
  } else {
    if (lres->resok->obj_attributes.present) 
      a = lres->resok->obj_attributes;
    else if (lres->resok->dir_attributes.present)
      a = lres->resok->dir_attributes;
    else { 
      warn << "lookup: error no attr present\n";
      return;
    }
  }

#if 0
  *curfh = lres->resok->object;
  if (a.attributes->type == NF3DIR)
    *curdir = lres->resok->object;
#endif 
  if (nfsobj2xfsnode(h->cred, lres->resok->object, a, &msg.node)) {
    warn << "lookup: can't extract sfsobj/attributes\n";
    return;
  }

  msg.header.opcode = XFS_MSG_INSTALLNODE;
  msg.parent_handle = h->parent_handle;
  strcpy(msg.name, h->name);
  h0 = (struct xfs_message_header *)&msg;
  h0_len = sizeof(msg);
  
  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);
}

static int
xfs_message_getnode (int fd, struct xfs_message_getnode *h, u_int size)
{
  warn << "get node !! msg.parent_handle (" 
       << (int)h->parent_handle.a << ","
       << (int)h->parent_handle.b << ","
       << (int)h->parent_handle.c << ","
       << (int)h->parent_handle.d << ")\n";
  warn << "file name = " << h->name << "\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_getnode: Can't find parent_handle\n";
    return -1;
  }
  doa = new diropargs3;
  doa->dir = fht.getnh(fht.getcur());
  doa->name = h->name;
  warn << "requesting file name " << doa->name.cstr() << "\n";
  ex_lookup3res *res = new ex_lookup3res;

  nfsc->call(ex_NFSPROC3_LOOKUP, doa, res, 
	     wrap (&nfs3_lookup, fd, h, res));

  return 0;
}

static long blocksize = XFS_DIRENT_BLOCKSIZE;

void flushbuf(write_dirent_args *args) {
  unsigned inc = blocksize - (args->ptr - args->buf);
  xfs_dirent *last = (xfs_dirent *)args->last;

  last->d_reclen += inc;
  if (write (args->fd, args->buf, blocksize) != blocksize)
    warn << errno << ":write\n";
  args->ptr = args->buf;
  args->last = NULL;
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
    if (args->ptr + reclen > args->buf + blocksize)
      flushbuf (args);
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

void nfs3_readdir(int fd, struct xfs_message_getdata *h, ex_readdir3res *res, 
	     clnt_stat err) {
  
  if (res->status == NFS3_OK) {

    struct xfs_message_installdata msg; 
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct write_dirent_args args;

    if (fht.setcur(h->handle)) {
      warn << "nfs3_readdir: Can't find node handle\n";
      return;
    }

    if (nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		       res->resok->dir_attributes, &msg.node)) {
      warn << "lookup: can't extract sfsobj/attributes\n";
      return;
    }
    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;

    //fill in cache_name, cache_handle, flag
    strcpy(msg.cache_name, "dir_file");
    args.fd = open(msg.cache_name, O_CREAT | O_RDWR, 0666); 
    if (args.fd < 0) { 
      warn << strerror(errno) << "(" << errno << ") on args.fd=" << args.fd << "\n";
      return;
    }
  
    fhandle_t cfh;
    if (getfh(msg.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg.cache_handle, &cfh, sizeof(cfh));
    nfsdir2xfsfile(res, &args); 
    if (args.last) 
      flushbuf(&args);
    free (args.buf);
    close(args.fd);
    
    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg;
    h0_len = sizeof(msg);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);

  } else {
    warn << "error: " << strerror(errno) << "(" << errno << ")\n";
    if (res->resfail->present) 
      warn << "dir present\n";
    else warn << "dir not present\n";
  }
}

void nfs3_read(int fd, struct xfs_message_getdata *h, ex_read3res *res, 
	     clnt_stat err) {
  
  if (res->status == NFS3_OK) {

    struct xfs_message_installdata msg; 
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    int cfd;

    if (fht.setcur(h->handle)) {
      warn << "nfs3_read: Can't find node handle\n";
      return;
    }

    if (nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		       res->resok->file_attributes, &msg.node)) {
      warn << "lookup: can't extract sfsobj/attributes\n";
      return;
    }
    msg.node.tokens |= XFS_OPEN_NR;
    strcpy(msg.cache_name,"file");
    cfd = open(msg.cache_name, O_CREAT | O_RDWR, 0666); 
    if (cfd < 0) { 
      warn << strerror(errno) << "(" << errno << ") on cfd=" << cfd << "\n";
      return;
    }
      
    fhandle_t cfh;
    if (getfh(msg.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg.cache_handle, &cfh, sizeof(cfh));
    int err = write(cfd, res->resok->data.base(), res->resok->data.size());
    if (err != (int)res->resok->data.size()) {
      warn << "write error or short write!!\n";
    }

    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg;
    h0_len = sizeof(msg);
    
    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  } else {
    warn << "error: " << strerror(errno) << "(" << errno << ")\n";
    if (res->resfail->present) 
      warn << "file present\n";
    else warn << "file not present\n";
  }
}

static int
xfs_message_getdata (int fd, struct xfs_message_getdata *h, u_int size)
{

  warn << "get data !! msg.handle (" 
       << (int)h->handle.a << ","
       << (int)h->handle.b << ","
       << (int)h->handle.c << ","
       << (int)h->handle.d << ")\n";
  
  if (fht.setcur(h->handle)) {
    warn << "nfs3_readdir: Can't find node handle\n";
    return -1;
  }

  if (fht.getattr(fht.getcur()).type == NF3DIR) {
    rda = new readdir3args;
    rda->dir = fht.getnh(fht.getcur());
    rda->cookie = 0;
    rda->cookieverf = cookieverf3();
    rda->count = 2000; //GUESS!! should use dtpres

    ex_readdir3res *rdres = new ex_readdir3res;
    nfsc->call(ex_NFSPROC3_READDIR, rda, rdres,
	       wrap (&nfs3_readdir, fd, h, rdres));
  } else
    if (fht.getattr(fht.getcur()).type == NF3REG) {

      ra = new read3args;
      ra->file = fht.getnh(fht.getcur());
      ra->offset = 0;
      ra->count = NFS_MAXDATA;

      ex_read3res *rres = new ex_read3res;
      nfsc->call(ex_NFSPROC3_READ, ra, rres,
		 wrap(&nfs3_read, fd, h, rres));
    }
  
  return 0;
}

void nfs3_getattr(int fd, struct xfs_message_getattr *h,
		  ex_getattr3res *res, clnt_stat err) {

  assert(res->status == NFS3_OK);

  struct xfs_message_installattr msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  if (nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), *res, &msg.node)) {
    warn << "lookup: can't extract sfsobj/attributes\n";
    return;
  }

#if 0
  curfh_attrres = *res;
  if (res->attributes->type == NF3DIR)
    curdir_attrres = *res;
#endif

  msg.header.opcode = XFS_MSG_INSTALLATTR;
  h0 = (struct xfs_message_header *)&msg;
  h0_len = sizeof(msg);
  
  xfs_send_message_wakeup_multiple (fd,	h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);
}

static int
xfs_message_getattr (int fd, struct xfs_message_getattr *h, u_int size)
{
  warn << "get attr !!\n";
  warn << "msg.handle ("
       << (int)h->handle.a << ","
       << (int)h->handle.b << ","
       << (int)h->handle.c << ","
       << (int)h->handle.d << ")\n";

  if (fht.setcur(h->handle)) {
    //getnode!!
    warn << "get node from getattr!!\n";
  } else { 
    fh = new nfs_fh3; 
    *fh = fht.getnh(fht.getcur());
    ex_getattr3res *res = new ex_getattr3res;
    nfsc->call(ex_NFSPROC3_GETATTR, fh, res, 
	       wrap(&nfs3_getattr, fd, h, res));
  }
  
  return 0;
}

