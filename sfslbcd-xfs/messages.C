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

nfs_fh3 *fh;
diropargs3 *doa;
readdir3args *rda;
read3args *ra;

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
 
void getrootattr(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, ex_getattr3res *res, clnt_stat err) {

  struct xfs_message_installroot msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  assert(res->status == NFS3_OK);

  nfsobj2xfsnode(h->cred, fsi->nfs->v3->root, *res->attributes, &msg.node);
  
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

  nfsobj2xfsnode(h->cred, lres->resok->object, *a.attributes, &msg.node);

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

int assign_dirname(char *dname, int index) {
  return snprintf(dname, MAXPATHLEN, "cache/%02X", index / 0x100);
}

int assign_filename(char *fname, int index) {
  return snprintf(fname, MAXPATHLEN, "cache/%02X/%02X", 
		  index / 0x100, index % 0x100);
}

int assign_file(char *fname, int index) { //move this to a cache class soon
  
  int fd;

  assign_filename(fname, index);
  fd = open(fname, O_CREAT | O_RDWR | O_TRUNC, 0666); 
  if (fd < 0) { 
    if (errno == ENOENT) {
      char *dname = new char[MAXPATHLEN];
      assign_dirname(dname, index);
      warn << "Creating dir: " << dname << "\n";
      if (mkdir(dname, 0777) < 0) {
	warn << strerror(errno) << "(" << errno << ") mkdir " << dname << "\n";
	return -1;
      }
      fd = open(fname, O_CREAT | O_RDWR | O_TRUNC, 0666); 
      if (fd < 0) {
	warn << strerror(errno) << "(" << errno << ") on file =" << fname << "\n";
	return -1;
      }
    } else {
      warn << strerror(errno) << "(" << errno << ") on file =" << fname << "\n";
      return -1;
    }
  }
    
  return fd;
}

void write_dirfile(int fd, struct xfs_message_getdata *h, ex_readdir3res *res,
		   write_dirent_args args, struct xfs_message_installdata msg, 
		   clnt_stat cl_err) {

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    nfsdir2xfsfile(res, &args); 
    if (args.last) 
      flushbuf(&args);
    free (args.buf);

    if (!res->resok->reply.eof) {
      //rda->dir = fht.getnh(fht.getcur());
      entry3 *e = res->resok->reply.entries;
      while (e->nextentry != NULL) e = e->nextentry;
      rda->cookie = e->cookie;
      rda->cookieverf = res->resok->cookieverf;
      //rda->count = 2000; //GUESS!! should use dtpres

      ex_readdir3res *rdres = new ex_readdir3res;
      nfsc->call(ex_NFSPROC3_READDIR, rda, rdres,
		 wrap (&write_dirfile, fd, h, rdres, args, msg));
    } else {

      close(args.fd);

      msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *)&msg;
      h0_len = sizeof(msg);
      
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);

    }

}

void nfs3_readdir(int fd, struct xfs_message_getdata *h, ex_readdir3res *res, 
	     clnt_stat err) {
  
  if (res->status == NFS3_OK) {

    struct xfs_message_installdata msg; 
    struct write_dirent_args args;

    if (fht.setcur(h->handle)) {
      warn << "nfs3_readdir: Can't find node handle\n";
      return;
    }

    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *res->resok->dir_attributes.attributes, &msg.node);

    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;

    //fill in cache_name, cache_handle, flag
    //strcpy(msg.cache_name, "dir_file"); 
    args.fd = assign_file(msg.cache_name, fht.getcur());
    if (args.fd < 0) 
      return;
  
    fhandle_t cfh;
    if (getfh(msg.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg.cache_handle, &cfh, sizeof(cfh));
    write_dirfile(fd, h, res, args, msg, clnt_stat(0));
    
  } else {
    warn << "error: " << strerror(errno) << "(" << errno << ")\n";
    if (res->resfail->present) 
      warn << "dir present\n";
    else warn << "dir not present\n";
  }
}

void write_file(int fd, struct xfs_message_getdata *h, ex_read3res *res, 
		int cfd, struct xfs_message_installdata msg, clnt_stat cl_err) {

  int err = write(cfd, res->resok->data.base(), res->resok->data.size());
  if (err != (int)res->resok->data.size()) {
    warn << "write error or short write!!\n";
  }

  if (!res->resok->eof) {
    //ra->file = fht.getnh(fht.getcur());
    ra->offset += res->resok->count;
    //ra->count = NFS_MAXDATA;
    
    ex_read3res *rres = new ex_read3res;
    nfsc->call(ex_NFSPROC3_READ, ra, rres,
	       wrap(&write_file, fd, h, rres, cfd, msg));
  } else {

    close(cfd);

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg;
    h0_len = sizeof(msg);
    
    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  }
  
}

void nfs3_read(int fd, struct xfs_message_getdata *h, ex_read3res *res, 
	     clnt_stat err) {
  
  if (res->status == NFS3_OK && res->resok->file_attributes.present) {

    int cfd;
    struct xfs_message_installdata msg; 
 
    if (fht.setcur(h->handle)) {
      warn << "nfs3_read: Can't find node handle\n";
      return;
    }

    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *res->resok->file_attributes.attributes, &msg.node);

    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;
    //strcpy(msg.cache_name,"file");    
    cfd = assign_file(msg.cache_name, fht.getcur());
    if (cfd < 0) 
      return;
      
    fhandle_t cfh;
    if (getfh(msg.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg.cache_handle, &cfh, sizeof(cfh));
    write_file(fd, h, res, cfd, msg, clnt_stat(0));
  } else {
    warn << "error: " << strerror(errno) << "(" << errno << ")\n";
    if (res->resfail->present) 
      warn << "dir present\n";
    else warn << "dir not present\n";
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

  nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), *res->attributes, &msg.node);

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









