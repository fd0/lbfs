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
write3args *wa;
create3args *ca;
mkdir3args *ma;
setattr3args *sa;

static char iobuf[NFS_MAXDATA];

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
(xfs_message_function)xfs_message_inactivenode,	/* inactivenode */
NULL,						/* invalidnode */ 
(xfs_message_function)xfs_message_getdata,	/* open */
(xfs_message_function)xfs_message_putdata,      /* put_data */
(xfs_message_function)xfs_message_putattr,      /* put attr */
(xfs_message_function)xfs_message_create,       /* create */
(xfs_message_function)xfs_message_mkdir,	/* mkdir */
(xfs_message_function)xfs_message_link,		/* link */
(xfs_message_function)xfs_message_symlink,      /* symlink */
(xfs_message_function)xfs_message_remove,	/* remove */
(xfs_message_function)xfs_message_rmdir,	/* rmdir */
#if 0
(xfs_message_function)xfs_message_rename,	/* rename */
(xfs_message_function)xfs_message_pioctl,	/* pioctl */
NULL,	                                        /* wakeup_data */
NULL,						/* updatefid */
NULL,						/* advlock */
NULL						/* gc nodes */
#endif

};

void getrootattr(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, ex_getattr3res *res, clnt_stat err) {

  struct xfs_message_installroot msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  assert(res->status == NFS3_OK);
  
  //char uname = new char[20];
  //getpw(getuid(), uname);

  warn << "uid = " << getuid() << "\n"; // "username = " << uname << "\n";

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
	     wrap (&getrootattr, fd, h, fsi, res));
	     //	     auth_default);
}

int xfs_message_getroot (int fd, struct xfs_message_getroot *h, u_int size)
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

int xfs_message_getnode (int fd, struct xfs_message_getnode *h, u_int size)
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

    if (nfsdir2xfsfile(res, &args) < 0)
      return;
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
    ra->file = fht.getnh(fht.getcur());
    ra->offset += res->resok->count;
    ra->count = NFS_MAXDATA;
    
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
    
    //if (fht.getcur() == fht.getmax()) //node did not exist before
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *res->resok->file_attributes.attributes, &msg.node);

    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R 
                     | XFS_OPEN_NW | XFS_DATA_W; //This line is a hack...need to get read access 

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

int xfs_message_getdata (int fd, struct xfs_message_getdata *h, u_int size)
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

int xfs_message_getattr (int fd, struct xfs_message_getattr *h, u_int size)
{
  warn << "get attr !!\n";
  warn << "msg.handle ("
       << (int)h->handle.a << ","
       << (int)h->handle.b << ","
       << (int)h->handle.c << ","
       << (int)h->handle.d << ")\n";

  if (fht.setcur(h->handle)) {
    //getnode!!
    warn << "xfs_getattr: Can't find node handle\n";
    return -1;
  } 

  fh = new nfs_fh3; 
  *fh = fht.getnh(fht.getcur());
  ex_getattr3res *res = new ex_getattr3res;
  nfsc->call(ex_NFSPROC3_GETATTR, fh, res, 
	     wrap(&nfs3_getattr, fd, h, res));
  
  return 0;
}


int xfs_message_inactivenode (int,struct xfs_message_inactivenode*,u_int) {

  warn << "inactivenode !!\n";

  return 0;

}

void write_data(int fd, struct xfs_message_putdata *h, 
		int ffd, ex_write3res *res, clnt_stat cl_err) {

  int err = 0;

  if (res != NULL) {
    if (res->status == NFS3_OK)
      wa->offset += res->resok->count;
    else return;
  }

  if (lseek (ffd, (long) wa->offset, L_SET) >= 0)
    err = read(ffd, iobuf, NFS_MAXDATA);
  else {
    warn << strerror(errno) << "(" << errno << "): write_data\n";
    return;
  }

  if (err == -1) {
    warn << strerror(errno) << "(" << errno << ") write_data\n";
    close(ffd);
    xfs_send_message_wakeup (fd, h->header.sequence_num, err);
  } else
    if (err == 0) { //end of file
      close(ffd);
      xfs_send_message_wakeup (fd, h->header.sequence_num, err);
    } else //maybe more to read
      if (err <= NFS_MAXDATA) {  
	wa->count = err;
	wa->data.setsize(err);
	memcpy (wa->data.base(), iobuf, err);	
	res = new ex_write3res;
	nfsc->call(ex_NFSPROC3_WRITE, wa, res,
		   wrap(&write_data, fd, h, ffd, res));
      }      
}

int xfs_message_putdata (int fd, struct xfs_message_putdata *h, u_int size) {
  
  warn << "putdata !!\n";

  if (fht.setcur(h->handle)) {
    warn << "xfs_putdata: Can't find node handle\n";
    return -1;
  } 

  char *fname;
  assign_filename(fname, fht.getcur());
  int ffd = open(fname, O_RDONLY, 0666);
  
  wa = new write3args;
  wa->file = fht.getnh(fht.getcur());
  wa->offset = 0;
  wa->stable = FILE_SYNC;
  write_data(fd, h, ffd, NULL, clnt_stat(0));

  return 0;
}

void nfs3_setattr(int fd, struct xfs_message_putattr *h, ex_wccstat3 *res,
		  clnt_stat err) {
  
  if (res->status != -1) {
 
   assert(res->wcc->after.present);

   struct xfs_message_installattr msg;
   struct xfs_message_header *h0 = NULL;
   size_t h0_len = 0;

   if (fht.setcur(h->handle)) {
     warn << "nfs3_read: Can't find node handle\n";
     return;
   }
   nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		  *res->wcc->after.attributes, &msg.node);

   msg.header.opcode = XFS_MSG_INSTALLATTR;
   h0 = (struct xfs_message_header *)&msg;
   h0_len = sizeof(msg);

   xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				     h0, h0_len, NULL, 0);

  } else 
    warn << strerror(errno) << "(" << errno << "): nfs3_setattr\n";

}

int xfs_message_putattr (int fd, struct xfs_message_putattr *h, u_int size) {
  
  warn << "putattr !!\n";

  if (fht.setcur(h->handle)) {
    warn << "nfs3_read: Can't find node handle\n";
    return -1;
  }

  sa = new setattr3args;
  sa->object = fht.getnh(fht.getcur());
  xfsattr2nfsattr(h->attr, &sa->new_attributes);
  sa->guard.set_check(true);
  if (sa->guard.check)
    sa->guard.ctime->seconds = h->attr.xa_ctime;

  ex_wccstat3 *res = new ex_wccstat3;
  nfsc->call(ex_NFSPROC3_SETATTR, sa, res, 
	     wrap(&nfs3_setattr, fd, h, res));

  return 0;
}

void nfs3_create(int fd, struct xfs_message_create *h, ex_diropres3 *res,
		 clnt_stat err) {

  if (res->status == NFS3_OK) {

    struct xfs_message_installdata msg1; //change content of parent dir
    struct xfs_message_installnode msg2; //new file node
    struct xfs_message_installdata msg3; //new file content (null)
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    struct xfs_message_header *h2 = NULL;
    size_t h2_len = 0;

    assert(res->resok->obj.present && res->resok->obj_attributes.present);
    //create new file
    nfsobj2xfsnode(h->cred, *(res->resok->obj.handle), 
		   *(res->resok->obj_attributes.attributes), &msg2.node);
    int new_fd = assign_file(msg3.cache_name, fht.getcur());
    if (new_fd < 0)
      return;
    close(new_fd);
  
    fhandle_t new_fh;
    if (getfh(msg3.cache_name, &new_fh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg3.cache_handle, &new_fh, sizeof(new_fh));
    
    //write new direntry to parent dirfile (do a readdir or just append that entry?)
    if (fht.setcur(h->parent_handle)) {
      warn << "nfs3_read: Can't find parent handle\n";
      return;
    }
    //struct write_dirent_args args;
    assign_filename(msg1.cache_name, fht.getcur());
    int dir_fd = open(msg1.cache_name, O_CREAT | O_RDWR | O_APPEND, 0666);
    if (nfsdirent2xfsfile(dir_fd, h->name, (*res->resok->obj_attributes.attributes).fileid) < 0)
      return;
    close(dir_fd);
    //msg1.node.tokens = same as parent dir's

    assert(res->resok->dir_wcc.after.present);
    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(res->resok->dir_wcc.after.attributes), &msg1.node);

    //get new file data ??
    msg1.flag = 0;  
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg1;
    h0_len = sizeof(msg1);

    msg2.node.tokens |= XFS_ATTR_R 
      | XFS_OPEN_NW | XFS_OPEN_NR
      | XFS_DATA_R  | XFS_DATA_W;  //override nfsobj2xfsnode?
    msg2.parent_handle = h->parent_handle;
    strlcpy (msg2.name, h->name, sizeof(msg2.name));
  
    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *)&msg2;
    h1_len = sizeof(msg2);

    msg3.node          = msg2.node;
    msg3.flag = 0;
    msg3.header.opcode = XFS_MSG_INSTALLDATA;

    h2 = (struct xfs_message_header *)&msg3;
    h2_len = sizeof(msg3);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, h2, h2_len,
				      NULL, 0);
  } else 
    warn << strerror(errno) << "(" << errno << "): nfs3_create\n";

}

int xfs_message_create (int fd, struct xfs_message_create *h, u_int size) {
  
  warn << "create !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_create: Can't find parent_handle\n";
    return -1;
  }

  ca = new create3args;
  ca->where.dir = fht.getnh(fht.getcur());
  ca->where.name = h->name;
  ca->how.set_mode(GUARDED);
  if (ca->how.mode == UNCHECKED || ca->how.mode == GUARDED)
    xfsattr2nfsattr(h->attr, &(*ca->how.obj_attributes));
  else warn << "xfs_message_create: create mode not UNCHECKED or GUARDED\n";

  ex_diropres3 *res = new ex_diropres3;
  nfsc->call(ex_NFSPROC3_CREATE, ca, res, 
	     wrap(&nfs3_create, fd, h, res));

  return 0;
}

void nfs3_mkdir(int fd, struct xfs_message_mkdir *h, ex_diropres3 *res,
	       clnt_stat err) {

  if (res->status == NFS3_OK) {

    struct xfs_message_installdata msg1; //change content of parent dir
    struct xfs_message_installnode msg2; //new dir node
    struct xfs_message_installdata msg3; //new dir content (null)
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    struct xfs_message_header *h2 = NULL;
    size_t h2_len = 0;

    assert(res->resok->obj.present && res->resok->obj_attributes.present);
    //create new dirfile
    nfsobj2xfsnode(h->cred, *(res->resok->obj.handle), 
		   *(res->resok->obj_attributes.attributes), &msg2.node);
    int new_fd = assign_file(msg3.cache_name, fht.getcur());
    if (new_fd < 0)
      return;
    close(new_fd);
  
    fhandle_t new_fh;
    if (getfh(msg3.cache_name, &new_fh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg3.cache_handle, &new_fh, sizeof(new_fh));
    
    //write new direntry to parent dirfile (do a readdir or just append that entry?)
    if (fht.setcur(h->parent_handle)) {
      warn << "nfs3_read: Can't find node handle\n";
      return;
    }
    //struct write_dirent_args args;
    assign_filename(msg1.cache_name, fht.getcur());
    int dir_fd = open(msg1.cache_name, O_CREAT | O_RDWR | O_APPEND, 0666);
    if (nfsdirent2xfsfile(dir_fd, h->name, (*res->resok->obj_attributes.attributes).fileid) < 0)
      return;
    close(dir_fd);
    //msg1.node.tokens = same as parent dir's

    assert(res->resok->dir_wcc.after.present);
    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(res->resok->dir_wcc.after.attributes), &msg1.node);

    //get new file data ??
    msg1.flag = 0;  
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg1;
    h0_len = sizeof(msg1);

    //msg2.node.tokens = same as parent dir's
    msg2.parent_handle = h->parent_handle;
    strlcpy (msg2.name, h->name, sizeof(msg2.name));
  
    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *)&msg2;
    h1_len = sizeof(msg2);

    msg3.node          = msg2.node;
    msg3.flag = 0;
    msg3.header.opcode = XFS_MSG_INSTALLDATA;

    h2 = (struct xfs_message_header *)&msg3;
    h2_len = sizeof(msg3);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, h2, h2_len,
				      NULL, 0);
  } else 
    warn << strerror(errno) << "(" << errno << "): nfs3_mkdir\n";

}

int xfs_message_mkdir (int fd, struct xfs_message_mkdir *h, u_int size) {
  
  warn << "mkdir !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
    return -1;
  }
  
  ma = new mkdir3args;
  ma->where.dir = fht.getnh(fht.getcur());
  ma->where.name = h->name;
  xfsattr2nfsattr(h->attr, &ma->attributes);

  ex_diropres3 *res = new ex_diropres3;
  nfsc->call(ex_NFSPROC3_MKDIR, ma, res, 
	     wrap(&nfs3_mkdir, fd, h, res));

  return 0;
}

int xfs_message_link(int fd, struct xfs_message_link *h, u_int size) {

  return 0;
}

int xfs_message_symlink(int fd, struct xfs_message_symlink *h, u_int size) {

  return 0;
}

void remove(int fd, struct xfs_message_remove *h, ex_lookup3res *lres,
	    ex_wccstat3 *wres, clnt_stat err) {

  if (lres->status == NFS3_OK) {

    assert(wres->wcc->after.present);

    struct xfs_message_installdata msg1;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installattr msg2;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    ex_post_op_attr a;

    //remove entry from parent dir
    if (fht.setcur(h->parent_handle)) {
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
      return;
    }
    
    assign_filename(msg1.cache_name, fht.getcur());
    fhandle_t cfh;
    if (getfh(msg1.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg1.cache_handle, &cfh, sizeof(cfh));

    int dir_fd = open(msg1.cache_name, O_RDWR | O_SHLOCK, 0666);
    if (xfsfile_rm_dirent(dir_fd, h->name) < 0)
      return;
    close(dir_fd);

    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(wres->wcc->after.attributes), &msg1.node);

    msg1.flag = XFS_ID_INVALID_DNLC;
    msg1.node.tokens |=  XFS_DATA_R;
    
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg1;
    h0_len = sizeof(msg1);
 
    //if the entry being removed from parent is still being referenced (nlink > 1)
    //update its attr, otherwise, evict from cache
    
    if (lres->resok->obj_attributes.present) 
      a = lres->resok->obj_attributes;
    else if (lres->resok->dir_attributes.present)
      a = lres->resok->dir_attributes;
    else { 
      warn << "lookup in remove: error no attr present\n";
      return;
    }

    if (a.attributes->nlink > 2) {
      msg2.header.opcode = XFS_MSG_INSTALLATTR;
      --a.attributes->nlink;
      nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		     *(a.attributes), &msg1.node);

      //msg2.node.tokens   = limbo_entry->tokens;
    
      //  if data is not being used in the kernel...
      //  msg2.node.tokens &= ~XFS_DATA_MASK;
    
      h1 = (struct xfs_message_header *)&msg2;
      h1_len = sizeof(msg2);
    
      
      xfs_send_message_wakeup_multiple (fd,
					h->header.sequence_num,
					0,
					h0, h0_len,
					h1, h1_len,
					NULL, 0);

    } else {
      //TODO: evict from cache..
      
      xfs_send_message_wakeup_multiple (fd,
					h->header.sequence_num,
					0,
					h0, h0_len,
					NULL, 0);
    }
      
  } else 
    warn << strerror(errno) << "(" << errno << "): nfs3_lookup in remove\n";

}

void nfs3_remove(int fd, struct xfs_message_remove *h, ex_lookup3res *lres,
		 clnt_stat err) {

  if (lres->status != -1) {

    //lookup entry's filehandle and attr
    //if the entry being removed from parent is still being referenced (nlink > 1)
    //update its attr
    
    if (fht.setcur(h->parent_handle)) {
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
      return;
    }

    doa = new diropargs3;
    doa->dir = fht.getnh(fht.getcur());
    doa->name = h->name;

    ex_wccstat3 *wres = new ex_wccstat3;
    nfsc->call(ex_NFSPROC3_REMOVE, doa, wres,
	       wrap(&remove, fd, h, lres, wres));

  } else
    warn << "nfs3_remove: lres->status = " << lres->status << "\n";

}

int xfs_message_remove(int fd, struct xfs_message_remove *h, u_int size) {

  warn << "remove !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
    return -1;
  }

  doa = new diropargs3;
  doa->dir = fht.getnh(fht.getcur());
  doa->name = h->name;
  warn << "requesting file name " << doa->name.cstr() << "\n";
  ex_lookup3res *res = new ex_lookup3res;
    
  nfsc->call(ex_NFSPROC3_LOOKUP, doa, res, 
	     wrap (&nfs3_remove, fd, h, res));

  return 0;
}

void nfs3_rmdir(int fd, struct xfs_message_rmdir *h, ex_lookup3res *lres,
		 clnt_stat err) {

  if (lres->status != -1) {

    //lookup entry's filehandle and attr
    //if the entry being removed from parent is still being referenced (nlink > 1)
    //update its attr
    
    if (fht.setcur(h->parent_handle)) {
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
      return;
    }

    doa = new diropargs3;
    doa->dir = fht.getnh(fht.getcur());
    doa->name = h->name;

    ex_wccstat3 *wres = new ex_wccstat3;
    nfsc->call(ex_NFSPROC3_RMDIR, doa, wres,
	       wrap(&remove, fd, (struct xfs_message_remove *)h, lres, wres));

  } else
    warn << "nfs3_rmdir: lres->status = " << lres->status << "\n";

}

int xfs_message_rmdir(int fd, struct xfs_message_rmdir *h, u_int size) {

  warn << "rmdir !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
    return -1;
  }

  doa = new diropargs3;
  doa->dir = fht.getnh(fht.getcur());
  doa->name = h->name;
  warn << "requesting file name " << doa->name.cstr() << "\n";
  ex_lookup3res *res = new ex_lookup3res;
    
  nfsc->call(ex_NFSPROC3_LOOKUP, doa, res, 
	     wrap (&nfs3_rmdir, fd, h, res));


  return 0;
}
