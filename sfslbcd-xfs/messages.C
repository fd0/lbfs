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

/* Non-volatile File System Info */
ex_fsinfo3resok fsinfo;

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
(xfs_message_function)xfs_message_rename,	/* rename */
#if 0
(xfs_message_function)xfs_message_pioctl,	/* pioctl */
NULL,	                                        /* wakeup_data */
NULL,						/* updatefid */
NULL,						/* advlock */
NULL						/* gc nodes */
#endif

};

void lbfs_condwrite(ref<condwrite3args> cwa, clnt_stat err);
void normal_read(ref<getfp_args> ga, uint64 offset, uint32 count);
void nfs3_rmdir(int fd, struct xfs_message_rmdir *h, ref<ex_lookup3res> lres,
		clnt_stat err);

void reply_err(int fd, u_int seqnum, int err) {
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  
  xfs_send_message_wakeup_multiple (fd, seqnum, err, h0, h0_len, NULL, 0);
}

void getrootattr(int fd, struct xfs_message_getroot *h, ref<sfs_fsinfo> fsi, ref<ex_getattr3res> res, time_t rqtime, clnt_stat err) {

  struct xfs_message_installroot msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  assert(res->status == NFS3_OK);
  
  warn << "uid = " << getuid() << "\n"; 

  nfsobj2xfsnode(h->cred, fsi->nfs->v3->root, *res->attributes, rqtime, &msg.node);
  
  msg.header.opcode = XFS_MSG_INSTALLROOT;
  h0 = (struct xfs_message_header *)&msg;
  h0_len = sizeof(msg);

  xfs_send_message_wakeup_multiple (fd,	h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);
}

void nfs3_fsinfo(int fd, struct xfs_message_getroot *h, ref<sfs_fsinfo> fsi, 
		 ref<ex_fsinfo3res> res, clnt_stat err) {

  assert(res->status == NFS3_OK); 

  fsinfo = *res->resok;

  ref<ex_getattr3res> ares = New refcounted<ex_getattr3res>;
  //AUTH *auth_default = authunix_create_default ();

  nfsc->call(lbfs_NFSPROC3_GETATTR, &fsi->nfs->v3->root, ares, 
	     wrap (&getrootattr, fd, h, fsi, ares, timenow));
  //	     auth_default);  
}

void sfs_getfsinfo(int fd, struct xfs_message_getroot *h, ref<sfs_fsinfo> fsi, clnt_stat err) {

  assert(fsi->prog == ex_NFS_PROGRAM && fsi->nfs->vers == ex_NFS_V3);
  //x->compress ();
  ref<ex_fsinfo3res> res = New refcounted<ex_fsinfo3res>;

  nfsc->call(lbfs_NFSPROC3_FSINFO, &fsi->nfs->v3->root, res,
	     wrap(&nfs3_fsinfo, fd, h, fsi, res));
}

int xfs_message_getroot (int fd, struct xfs_message_getroot *h, u_int size)
{
  warn << "get root!!\n";

  ref<sfs_fsinfo> fsi = New refcounted<sfs_fsinfo>;
  sfsc->call(SFSPROC_GETFSINFO, NULL, fsi,
	  wrap (&sfs_getfsinfo, fd, h, fsi), NULL, NULL);

  return 0;
}

void nfs3_lookup(int fd, struct xfs_message_getnode *h, 
		 ref<ex_lookup3res> lres, time_t rqtime, clnt_stat err) {

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

  nfsobj2xfsnode(h->cred, lres->resok->object, *a.attributes, rqtime, &msg.node);

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
  
  diropargs3 doa;
  doa.dir = fht.getnh(fht.getcur());
  doa.name = h->name;
  warn << "requesting file name " << doa.name.cstr() << "\n";
  ref<ex_lookup3res> res = New refcounted<ex_lookup3res>;
  
  nfsc->call(lbfs_NFSPROC3_LOOKUP, &doa, res, 
	     wrap (&nfs3_lookup, fd, h, res, timenow));

  return 0;
}

void write_dirfile(int fd, struct xfs_message_getdata *h, ref<ex_readdir3res> res,
		   write_dirent_args args, struct xfs_message_installdata msg, 
		   clnt_stat cl_err) {

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    if (nfsdir2xfsfile(res, &args) < 0) {
      delete res;
      return;
    }

    if (args.last) 
      flushbuf(&args);
    free (args.buf);

    if (!res->resok->reply.eof) {
      readdir3args rda;
      rda.dir = fht.getnh(fht.getcur());

      entry3 *e = res->resok->reply.entries;
      while (e->nextentry != NULL) e = e->nextentry;
      rda.cookie = e->cookie;
      rda.cookieverf = res->resok->cookieverf;
      rda.count = fsinfo.dtpref;
      //delete e; not creating e, got it from res

      ref<ex_readdir3res> rdres = New refcounted<ex_readdir3res>;
      nfsc->call(lbfs_NFSPROC3_READDIR, &rda, rdres,
		 wrap (&write_dirfile, fd, h, rdres, args, msg));
    } else {

      close(args.fd);

      fht.setopened(true);

      msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *)&msg;
      h0_len = sizeof(msg);
      
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);

    }
}

void nfs3_readdir(int fd, struct xfs_message_getdata *h, ref<ex_readdir3res> res, 
		  time_t rqtime, clnt_stat err) {
  
  if (!err && res->status == NFS3_OK) {

    struct xfs_message_installdata msg; 
    struct write_dirent_args args;

    if (fht.setcur(h->handle)) {
      warn << "nfs3_readdir: Can't find node handle\n";
      return;
    }

    ex_fattr3 attr = *res->resok->dir_attributes.attributes;
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   attr, rqtime, &msg.node);
    fht.set_ltime(attr.mtime, attr.ctime);

    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;

    //fill in cache_name, cache_handle, flag
    strcpy(msg.cache_name, fht.getcache_name());
    args.fd = open(msg.cache_name, O_CREAT | O_RDWR | O_TRUNC, 0666); 

    if (args.fd < 0) {
      return;
    }
  
    fhandle_t cfh;
    if (getfh(msg.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg.cache_handle, &cfh, sizeof(cfh));
    write_dirfile(fd, h, res, args, msg, clnt_stat(0));
    
  } else {
    warn << "error: " << strerror(errno) << "(" << errno << ")\n";
    reply_err(fd, h->header.sequence_num, res->status);
  }
}

void write_file(ref<getfp_args> ga, uint64 offset, uint32 count, 
		ref<ex_read3res> res)
{

  warn << "filename = " << ga->out_fname << " offset = " << offset << "\n";
  int out_fd = open(ga->out_fname, O_CREAT | O_WRONLY, 0666);
  if (out_fd < 0) {
    warn << "write_file1: " << ga->out_fname << " " <<  strerror(errno) << "\n";
    return;
  }

  int err;
  if ((err = lseek(out_fd, offset, SEEK_SET)) < 0) {
    warn << "write_file2: " << ga->out_fname << " " << strerror(errno) << "\n";
    return;
  }
  
  if ((err = write(out_fd, res->resok->data.base(), res->resok->data.size())) < 0) {
    warn << "write_file3: " << ga->out_fname << " " << strerror(errno) << "\n";
    return;
  } else
    if (err != (int)res->resok->data.size()) {
      warn << "write error or short write!!\n";
      return;
    }
  close(out_fd);

  if (res->resok->count < count)
    normal_read(ga, offset+res->resok->count, count-res->resok->count);
  else ga->blocks_written++;
}

void nfs3_read(ref<getfp_args> ga, uint64 offset, uint32 count, 
               ref<ex_read3res> res, clnt_stat err) 
{
  if (!err && res->status == NFS3_OK && res->resok->file_attributes.present) {

    write_file(ga, offset, count, res); 

    if (ga->blocks_written == ga->total_blocks && ga->eof) {

      //add chunk to the database
      vec<lbfs_chunk *> cvp;
      if (chunk_file(CHUNK_SIZES(0), &cvp, (char const*)ga->msg.cache_name) < 0) {
	warn << strerror(errno) << "(" << errno << "): nfs3_read(chunkfile)\n";
	delete res;
	return;
      }
      for (uint i=0; i<cvp.size(); i++) {
	warn << "adding fp = " << cvp[i]->fingerprint << " to lbfsdb\n";
	cvp[i]->loc.set_fh(fht.getnh(fht.getcur()));
	lbfsdb.add_entry(cvp[i]->fingerprint, &(cvp[i]->loc));
      }
      lbfsdb.sync();
      fht.setopened(true);
	
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
	
      ga->msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *)&(ga->msg);
      h0_len = sizeof(ga->msg);
	
      xfs_send_message_wakeup_multiple (ga->fd, ga->h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);
    }
  } else {
    warn << "nfs3_read: " << strerror(res->status) << "\n";
    reply_err(ga->fd, ga->h->header.sequence_num, res->status);
  }
}

void normal_read(ref<getfp_args> ga, uint64 offset, uint32 count)
{
  if (fht.setcur(ga->h->handle)) {
    warn << "normal_read: Can't find node handle\n";
    return;
  }
 
  read3args ra;
  ra.file = fht.getnh(fht.getcur());
  ra.offset = offset;
  ra.count = count;
  
  ref<ex_read3res> rres = New refcounted<ex_read3res>;
  nfsc->call(lbfs_NFSPROC3_READ, &ra, rres,
	     wrap(&nfs3_read, ga, offset, count, rres));
}

void compose_file(ref<getfp_args> ga, ref<lbfs_getfp3res> res) {

  int err, chfd, out_fd;
  uint64 offset = ga->offset; //chunk position

  fp_db::iterator *ci = NULL;
  bool found = false;
  //  ga->blocks_written = 0;
  nfs_fh3 fh;
  lbfs_chunk_loc c;

  for (uint i=0; i<res->resok->fprints.size(); i++) {
    found = false;
    unsigned char buf[res->resok->fprints[i].count];
    //find matching fp in the database
    //if found, write that chunk to the file,
    //otherwise, send for a normal read of that chunk
    if (!lbfsdb.get_iterator(res->resok->fprints[i].fingerprint, &ci)) {
      if (!ci) warn << "ci is NULL\n";
      if (ci && !(ci->get(&c))) {
	do {
	  found = true;
	  c.get_fh(fh);

	  if (c.count() != res->resok->fprints[i].count) {
	    warn << "chunk size != size from server..\n";
	    found = false;
	  } else {
	    //read chunk c.pos() to c.count() from fh into buf 
	    if (fht.setcur(fh)) {
	      warn << "compose_file: null fh or Can't find node handle\n";
	      return;
	    }
	    warn << "reading chunks from " << fht.getcache_name() << "\n";
	    chfd = open(fht.getcache_name(), O_RDONLY, 0666);
	    if (chfd < 0) {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;
	    }
	    if (lseek(chfd, c.pos(), SEEK_SET) < 0) {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;	    
	    }
	    if ((err = read(chfd, buf, c.count())) > -1) {
	      if ((uint32)err != c.count()) {
		warn << "compose_file: error: " << err << " != " << c.count() << "\n";
		return;
	      } 
	      if (compare_sha1_hash(buf, c.count(), 
				    res->resok->fprints[i].hash)) {
		warn << "compose_file: sha1 hash mismatch\n";
		//warn << buf << "\n";
		found = false;
	      }
	    } else {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;	    	    
	    }
	    close(chfd);
	  }
  
	  if (found) {
	    warn << "FOUND!! compose_file: fp = " << res->resok->fprints[i].fingerprint << " in client DB\n";
	    out_fd = open(ga->out_fname, O_CREAT | O_WRONLY, 0666);
	    if (out_fd < 0) {
	      warn << "compose_file: " << strerror(errno) << "\n";
	      return;
	    }

	    //write that chunk to the file
	    if (lseek(out_fd, offset, SEEK_SET) < 0) {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;	    
	    } 
	    if ((err = write(out_fd, buf, c.count())) > -1) {
	      if ((uint32)err != c.count()) {
		warn << "compose_file: error: " << err << " != " << c.count() << "\n";
		return;
	      }
	    } else {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;	    	     
	    }
	    close(out_fd);
	    ga->blocks_written++;
	  }
	} while (!found && !(ci->next(&c)));
      }
      delete ci;
    }
    if (!found) {
      warn << "compose_file: fp = " << res->resok->fprints[i].fingerprint << " not in DB\n";
      normal_read(ga, offset, res->resok->fprints[i].count);
    }
    offset += res->resok->fprints[i].count;
  }
  ga->offset = offset; //offset is 'the' current position in the file
  if (ga->blocks_written == ga->total_blocks && ga->eof) { 
    fht.setopened(true);
    
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    
    ga->msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&(ga->msg);
    h0_len = sizeof(ga->msg);
    
    xfs_send_message_wakeup_multiple (ga->fd, ga->h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  }
}

void lbfs_getfp(ref<getfp_args> ga, ref<lbfs_getfp3res> res, time_t rqtime, 
		clnt_stat err) {

  if (!err && res->status == NFS3_OK) {
    if (fht.setcur(ga->h->handle)) {
      warn << "lbfs_getfp: Can't find node handle\n";
      return;
    }

    ex_fattr3 attr = *(res->resok->file_attributes.attributes);
    attr.expire += rqtime;
    fht.set_nfsattr(attr);
    fht.set_ltime(attr.mtime, attr.ctime);

    ga->total_blocks += res->resok->fprints.size();
    ga->eof = res->resok->eof;
    compose_file(ga, res); 

    if (!res->resok->eof) {
      //ga->offset += gfp->count; //ga->res->resok->count;
      lbfs_getfp3args gfp;
      gfp.file = fht.getnh(fht.getcur());
      gfp.offset = ga->offset;
      gfp.count = LBFS_MAXDATA;

      ref<lbfs_getfp3res> fpres = New refcounted<lbfs_getfp3res>;
      //ga->res = fpres;
      nfsc->call(lbfs_GETFP, &gfp, fpres,
		 wrap (&lbfs_getfp, ga, fpres, timenow));
    }
  } else {
    warn << "lbfs_getfp: " << strerror(res->status) << "\n";
    reply_err(ga->fd, ga->h->header.sequence_num, res->status);
  }
}

void nfs3_read_exist(int fd, struct xfs_message_getdata *h) {

  struct xfs_message_installdata msg; 
  
  nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		 fht.get_nfsattr(), 0, &msg.node);

  msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R 
                  | XFS_OPEN_NW | XFS_DATA_W; //This line is a hack...need to get read access 

  strcpy(msg.cache_name, fht.getcache_name());
  fhandle_t cfh;
  if (getfh(msg.cache_name, &cfh)) {
    warn << "getfh failed\n";
    return;
  }
  memmove(&msg.cache_handle, &cfh, sizeof(cfh));

  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  
  msg.header.opcode = XFS_MSG_INSTALLDATA;
  h0 = (struct xfs_message_header *)&msg;
  h0_len = sizeof(msg);
  
  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);
 
}

void getfp(int fd, struct xfs_message_getdata *h) {

  if (fht.setcur(h->handle)) {
    warn << "getfp: Can't find node handle\n";
    return;
  }

  struct xfs_message_installdata msg;
  
  //this is to fill in msg.node only
  nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		 fht.get_nfsattr(), 0, &msg.node);
  
  msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R 
    | XFS_OPEN_NW | XFS_DATA_W; //This line is a hack...need to get read access 

  strcpy(msg.cache_name, fht.getcache_name());
  int cfd = open(msg.cache_name, O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (cfd < 0) {
    warn << "xfs_message_getdata: " << strerror(errno) << "\n";
    return;
  }
  close(cfd);
  
  fhandle_t cfh;
  if (getfh(msg.cache_name, &cfh)) {
    warn << "getfh failed\n";
    return;
  }
  memmove(&msg.cache_handle, &cfh, sizeof(cfh));
  
  ref<getfp_args> ga = New refcounted<getfp_args> (fd, h);
  ga->msg = msg;
  strcpy(ga->out_fname, fht.getcache_name());
  
  lbfs_getfp3args gfp;
  gfp.file = fht.getnh(fht.getcur());
  gfp.offset = 0;
  gfp.count = LBFS_MAXDATA;
  
  ref<lbfs_getfp3res> fpres = New refcounted<lbfs_getfp3res>;
  
  nfsc->call(lbfs_GETFP, &gfp, fpres,
	     wrap (&lbfs_getfp, ga, fpres, timenow));
}

bool greater(nfstime3 a, nfstime3 b) {
  if (a.seconds > b.seconds)
    return true;
  else 
    if (a.seconds == b.seconds &&
	a.nseconds > b.nseconds)
      return true;
    else return false;
}

void comp_time(int fd, struct xfs_message_getdata *h, bool dirfile, 
	       ptr<ex_getattr3res> res, time_t rqtime, clnt_stat err) {

  if (fht.setcur(h->handle)) {
    warn << "comp_time: Can't find node handle\n";
    return;
  }

  if (res != NULL) {
    ex_fattr3 attr = *(res->attributes);
    attr.expire += rqtime;
    fht.set_nfsattr(attr);
  }
  
  nfstime3 maxtime = fht.max(fht.get_nfsattr().mtime, fht.get_nfsattr().ctime);
  if (greater(maxtime, fht.get_ltime())) {
    if (dirfile) {
      if (fht.setcur(h->handle)) {
	warn << "comp_time: Can't find node handle\n";
        delete res;
	return;
      }
      readdir3args rda;
      rda.dir = fht.getnh(fht.getcur());
      rda.cookie = 0;
      rda.cookieverf = cookieverf3();
      rda.count = fsinfo.dtpref;

      ref<ex_readdir3res> rdres = New refcounted<ex_readdir3res>;
      nfsc->call(lbfs_NFSPROC3_READDIR, &rda, rdres,
		 wrap (&nfs3_readdir, fd, h, rdres, timenow));
    } else getfp(fd, h);
  } else nfs3_read_exist(fd, h);
}

void nfs3_readlink(int fd, struct xfs_message_getdata *h, ref<ex_readlink3res> res,
		   time_t rqtime, clnt_stat err) {

  if (!err && res->status == NFS3_OK) {
    
    struct xfs_message_installdata msg; 
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    if (fht.setcur(h->handle)) {
      warn << "nfs3_readlink: Can't find node handle\n";
      return;
    }

    ex_fattr3 attr = *res->resok->symlink_attributes.attributes;    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   attr, rqtime, &msg.node);
    fht.set_ltime(attr.mtime, attr.ctime);
    
    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R 
      | XFS_OPEN_NW | XFS_DATA_W; //This line is a hack...need to get read access 
    strcpy(msg.cache_name, fht.getcache_name());
     
    int lfd = open(msg.cache_name, O_CREAT | O_WRONLY | O_TRUNC, 0666); 
    if (lfd < 0) 
      return;
 
    fhandle_t cfh;
    if (getfh(msg.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }

    memmove(&msg.cache_handle, &cfh, sizeof(cfh));
    write(lfd, res->resok->data.cstr(), res->resok->data.len());
    close(lfd);
    fht.setopened(true);

    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg;
    h0_len = sizeof(msg);
    
    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);    
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
    warn << "xfs_message_getdata: Can't find node handle\n";
    return -1;
  }
  
  if (fht.get_nfsattr().type == NF3LNK) {
    warn << "reading a symlink!!\n";
    nfs_fh3 fh = fht.getnh(fht.getcur());
    ref<ex_readlink3res> rlres = New refcounted<ex_readlink3res>;
    nfsc->call(lbfs_NFSPROC3_READLINK, &fh, rlres,
	       wrap(&nfs3_readlink, fd, h, rlres, timenow));
    return 0;
  }

  if (!fht.opened()) { 
    if (fht.get_nfsattr().type == NF3DIR) {
      readdir3args rda;
      rda.dir = fht.getnh(fht.getcur());
      rda.cookie = 0;
      rda.cookieverf = cookieverf3();
      rda.count = fsinfo.dtpref;

      ref<ex_readdir3res> rdres = New refcounted<ex_readdir3res>;
      nfsc->call(lbfs_NFSPROC3_READDIR, &rda, rdres,
		 wrap (&nfs3_readdir, fd, h, rdres, timenow));
    } else 
      if (fht.get_nfsattr().type == NF3REG) 
	getfp(fd, h);
  } else {
    if (fht.get_nfsattr().expire < (uint32)timenow) {
      nfs_fh3 fh = fht.getnh(fht.getcur());
      ptr<ex_getattr3res> res = New refcounted<ex_getattr3res>;
      nfsc->call(lbfs_NFSPROC3_GETATTR, &fh, res, 
		 wrap(&comp_time, fd, h, 
		      fht.get_nfsattr().type == NF3DIR, res, timenow));
    } else comp_time(fd, h, fht.get_nfsattr().type == NF3DIR, NULL, 0, clnt_stat(0));
  }    
  return 0;
}

void nfs3_getattr(int fd, struct xfs_message_getattr *h,
		  ref<ex_getattr3res> res, time_t rqtime, clnt_stat err) {

  assert(!err && res->status == NFS3_OK);

  struct xfs_message_installattr msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), *res->attributes, rqtime, &msg.node);

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

  nfs_fh3 fh = fht.getnh(fht.getcur());
  ref<ex_getattr3res> res = New refcounted<ex_getattr3res>;
  nfsc->call(lbfs_NFSPROC3_GETATTR, &fh, res, 
	     wrap(&nfs3_getattr, fd, h, res, timenow));
  
  return 0;
}

void committmp(ref<condwrite3args> cwa, ref<ex_commit3res> res, time_t rqtime, 
	       clnt_stat err) {
  if (!err && res->status == NFS3_OK) {

    if (fht.setcur(cwa->h->handle)) {
      warn << "committmp: Can't find node handle\n";
      return;
    } 
    ex_fattr3 attr = *(res->resok->file_wcc.after.attributes);
    attr.expire += rqtime;
    fht.set_nfsattr(attr);
    fht.set_ltime(attr.mtime, attr.ctime);
    xfs_send_message_wakeup (cwa->fd, cwa->h->header.sequence_num, 0);
  } else {
    warn << "nfs3_committmp: " << strerror(res->status) << "\n";
    reply_err(cwa->fd, cwa->h->header.sequence_num, res->status);
  }
}

void sendcommittmp(ref<condwrite3args> cwa) {

  //signal the server to commit the tmp file
  lbfs_committmp3args ct;
  ct.commit_from = cwa->tmpfh;

  if (fht.setcur(cwa->h->handle)) {
    warn << "xfs_getattr: Can't find node handle\n";
    return;
  }     
  ct.commit_to = fht.getnh(fht.getcur());
    
  ref<ex_commit3res> cres = New refcounted<ex_commit3res>;
  nfsc->call(lbfs_COMMITTMP, &ct, cres,
	     wrap(&committmp, cwa, cres, timenow));
}

void nfs3_write (ref<condwrite3args> cwa, ref<ex_write3res> res, clnt_stat err) {
  
  if (!err && res->status == NFS3_OK) {
    cwa->blocks_written++;
    if (/*cwa->done && */cwa->blocks_written == cwa->total_blocks)
      sendcommittmp(cwa);
  } else {
    warn << "nfs3_write: error: " << strerror(res->status) << "\n";
    reply_err(cwa->fd, cwa->h->header.sequence_num, res->status);
  }
}

void sendwrite(ref<condwrite3args> cwa, lbfs_chunk *chunk) {
  int err, ost;
  char iobuf[NFS_MAXDATA];
  uint64 offst = chunk->loc.pos();
  uint32 count = chunk->loc.count();

  int rfd = open(cwa->fname, O_RDONLY, 0666);
  if (rfd < 0) {
    warn << "sendwrite: " << strerror(errno) << "\n";
    return;
  }

  while (count > 0) {
    ost = lseek(rfd, offst, SEEK_SET);
    if (count < NFS_MAXDATA)
      err = read(rfd, iobuf, count);
    else err = read(rfd, iobuf, NFS_MAXDATA);
    count -= err;
    offst += err;
    if (err < 0) {
      warn << "lbfs_condwrite: error: " << strerror(errno) << "(" << errno << ")\n";
      return;
    }
    write3args wa;
    wa.file = cwa->tmpfh;
    wa.offset = ost;
    wa.stable = UNSTABLE;
    wa.count = err;
    wa.data.setsize(err);
    memcpy (wa.data.base(), iobuf, err);	
    
    ref<ex_write3res> res = New refcounted<ex_write3res>;
    nfsc->call(lbfs_NFSPROC3_WRITE, &wa, res, 
	       wrap(&nfs3_write, cwa, res));
  }
  close(rfd);
}

void lbfs_sendcondwrite(ref<condwrite3args> cwa, lbfs_chunk *chunk, 
			ref<ex_write3res> res, clnt_stat err) {
  if (!err && res->status == NFS3_OK) {
    if (res->resok->count != chunk->loc.count()) {
      warn << "lbfs_sendcondwrite: did not write the whole chunk...\n";
      return;
    }
    cwa->blocks_written++;
    if (/*cwa->done && */cwa->blocks_written == cwa->total_blocks)
      sendcommittmp(cwa);
  } else {
    if (res->status == NFS3ERR_FPRINTNOTFOUND) 
      sendwrite(cwa, chunk);
    else {
      warn << "lbfs_sendcondwrite: " << strerror(res->status) << "\n";
      reply_err(cwa->fd, cwa->h->header.sequence_num, res->status);
    }
  }
}

void sendcondwrite(ref<condwrite3args> cwa, lbfs_chunk *chunk) {

  lbfs_condwrite3args cw;
  cw.file = cwa->tmpfh;
  cw.offset = chunk->loc.pos();
  cw.count = chunk->loc.count();
  cw.fingerprint = chunk->fingerprint;

  int rfd = open(cwa->fname, O_RDONLY, 0666);
  if (rfd < 0) {
    warn << "sendcondwrite: " << cwa->fname << ".." << strerror(errno) << "\n";
    return;
  }

  lseek(rfd, chunk->loc.pos(), SEEK_SET);
  char buf[cw.count];
  int err = read(rfd, buf, cw.count);
  if (err < 0) {
    warn << "lbfs_condwrite: error: " << strerror(errno) << "(" << errno << ")\n";
    return;
  } else if (uint(err) != cw.count) {
    warn << "reading: chunk size " << cw.count << " got " << err << "\n";
    return;
  }
  sha1_hash(&cw.hash, buf, err);
  close(rfd);
  
  ref<ex_write3res> res = New refcounted<ex_write3res>;

  nfsc->call(lbfs_CONDWRITE, &cw, res,
	     wrap(&lbfs_sendcondwrite, cwa, chunk, res));
}

void lbfs_mktmpfile(int fd, struct xfs_message_putdata* h, 
		    ref<ex_diropres3> res, clnt_stat err) {

  if (res->status != NFS3_OK) {
    warn << "lbfs_mktmpfile: error: " << strerror(res->status) << "(" << res->status << ")\n";
    reply_err(fd, h->header.sequence_num, res->status);
    return;
  } else if (!res->resok->obj.present) {
    warn << "tmpfile handle not present\n";
    return;
  }
  //send data to server
  if (fht.setcur(h->handle)) {
    warn << "xfs_getattr: Can't find node handle\n";
    return;
  } 
  
  ref<condwrite3args> cwa = New refcounted<condwrite3args> (fd, h, 
							    *res->resok->obj.handle);
  strcpy(cwa->fname, fht.getcache_name());

  int data_fd = open(cwa->fname, O_RDONLY, 0666);
  if (data_fd < 0) {
    warn << "lbfs_mktmpfile: " << strerror(errno) << "\n";
    return;
  }
  Chunker *chunker = New Chunker(CHUNK_SIZES(0));
  uint count, index = 0, v_size = 0;
  unsigned char buf[4096];
  while ((count = read(data_fd, buf, 4096)) > 0) {
    chunker->chunk(buf, count);
    if (chunker->chunk_vector().size() > v_size) {
      //send condwrite request on last_index..size()
      v_size = chunker->chunk_vector().size();
      cwa->total_blocks = v_size;
      warn << "chindex = " << index << " size = " << v_size << "\n";
      sendcondwrite(cwa, chunker->chunk_vector()[index++]);
    }
  }
  chunker->stop();
  close(data_fd);
  cwa->done = true;
  cwa->total_blocks = chunker->chunk_vector().size();
  warn << "blocks written = " << cwa->blocks_written
       << " total_blocks = " << cwa->total_blocks << "\n";
  if (index+1 != cwa->total_blocks) {
    warn << "lbfs_mktmpfile: index = " << index << " total = " << cwa->total_blocks
	 << "still more chunks!!!\n";
  }
  for (uint i=index; i<cwa->total_blocks; i++) {
    warn << "chindex = " << i << " size = " <<  cwa->total_blocks<< "\n";
    sendcondwrite(cwa, chunker->chunk_vector()[i]);    
  }
  delete chunker;
  delete v;
}

int xfs_message_putdata (int fd, struct xfs_message_putdata *h, u_int size) {
  
  warn << "putdata !!\n";

  if (fht.setcur(h->handle)) {
    warn << "xfs_putdata: Can't find node handle\n";
    return -1;
  } 

  //get temp file handle so the update will be atomic
  lbfs_mktmpfile3args mt;
  mt.commit_to = fht.getnh(fht.getcur());
  xfsattr2nfsattr(h->attr, &mt.obj_attributes);

  ref<ex_diropres3> res = New refcounted<ex_diropres3>;

  nfsc->call(lbfs_MKTMPFILE, &mt, res,
	     wrap(&lbfs_mktmpfile, fd, h, res));
 
  return 0;
}

int xfs_message_inactivenode (int fd, struct xfs_message_inactivenode* h, u_int size) {

  warn << "inactivenode !!\n";

  //remove node from cache
  if (h->flag == XFS_DELETE || //Node is no longer in kernel cache. Delete immediately!!
      h->flag == XFS_NOREFS) { //Node is still in kernel cache. Delete when convenient.
    fht.remove(h->handle);
  }

  return 0;

}

void nfs3_setattr(int fd, struct xfs_message_putattr *h, ref<ex_wccstat3> res, 
		  time_t rqtime, clnt_stat err) {
  
  if (!err && res->status == NFS3_OK) {
 
   assert(res->wcc->after.present);

   struct xfs_message_installattr msg;
   struct xfs_message_header *h0 = NULL;
   size_t h0_len = 0;

   if (fht.setcur(h->handle)) {
     warn << "nfs3_read: Can't find node handle\n";
     return;
   }
   nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		  *res->wcc->after.attributes, rqtime, &msg.node);

   msg.header.opcode = XFS_MSG_INSTALLATTR;
   h0 = (struct xfs_message_header *)&msg;
   h0_len = sizeof(msg);

   xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				     h0, h0_len, NULL, 0);

  } else {
    warn << strerror(res->status) << ": nfs3_setattr\n";
    reply_err(fd, h->header.sequence_num, res->status);
  }
}

int xfs_message_putattr (int fd, struct xfs_message_putattr *h, u_int size) {
  
  warn << "putattr !!\n";

  if (fht.setcur(h->handle)) {
    warn << "nfs3_read: Can't find node handle\n";
    return -1;
  }

  setattr3args sa;
  sa.object = fht.getnh(fht.getcur());
  xfsattr2nfsattr(h->attr, &sa.new_attributes);
  sa.guard.set_check(false);
#if 0
  if (sa->guard.check)
    sa.guard.ctime->seconds = h->attr.xa_ctime;
#endif
  ref<ex_wccstat3> res = New refcounted<ex_wccstat3>;
  nfsc->call(lbfs_NFSPROC3_SETATTR, &sa, res, 
	     wrap(&nfs3_setattr, fd, h, res, timenow));

  return 0;
}

void nfs3_create(int fd, struct xfs_message_create *h, ref<ex_diropres3> res, 
		 time_t rqtime, clnt_stat err) {

  if (!err && res->status == NFS3_OK) {

    struct xfs_message_installdata msg1; //change content of parent dir
    struct xfs_message_installnode msg2; //New file node
    struct xfs_message_installdata msg3; //New file content (null)
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    struct xfs_message_header *h2 = NULL;
    size_t h2_len = 0;

    assert(res->resok->obj.present && res->resok->obj_attributes.present);
    //create new file
    nfsobj2xfsnode(h->cred, *(res->resok->obj.handle), 
		   *(res->resok->obj_attributes.attributes), rqtime, &msg2.node);
    //int new_fd = assign_file(msg3.cache_name, fht.getcur());

    if (fht.setcur(*(res->resok->obj.handle))) {
      warn << "nfs3_create: Can't find node handle\n";
      return;
    }

    strcpy(msg3.cache_name, fht.getcache_name());
    int new_fd = open(msg3.cache_name, O_CREAT | O_RDWR | O_TRUNC, 0666); 

    if (new_fd < 0) {
      warn << "nfs3_create: " << strerror(errno) << "\n";
      return;
    }
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

    strcpy(msg1.cache_name, fht.getcache_name());
#if 0
    int dir_fd = open(msg1.cache_name, O_CREAT | O_RDWR | O_APPEND, 0666);
    if (nfsdirent2xfsfile(dir_fd, h->name, (*res->resok->obj_attributes.attributes).fileid) < 0)
      return;
    close(dir_fd);
#endif
    //msg1.node.tokens = same as parent dir's

    assert(res->resok->dir_wcc.after.present);
    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(res->resok->dir_wcc.after.attributes), rqtime, &msg1.node);

    msg1.flag = 0;  
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg1;
    h0_len = sizeof(msg1);

    msg2.node.tokens = XFS_ATTR_R 
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
  } else {
    warn << strerror(res->status) << ": nfs3_create\n";
    reply_err(fd, h->header.sequence_num, res->status);
  }
}

int xfs_message_create (int fd, struct xfs_message_create *h, u_int size) {
  
  warn << "create !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_create: Can't find parent_handle\n";
    return -1;
  }

  create3args ca;
  ca.where.dir = fht.getnh(fht.getcur());
  ca.where.name = h->name;
  ca.how.set_mode(GUARDED);
  if (ca.how.mode == UNCHECKED || ca.how.mode == GUARDED)
    xfsattr2nfsattr(h->attr, &(*ca.how.obj_attributes));
  else warn << "xfs_message_create: create mode not UNCHECKED or GUARDED\n";

  ref<ex_diropres3> res = New refcounted<ex_diropres3>;
  nfsc->call(lbfs_NFSPROC3_CREATE, &ca, res, 
	     wrap(&nfs3_create, fd, h, res, timenow));

  return 0;
}

void nfs3_mkdir(int fd, struct xfs_message_mkdir *h, ref<ex_diropres3> res, 
		time_t rqtime, clnt_stat err) {

  if (!err && res->status == NFS3_OK) {

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
		   *(res->resok->obj_attributes.attributes), rqtime, &msg2.node);

    if (fht.setcur(*(res->resok->obj.handle))) {
      warn << "nfs3_read: Can't find node handle\n";
      return;
    }

    strcpy(msg3.cache_name, fht.getcache_name());
    int new_fd = open(msg3.cache_name, O_CREAT, 0666); 

    if (new_fd < 0) {
      warn << "nfs3_mkdir: " << errno << " " << strerror(errno) << "\n";
      return;
    }
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

    strcpy(msg1.cache_name, fht.getcache_name());
#if 0    
    int dir_fd = open(msg1.cache_name, O_WRONLY | O_APPEND, 0666);
    if (nfsdirent2xfsfile(dir_fd, h->name, (*res->resok->obj_attributes.attributes).fileid) < 0) 
      return;
    close(dir_fd);
#endif
    fhandle_t parent_fh;
    if (getfh(msg1.cache_name, &parent_fh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg1.cache_handle, &parent_fh, sizeof(parent_fh)); 

    //msg1.node.tokens = same as parent dir's

    assert(res->resok->dir_wcc.after.present);
    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(res->resok->dir_wcc.after.attributes), rqtime, &msg1.node);

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

    msg3.node = msg2.node;
    msg3.flag = 0;
    msg3.header.opcode = XFS_MSG_INSTALLDATA;

    h2 = (struct xfs_message_header *)&msg3;
    h2_len = sizeof(msg3);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, h2, h2_len,
				      NULL, 0);
  } else {
    warn << strerror(res->status) << ": nfs3_mkdir\n";
    reply_err(fd, h->header.sequence_num, res->status);
  }
}

int xfs_message_mkdir (int fd, struct xfs_message_mkdir *h, u_int size) {
  
  warn << "mkdir !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
    return -1;
  }
  
  mkdir3args ma;
  ma.where.dir = fht.getnh(fht.getcur());
  ma.where.name = h->name;
  xfsattr2nfsattr(h->attr, &ma.attributes);

  ref<ex_diropres3> res = New refcounted<ex_diropres3>;
  nfsc->call(lbfs_NFSPROC3_MKDIR, &ma, res, 
	     wrap(&nfs3_mkdir, fd, h, res, timenow));

  return 0;
}

void nfs3_link(int fd, struct xfs_message_link *h, ref<ex_link3res> res, 
	       time_t rqtime, clnt_stat err) {
  
  struct xfs_message_installdata msg1; //update parent dir's data
  struct xfs_message_installnode msg2; //update attr of from_handle
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  struct xfs_message_header *h1 = NULL;
  size_t h1_len = 0;

  //change attributes of parent dir
  //in the future implement local content change too..
  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_link: Can't find parent_handle\n";
    return;
  }
  strcpy(msg1.cache_name, fht.getcache_name());

  fhandle_t parent_fh;
  if (getfh(msg1.cache_name, &parent_fh)) {
    warn << "getfh failed\n";
    return;
  }
  memmove(&msg1.cache_handle, &parent_fh, sizeof(parent_fh)); 

  assert(res->res->linkdir_wcc.after.present);    
  nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		 *(res->res->linkdir_wcc.after.attributes), rqtime, &msg1.node);

  msg1.flag = 0;  
  msg1.header.opcode = XFS_MSG_INSTALLDATA;
  h0 = (struct xfs_message_header *)&msg1;
  h0_len = sizeof(msg1);

  if (fht.setcur(h->from_handle)) {
    warn << "xfs_message_link: Can't find from_handle\n";
    return;
  }

  assert(res->res->file_attributes.present);
  nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		 *(res->res->file_attributes.attributes), rqtime, &msg2.node);

  msg2.node.tokens   = XFS_ATTR_R;
  msg2.parent_handle = h->parent_handle;
  strcpy (msg2.name, h->name);

  msg2.header.opcode = XFS_MSG_INSTALLNODE;
  h1 = (struct xfs_message_header *)&msg2;
  h1_len = sizeof(msg2);

  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, h1, h1_len, NULL, 0);  
}

int xfs_message_link(int fd, struct xfs_message_link *h, u_int size) {

  warn << "(hard) link !!\n";

  if (fht.setcur(h->from_handle)) {
    warn << "xfs_message_link: Can't find from_handle\n";
    return -1;
  }
  link3args la;
  la.file = fht.getnh(fht.getcur());

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_link: Can't find parent_handle\n";
    return -1;
  }
  la.link.dir = fht.getnh(fht.getcur());
  la.link.name = h->name;

  ref<ex_link3res> res = New refcounted<ex_link3res>;
  nfsc->call(lbfs_NFSPROC3_LINK, &la, res, 
	     wrap(&nfs3_link, fd, h, res, timenow));

  return 0;
}

void nfs3_symlink(int fd, struct xfs_message_symlink *h, ref<ex_diropres3> res,
		  time_t rqtime, clnt_stat err) {

  if (!err && res->status == NFS3_OK) {
    struct xfs_message_installdata msg1; //install change in parent dir
    struct xfs_message_installnode msg2; //install symlink node
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    
    assert(res->resok->obj.present && res->resok->obj_attributes.present);
    //create symlink
    ex_fattr3 attr = *res->resok->obj_attributes.attributes;
    nfsobj2xfsnode(h->cred, *(res->resok->obj.handle), 
		   attr, rqtime, &msg2.node);
    fht.set_ltime(attr.mtime, attr.ctime);

    //write new direntry to parent dirfile (do a readdir or just append that entry?)
    if (fht.setcur(h->parent_handle)) {
      warn << "nfs3_symlink: Can't find parent handle\n";
      return;
    }
    strcpy(msg1.cache_name, fht.getcache_name());

    //add entry to parent dir (changing the mtime)
    assert(res->resok->dir_wcc.after.present);
    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(res->resok->dir_wcc.after.attributes), rqtime, &msg1.node);

    msg1.flag = 0;  
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg1;
    h0_len = sizeof(msg1);

    msg2.node.tokens = XFS_ATTR_R;
    msg2.parent_handle = h->parent_handle;
    strlcpy (msg2.name, h->name, sizeof(msg2.name));
    
    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *)&msg2;
    h1_len = sizeof(msg2);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, NULL, 0);

  } else {
    warn << "nfs3_symlink: " << strerror(res->status) << "\n";
    reply_err(fd, h->header.sequence_num, res->status);
  }
}

int xfs_message_symlink(int fd, struct xfs_message_symlink *h, u_int size) {

  warn << "symlimk !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_symlink: Can't find parent_handle\n";
    return -1;
  }

  symlink3args sla;
  sla.where.dir = fht.getnh(fht.getcur());
  sla.where.name = h->name;
  xfsattr2nfsattr(h->attr, &(sla.symlink.symlink_attributes));
  sla.symlink.symlink_data.setbuf(h->contents, strlen(h->contents));

  ref<ex_diropres3> res = New refcounted<ex_diropres3>;
  nfsc->call(lbfs_NFSPROC3_SYMLINK, &sla, res,
	     wrap(&nfs3_symlink, fd, h, res, timenow));
  
  return 0;
}

void remove(int fd, struct xfs_message_remove *h, ref<ex_lookup3res> lres,
	    ref<ex_wccstat3> wres, time_t rqtime, clnt_stat err) {

  if (!err && wres->status == NFS3_OK) {

    assert(wres->wcc->after.present);

    struct xfs_message_installdata msg1;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installattr msg2;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;

    //remove entry from parent dir
    if (fht.setcur(h->parent_handle)) {
      warn << "xfs_message_remove: Can't find parent_handle\n";
      return;
    }
    
    strcpy(msg1.cache_name, fht.getcache_name());
    fhandle_t cfh;
    if (getfh(msg1.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg1.cache_handle, &cfh, sizeof(cfh));
    //remove entry from local dir after..
#if 0
    int dir_fd = open(msg1.cache_name, O_RDWR | O_SHLOCK, 0666);
    if (xfsfile_rm_dirent(dir_fd, h->name) < 0)
      return;
    close(dir_fd);
#endif
    ex_fattr3 attr = *(wres->wcc->after.attributes);
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   attr, rqtime, &msg1.node);
    
    msg1.flag = XFS_ID_INVALID_DNLC;
    msg1.node.tokens |=  XFS_DATA_R;
    
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *)&msg1;
    h0_len = sizeof(msg1);
 
    //if the entry being removed from parent is still being referenced (nlink > 1)
    //update its attr, otherwise, evict from cache
    
    ex_post_op_attr a;
    if (lres->resok->obj_attributes.present) 
      a = lres->resok->obj_attributes;
    else if (lres->resok->dir_attributes.present)
      a = lres->resok->dir_attributes;
    else { 
      warn << "lookup in remove: error no attr present\n";
      return;
    }

    if ((a.attributes->type == NF3DIR && a.attributes->nlink > 2) || 
	(a.attributes->type == NF3REG && a.attributes->nlink > 1)) {

      if (fht.setcur(lres->resok->object)) {
	warn << "xfs_message_remove: Can't find handle\n";
	return;
      }

      msg2.header.opcode = XFS_MSG_INSTALLATTR;
      --(a.attributes->nlink);
      nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		     *(a.attributes), rqtime, &msg1.node);

      //msg2.node.tokens   = limbo_entry->tokens;
    
      //  if data is not being used in the kernel...
      //  msg2.node.tokens &= ~XFS_DATA_MASK;
    
      h1 = (struct xfs_message_header *)&msg2;
      h1_len = sizeof(msg2);
    
      
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					0, h0, h0_len, h1, h1_len,
					NULL, 0);

    } else {
      //TODO: evict from cache..
      //fht.remove(...);
      xfs_send_message_wakeup_multiple (fd,h->header.sequence_num,
					0, h0, h0_len, NULL, 0);
    }
      
  } else {
    warn << strerror(wres->status) << ": nfs3_lookup in remove\n";
    reply_err(fd, h->header.sequence_num, wres->status);
  }
}

void nfs3_remove(int fd, struct xfs_message_remove *h, ref<ex_lookup3res> lres,
		 clnt_stat err) {

  if (!err && lres->status == NFS3_OK) {

    //lookup entry's filehandle and attr
    //if the entry being removed from parent is still being referenced (nlink > 1)
    //update its attr
    
    if (fht.setcur(h->parent_handle)) {
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
      return;
    }

    diropargs3 doa;
    doa.dir = fht.getnh(fht.getcur());
    doa.name = h->name;

    ref<ex_wccstat3> wres = New refcounted<ex_wccstat3>;
    nfsc->call(lbfs_NFSPROC3_REMOVE, &doa, wres,
	       wrap(&remove, fd, h, lres, wres, timenow));

  } else {
      warn << "nfs3_remove: lres->status = " << strerror(lres->status) << "\n";
      reply_err(fd, h->header.sequence_num, lres->status);
  }
}

int xfs_message_remove(int fd, struct xfs_message_remove *h, u_int size) {

  warn << "remove !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
    return -1;
  }

  diropargs3 doa;
  doa.dir = fht.getnh(fht.getcur());
  doa.name = h->name;
  warn << "requesting file name " << doa.name.cstr() << "\n";
  ref<ex_lookup3res> res = New refcounted<ex_lookup3res>;
    
  nfsc->call(lbfs_NFSPROC3_LOOKUP, &doa, res, 
	     wrap (&nfs3_remove, fd, h, res));

  return 0;
}

void nfs3_rmdir(int fd, struct xfs_message_rmdir *h, ref<ex_lookup3res> lres,
		clnt_stat err) {
  
  if (!err && lres->status == NFS3_OK) {
    
    //lookup entry's filehandle and attr
    //if the entry being removed from parent is still being referenced (nlink > 1)
    //update its attr
    
    if (fht.setcur(h->parent_handle)) {
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
      return;
    }
    
    diropargs3 doa;
    doa.dir = fht.getnh(fht.getcur());
    doa.name = h->name;

    ref<ex_wccstat3> wres = New refcounted<ex_wccstat3>;
    nfsc->call(lbfs_NFSPROC3_RMDIR, &doa, wres,
	       wrap(&remove, fd, (struct xfs_message_remove *)h, lres, wres, timenow));

  } else {
    warn << "nfs3_rmdir: lres->status = " << lres->status << "\n";
    reply_err(fd, h->header.sequence_num, lres->status);
  }
}

int xfs_message_rmdir(int fd, struct xfs_message_rmdir *h, u_int size) {
  
  warn << "rmdir !!\n";
  
  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
    return -1;
  }
  
  diropargs3 doa;
  doa.dir = fht.getnh(fht.getcur());
  doa.name = h->name;
  warn << "requesting file name " << doa.name.cstr() << "\n";
  ref<ex_lookup3res> res = New refcounted<ex_lookup3res>;
    
  nfsc->call(lbfs_NFSPROC3_LOOKUP, &doa, res, 
	     wrap(&nfs3_rmdir, fd, h, res));
  
  return 0;
}

void update_attr(ex_fattr3 attr1, ex_fattr3 attr2, time_t rqtime1, time_t rqtime2) {

  nfstime3 cache_time = fht.get_ltime();
  if (greater(attr1.mtime, cache_time) || greater(attr1.ctime, cache_time)) {
    attr1.expire += rqtime1;
    fht.set_nfsattr(attr1);
  } else 
    if (greater(attr2.mtime, attr1.mtime)) {
      attr2.expire += rqtime2;
      fht.set_nfsattr(attr2);
    } else {
      fht.set_ltime(attr2.mtime, attr2.ctime);
      attr2.expire += rqtime2;
      fht.set_nfsattr(attr2);
    }
}

void nfs3_rename_getattr(ref<rename_args> rena, time_t rqtime2, clnt_stat err) {

  
  if (rena->gares->status != NFS3_OK) {
    warn << "nfs3_rename_getattr: gares->status = " << strerror(rena->gares->status) << "\n";
    reply_err(rena->fd, rena->h->header.sequence_num, rena->gares->status);
    return;
  }

  struct xfs_message_installnode msg1; //update attr of file renamed 
  struct xfs_message_installdata msg2; //new parent dir content
  struct xfs_message_installdata msg3; //old parent dir content
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  struct xfs_message_header *h1 = NULL;
  size_t h1_len = 0;
  struct xfs_message_header *h2 = NULL;
  size_t h2_len = 0;
  nfs_fh3 file = rena->lres->resok->object;

  if (fht.setcur(file)) {
    warn << "nfs3_rename_getattr: Can't find file handle\n";
    return;    
  }

  if (rena->lres->resok->obj_attributes.present) 
    update_attr(*(rena->lres->resok->obj_attributes.attributes), 
		*(rena->gares->attributes), rena->rqtime1, rqtime2);
  else 
    if (rena->lres->resok->dir_attributes.present) 
      update_attr(*(rena->lres->resok->dir_attributes.attributes), 
		  *(rena->gares->attributes), rena->rqtime1, rqtime2);

  nfsobj2xfsnode(rena->h->cred, file, fht.get_nfsattr(), 0, &msg1.node);
  
  msg1.parent_handle = rena->h->new_parent_handle;
  strlcpy (msg1.name, rena->h->new_name, sizeof(msg1.name));
  
  msg1.header.opcode = XFS_MSG_INSTALLNODE;
  h0 = (struct xfs_message_header *)&msg1;
  h0_len = sizeof(msg1);

  if (fht.setcur(rena->h->new_parent_handle)) {
    warn << "nfs3_rename_getattr: Can't find file new_parent_handle\n";
    return;    
  }
  strcpy(msg2.cache_name, fht.getcache_name());
  //change content of new parent dir (later)
  fhandle_t parent_fh;
  if (getfh(msg2.cache_name, &parent_fh)) {
    warn << "getfh failed\n";
    return;
  }
  memmove(&msg2.cache_handle, &parent_fh, sizeof(parent_fh)); 
  assert(rena->rnres->res->todir_wcc.after.present);
  nfsobj2xfsnode(rena->h->cred, fht.getnh(fht.getcur()), 
		 *(rena->rnres->res->todir_wcc.after.attributes), 
		 rqtime2, &msg2.node);

  msg2.flag = 0;  
  msg2.header.opcode = XFS_MSG_INSTALLDATA;
  h1 = (struct xfs_message_header *)&msg2;
  h1_len = sizeof(msg2);

  if (!xfs_handle_eq(&rena->h->old_parent_handle, &rena->h->new_parent_handle)) {
    if (fht.setcur(rena->h->old_parent_handle)) {
      warn << "nfs3_rename_getattr: Can't find file old_parent_handle\n";
      return;    
    }
    strcpy(msg3.cache_name, fht.getcache_name());
    //change content of new parent dir (later)
    if (getfh(msg3.cache_name, &parent_fh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg3.cache_handle, &parent_fh, sizeof(parent_fh)); 
    assert(rena->rnres->res->fromdir_wcc.after.present);
    nfsobj2xfsnode(rena->h->cred, fht.getnh(fht.getcur()), 
		   *(rena->rnres->res->fromdir_wcc.after.attributes), 
		   rqtime2, &msg3.node);
    
    msg3.flag = 0;  
    msg3.header.opcode = XFS_MSG_INSTALLDATA;
    h2 = (struct xfs_message_header *)&msg3;
    h2_len = sizeof(msg3);
  }

  xfs_send_message_wakeup_multiple (rena->fd, rena->h->header.sequence_num,
				    0, h0, h0_len, h1, h1_len, h2, h2_len,
				    NULL, 0);
}

void nfs3_rename_rename(ref<rename_args> rena, clnt_stat err) {

  warn << "rename_rename !!\n";
  
  nfs_fh3 fh = rena->lres->resok->object;
  //rena->gares = New refcounted<ex_getattr3res>;

  nfsc->call(lbfs_NFSPROC3_GETATTR, &fh, rena->gares, 
	     wrap (&nfs3_rename_getattr, rena, timenow));
}

void nfs3_rename_lookup(ref<rename_args> rena, time_t rqtime, clnt_stat err) {
  
  warn << "rename_lookup !!\n";
  
  if (rena->lres->status != NFS3_OK) {
    warn << "nfs3_rename_lookup: lres->status = " << strerror(rena->lres->status) << "\n";
    reply_err(rena->fd, rena->h->header.sequence_num, rena->lres->status);
    return;
  }

  if (fht.setcur(rena->h->old_parent_handle)) {
    warn << "xfs_message_rename: Can't find old_parent_handle\n";
    return;
  }

  rename3args rna;
  rna.from.dir = fht.getnh(fht.getcur());
  rna.from.name = rena->h->old_name;

  if (fht.setcur(rena->h->new_parent_handle)) {
    warn << "xfs_message_rename: Can't find new_parent_handle\n";
    return;
  }
  
  rna.to.dir = fht.getnh(fht.getcur());
  rna.to.name = rena->h->new_name;
  
  rena->rqtime1 = rqtime;
  //rena->rnres = New refcounted<ex_rename3res>;

  nfsc->call(lbfs_NFSPROC3_RENAME, &rna, rena->rnres, 
	     wrap (&nfs3_rename_rename, rena));
}

int xfs_message_rename(int fd, struct xfs_message_rename *h, u_int size) {
  
  warn << "rename !!\n";
  
  if (fht.setcur(h->old_parent_handle)) {
    warn << "xfs_message_rename: Can't find old_parent_handle\n";
    return -1;
  }

  diropargs3 doa;
  doa.dir = fht.getnh(fht.getcur());
  doa.name = h->old_name;
  
  ref<rename_args> rena = New refcounted<rename_args> (fd, h);
  //rena->lres = New ex_lookup3res;
  nfsc->call(lbfs_NFSPROC3_LOOKUP, &doa, rena->lres, 
	     wrap (&nfs3_rename_lookup, rena, timenow));
  return 0;
}

void cbdispatch(svccb *sbp)
{
  if (!sbp)
    return;

  switch (sbp->proc ()) {
  case ex_NFSCBPROC3_NULL:
    sbp->reply (NULL);
    break;
  case ex_NFSCBPROC3_INVALIDATE:
    {
      ex_invalidate3args *xa = sbp->template getarg<ex_invalidate3args> ();
      ex_fattr3 *a = NULL;
      if (xa->attributes.present && xa->attributes.attributes->expire) {
	a = xa->attributes.attributes.addr ();
	a->expire += timenow;
	if (fht.setcur(xa->handle)) {
	  warn << "cbdispatch: Can't find handle\n";
	  return;
	}      
	fht.set_nfsattr(*a);
      }
      delete a;
      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
