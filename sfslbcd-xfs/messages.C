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

nfs_fh3 *fh;
access3args *aca;
diropargs3 *doa;
readdir3args *rda;
read3args *ra;
write3args *wa;
create3args *ca;
mkdir3args *ma;
symlink3args *sla;
setattr3args *sa;
lbfs_mktmpfile3args *mt;
lbfs_condwrite3args *cw;
lbfs_committmp3args *ct;
vec<lbfs_chunk *> *cvp; 
lbfs_chunk_loc *chl;
lbfs_getfp3args *gfp;

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

void lbfs_condwrite(ref<condwrite3args> cwa, clnt_stat err);
void normal_read(ref<getfp_args> ga, uint64 offset, uint32 count);
void nfs3_rmdir(int fd, struct xfs_message_rmdir *h, ex_lookup3res *lres,
		clnt_stat err);

void getrootattr(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, ex_getattr3res *res, time_t rqtime, clnt_stat err) {

  struct xfs_message_installroot msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  assert(res->status == NFS3_OK);
  
  warn << "uid = " << getuid() << "\n"; 

  nfsobj2xfsnode(h->cred, fsi->nfs->v3->root, *res->attributes, rqtime, &msg.node);
  
  msg.header.opcode = XFS_MSG_INSTALLROOT;
  h0 = (struct xfs_message_header *)&msg;
  h0_len = sizeof(msg);

  delete fsi;
  xfs_send_message_wakeup_multiple (fd,	h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);

}

void nfs3_fsinfo(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, 
		 ex_fsinfo3res *res, clnt_stat err) {

  assert(res->status == NFS3_OK); 

  fsinfo = *res->resok;

  ex_getattr3res *ares = new ex_getattr3res;
  //AUTH *auth_default = authunix_create_default ();

  nfsc->call(lbfs_NFSPROC3_GETATTR, &fsi->nfs->v3->root, ares, 
	     wrap (&getrootattr, fd, h, fsi, ares, timenow));
  //	     auth_default);  
}

void sfs_getfsinfo(int fd, struct xfs_message_getroot *h, sfs_fsinfo *fsi, clnt_stat err) {

  assert(fsi->prog == ex_NFS_PROGRAM && fsi->nfs->vers == ex_NFS_V3);
  //x->compress ();
  ex_fsinfo3res *res = new ex_fsinfo3res;

  nfsc->call(lbfs_NFSPROC3_FSINFO, &fsi->nfs->v3->root, res,
	     wrap(&nfs3_fsinfo, fd, h, fsi, res));

}

int xfs_message_getroot (int fd, struct xfs_message_getroot *h, u_int size)
{
  warn << "get root!!\n";

  sfs_fsinfo *fsi = new sfs_fsinfo;
  sfsc->call(SFSPROC_GETFSINFO, NULL, fsi,
	  wrap (&sfs_getfsinfo, fd, h, fsi), NULL, NULL);

  return 0;
}

#if 0
void nfs3_access(int fd, xfs_message_function h, ex_access3res *res, clnt_stat err) {
  
  assert(res->status == NFS3_OK);

  if (h->header,opcode == XFS_MSG_GETNODE || h->header.opcode == XFS_MSG_CREATE ||
      h->header.opcode == XFS_MSG_MKDIR   || h->header.opcode == XFS_MSG_REMOVE || 
      h->header.opcode == XFS_MSG_RMDIR) {
    if (fht.setcur(h->parent_handle)) {
      warn << "nfs3_access: Can't find parent_handle\n";
      return -1;
    }
  } else {
    if (fht.setcur(h->handle)) {
      warn << "nfs3_access: Can't find handle\n";
      return -1;
    }
  }

#if 0
  aca = new access3args;
  aca->object = fht.getnh(fht.getcur());
  aca->access = ACCESS3_LOOKUP;
#endif

  switch (h->header) {
  case XFS_MSG_GETNODE:
    if (res->resok->access == ACCESS3_LOOKUP) {
      doa = new diropargs3;
      doa->dir = fht.getnh(fht.getcur());
      doa->name = h->name;
      warn << "requesting file name " << doa->name.cstr() << "\n";
      ex_lookup3res *lres = new ex_lookup3res;
      
      nfsc->call(lbfs_NFSPROC3_LOOKUP, doa, lres, 
		 wrap (&nfs3_lookup, fd, h, lres));
    } else 
      warn << "ACCESS3_LOOKUP permission denied..\n";
    break;
  case XFS_MSG_GETDATA:
    if (res->resok->access == ACCESS3_READ) {
      
    }
  }
}
#endif

void nfs3_lookup(int fd, struct xfs_message_getnode *h, 
	    ex_lookup3res *lres, time_t rqtime, clnt_stat err) {

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
  
  doa = new diropargs3;
  doa->dir = fht.getnh(fht.getcur());
  doa->name = h->name;
  warn << "requesting file name " << doa->name.cstr() << "\n";
  ex_lookup3res *res = new ex_lookup3res;
  
  nfsc->call(lbfs_NFSPROC3_LOOKUP, doa, res, 
	     wrap (&nfs3_lookup, fd, h, res, timenow));

  return 0;
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
      rda->dir = fht.getnh(fht.getcur());
      entry3 *e = res->resok->reply.entries;
      while (e->nextentry != NULL) e = e->nextentry;
      rda->cookie = e->cookie;
      rda->cookieverf = res->resok->cookieverf;
      rda->count = fsinfo.dtpref;

      ex_readdir3res *rdres = new ex_readdir3res;
      nfsc->call(lbfs_NFSPROC3_READDIR, rda, rdres,
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

void nfs3_readdir(int fd, struct xfs_message_getdata *h, ex_readdir3res *res, time_t rqtime,
	     clnt_stat err) {
  
  if (res->status == NFS3_OK) {

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

void write_file(ref<getfp_args> ga, uint64 offset, uint32 count, ex_read3res *res) {
//, clnt_stat cl_err) {

  if (fht.setcur(ga->h->handle)) {
    warn << "nfs3_readdir: Can't find node handle\n";
    return;
  }
  
  int err;
  if ((err = lseek(ga->cfd, offset, SEEK_SET)) < 0) {
    warn << "write_file: " << strerror(errno) << "\n";
    return;
  }
  
  err = write(ga->cfd, res->resok->data.base(), res->resok->data.size());
  if (err != (int)res->resok->data.size()) {
    warn << "write error or short write!!\n";
    return;
  }

  if (res->resok->count < count)
    normal_read(ga, offset+res->resok->count, count-res->resok->count);
  else ga->blocks_written++;

}

void nfs3_read(ref<getfp_args> ga, uint64 offset, uint32 count, ex_read3res *res, 
	       clnt_stat err) {
  
  if (res->status == NFS3_OK && res->resok->file_attributes.present) {

    write_file(ga, offset, count, res); 

    if (ga->blocks_written == ga->res->resok->fprints.size()) {

      //add chunk to the database
      cvp = new vec<lbfs_chunk *>;
      if (chunk_file(CHUNK_SIZES(0), cvp, (char const*)ga->msg.cache_name) < 0) {
	warn << strerror(errno) << "(" << errno << "): nfs3_read(chunkfile)\n";
	return;
      }
      for (uint i=0; i<cvp->size(); i++) {
	warn << "adding fp = " << (*cvp)[i]->fingerprint << " to lbfsdb\n";
	(*cvp)[i]->loc.set_fh(fht.getnh(fht.getcur()));
	lbfsdb.add_chunk((*cvp)[i]->fingerprint, &((*cvp)[i]->loc));
      }

      close(ga->cfd);
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
    warn << "error: " << strerror(errno) << "(" << errno << ")\n";
    if (res->resfail->present) 
      warn << "dir present\n";
    else warn << "dir not present\n";
  }
}

void normal_read(ref<getfp_args> ga, uint64 offset, uint32 count) {

  if (fht.setcur(ga->h->handle)) {
    warn << "normal_read: Can't find node handle\n";
    return;
  }
 
  ra = new read3args;
  ra->file = fht.getnh(fht.getcur());
  ra->offset = offset;
  ra->count = count;
  
  ex_read3res *rres = new ex_read3res;
  nfsc->call(lbfs_NFSPROC3_READ, ra, rres,
	     wrap(&nfs3_read, ga, offset, count, rres));

}

void compose_file(ref<getfp_args> ga) {

  int err, chfd;
  uint64 offset = ga->offset; //chunk position

  lbfs_db::chunk_iterator *ci = NULL;
  bool found = false;
  ga->blocks_written = 0;
  unsigned char *buf;
  nfs_fh3 fh;
  lbfs_chunk_loc c;

  for (uint i=0; i<ga->res->resok->fprints.size(); i++) {
    found = false;
    //find matching fp in the database
    //if found, write that chunk to the file,
    //otherwise, send for a normal read of that chunk
    if (!lbfsdb.get_chunk_iterator(ga->res->resok->fprints[i].fingerprint, &ci)) {
      if (!ci) warn << "ci is NULL\n";
      if (ci && !(ci->get(&c))) {
	do {
	  found = true;
	  c.get_fh(fh);

	  buf = new unsigned char[c.count()];

	  if (c.count() != ga->res->resok->fprints[i].count) {
	    warn << "chunk size != size from server..\n";
	    found = false;
	  }
	  //read chunk c.pos() to c.count() from fh into buf 
	  if (fht.setcur(fh)) {
	    warn << "compose_file: null fh or Can't find node handle\n";
	    return;
	  }
	  
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
				  ga->res->resok->fprints[i].hash)) {
	      warn << "compose_file: sha1 hash mismatch\n";
	      //warn << buf << "\n";
	      found = false;
	    }
	  } else {
	    warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	    return;	    	    
	  }
  
	  if (found) {
	    warn << "FOUND!! compose_file: fp = " << ga->res->resok->fprints[i].fingerprint << " in client DB\n";
	    
	    //write that chunk to the file
	    if (lseek(ga->cfd, offset, SEEK_SET) < 0) {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;	    
	    } 
	    if ((err = write(ga->cfd, buf, c.count())) > -1) {
	      if ((uint32)err != c.count()) {
		warn << "compose_file: error: " << err << " != " << c.count() << "\n";
		return;
	      }
	    } else {
	      warn << "compose_file: error: " << strerror(errno) << "(" << errno << ")\n";
	      return;	    	     
	    }
	    ga->blocks_written++;
	  }
	} while (!found && !(ci->next(&c)));
      }
      delete ci;
    }
    if (!found) {
      warn << "compose_file: fp = " << ga->res->resok->fprints[i].fingerprint << " not in DB\n";
      normal_read(ga, offset, ga->res->resok->fprints[i].count);
    }
    offset += ga->res->resok->fprints[i].count;

    if (ga->blocks_written == ga->res->resok->fprints.size()) {
      close(ga->cfd);
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
}

void lbfs_getfp(ref<getfp_args> ga, time_t rqtime, clnt_stat err) {

  if (ga->res->status == NFS3_OK) {
    if (fht.setcur(ga->h->handle)) {
      warn << "lbfs_getfp: Can't find node handle\n";
      return;
    }

    ex_fattr3 attr = *(ga->res->resok->file_attributes.attributes);
    attr.expire += rqtime;
    fht.set_nfsattr(attr);
    fht.set_ltime(attr.mtime, attr.ctime);

    compose_file(ga); 
    if (!ga->res->resok->eof) {
      ga->offset += gfp->count; //ga->res->resok->count;
      gfp->file = fht.getnh(fht.getcur());
      gfp->offset = ga->offset;
      gfp->count = LBFS_MAXDATA;

      lbfs_getfp3res *fpres = new lbfs_getfp3res;
      ga->res = fpres;
      nfsc->call(lbfs_GETFP, gfp, fpres,
		 wrap (&lbfs_getfp, ga, timenow));
    }
  } else {
    warn << "lbfs_getfp: " << strerror(errno) << "\n";
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
  
  fhandle_t cfh;
  if (getfh(msg.cache_name, &cfh)) {
    warn << "getfh failed\n";
    return;
  }
  memmove(&msg.cache_handle, &cfh, sizeof(cfh));
  
  ref<getfp_args> ga = new refcounted<getfp_args> (fd, h);
  
  gfp = new lbfs_getfp3args;
  gfp->file = fht.getnh(fht.getcur());
  gfp->offset = 0;
  gfp->count = LBFS_MAXDATA;
  
  lbfs_getfp3res *fpres = new lbfs_getfp3res;
  ga->offset = 0;
  ga->res = fpres;
  ga->cfd = cfd;
  ga->msg = msg;
  
  nfsc->call(lbfs_GETFP, gfp, fpres,
	     wrap (&lbfs_getfp, ga, timenow));
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
	       ex_getattr3res *res, time_t rqtime, clnt_stat err) {

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
	return;
      }
      rda = new readdir3args;
      rda->dir = fht.getnh(fht.getcur());
      rda->cookie = 0;
      rda->cookieverf = cookieverf3();
      rda->count = fsinfo.dtpref;

      ex_readdir3res *rdres = new ex_readdir3res;
      nfsc->call(lbfs_NFSPROC3_READDIR, rda, rdres,
		 wrap (&nfs3_readdir, fd, h, rdres, timenow));
    } else getfp(fd, h);
  } else nfs3_read_exist(fd, h);
}

void nfs3_readlink(int fd, struct xfs_message_getdata *h, ex_readlink3res *res,
		   time_t rqtime, clnt_stat err) {

  if (res->status == NFS3_OK) {
    
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
     
    int lfd = open(msg.cache_name, O_CREAT | O_RDWR | O_TRUNC, 0666); 
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
    fh = new nfs_fh3;
    *fh = fht.getnh(fht.getcur());
    ex_readlink3res *rlres = new ex_readlink3res;
    nfsc->call(lbfs_NFSPROC3_READLINK, fh, rlres,
	       wrap(&nfs3_readlink, fd, h, rlres, timenow));
    return -1;
  }

  if (!fht.opened()) { 
    if (fht.get_nfsattr().type == NF3DIR) {
      rda = new readdir3args;
      rda->dir = fht.getnh(fht.getcur());
      rda->cookie = 0;
      rda->cookieverf = cookieverf3();
      rda->count = fsinfo.dtpref;

      ex_readdir3res *rdres = new ex_readdir3res;
      nfsc->call(lbfs_NFSPROC3_READDIR, rda, rdres,
		 wrap (&nfs3_readdir, fd, h, rdres, timenow));
    } else 
      if (fht.get_nfsattr().type == NF3REG) 
	getfp(fd, h);
#if 0
      else 
	if (fht.get_nfsattr().type == NF3LNK) {
	  warn << "reading a symlink!!\n";
	  fh = new nfs_fh3;
	  *fh = fht.getnh(fht.getcur());
	  ex_readlink3res *rlres = new ex_readlink3res;
	  nfsc->call(lbfs_NFSPROC3_READLINK, fh, rlres,
		     wrap(&nfs3_readlink, fd, h, rlres, timenow));
	}
#endif
  } else {
    if (fht.get_nfsattr().expire < (uint32)timenow) {
      fh = new nfs_fh3; 
      *fh = fht.getnh(fht.getcur());
      ex_getattr3res *res = new ex_getattr3res;
      nfsc->call(lbfs_NFSPROC3_GETATTR, fh, res, 
		 wrap(&comp_time, fd, h, 
		      fht.get_nfsattr().type == NF3DIR, res, timenow));
    } else comp_time(fd, h, fht.get_nfsattr().type == NF3DIR, NULL, 0, clnt_stat(0));
  }    
  return 0;
}

void nfs3_getattr(int fd, struct xfs_message_getattr *h,
		  ex_getattr3res *res, time_t rqtime, clnt_stat err) {

  assert(res->status == NFS3_OK);

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

  fh = new nfs_fh3; 
  *fh = fht.getnh(fht.getcur());
  ex_getattr3res *res = new ex_getattr3res;
  nfsc->call(lbfs_NFSPROC3_GETATTR, fh, res, 
	     wrap(&nfs3_getattr, fd, h, res, timenow));
  
  return 0;
}

void nfs3_write (int fd, struct xfs_message_putdata* h, ex_write3res *res, clnt_stat err) {
  
  if (res->status == NFS3_OK)
    xfs_send_message_wakeup (fd, h->header.sequence_num, 0);
  else warn << "nfs3_write: error: " << strerror(errno) << "(" << errno << ")\n";
}

void sendwrite(ref<condwrite3args> cwa) {
  int err, ost;
  uint64 offst = (*cwa->cvp)[cwa->chunk_index-1]->loc.pos();
  uint32 count = (*cwa->cvp)[cwa->chunk_index-1]->loc.count();
  while (count > 0) {
    ost = lseek(cwa->rfd, offst, SEEK_SET);
    if (count < NFS_MAXDATA)
      err = read(cwa->rfd, iobuf, count);
    else err = read(cwa->rfd, iobuf, NFS_MAXDATA);
    count -= err;
    offst += err;
    if (err < 0) {
      warn << "lbfs_condwrite: error: " << strerror(errno) << "(" << errno << ")\n";
      return;
    }
    wa = new write3args;
    wa->file = cwa->tmpfh;
    wa->offset = ost;
    wa->stable = UNSTABLE;
    wa->count = err;
    wa->data.setsize(err);
    memcpy (wa->data.base(), iobuf, err);	
    
    ex_write3res *wres = new ex_write3res;
    nfsc->call(lbfs_NFSPROC3_WRITE, wa, wres, 
	       wrap(&nfs3_write, cwa->fd, cwa->h, wres));
  }
}

void sendcondwrite(ref<condwrite3args> cwa) {

  cw = new lbfs_condwrite3args;
  cw->file = cwa->tmpfh;
  cw->offset = (*cwa->cvp)[cwa->chunk_index]->loc.pos();
  cw->count = (*cwa->cvp)[cwa->chunk_index]->loc.count();
  cw->fingerprint = (*cwa->cvp)[cwa->chunk_index]->fingerprint;

  lseek(cwa->rfd, (*cwa->cvp)[cwa->chunk_index]->loc.pos(), SEEK_SET);
  char *buf = new char[cw->count];
  int err = read(cwa->rfd, buf, cw->count);
  warn << "reading: chunk size " << cw->count << " got " << err << "\n";
  if (err < 0) {
    warn << "lbfs_condwrite: error: " << strerror(errno) << "(" << errno << ")\n";
    return;
  }
  sha1_hash(&cw->hash, buf, err);
  delete buf;
  
  cwa->res = new ex_write3res;
  cwa->chunk_index++;

  nfsc->call(lbfs_CONDWRITE, cw, cwa->res,
	     wrap(&lbfs_condwrite, cwa));
}

void committmp(ref<condwrite3args> cwa, ex_commit3res *res, time_t rqtime, clnt_stat err) {
  if (res->status == NFS3_OK) {

    if (fht.setcur(cwa->h->handle)) {
      warn << "committmp: Can't find node handle\n";
      return;
    } 
    ex_fattr3 attr = *(res->resok->file_wcc.after.attributes);
    attr.expire += rqtime;
    fht.set_nfsattr(attr);
    fht.set_ltime(attr.mtime, attr.ctime);
    xfs_send_message_wakeup (cwa->fd, cwa->h->header.sequence_num, 0);
  } else warn << "nfs3_committmp: " << strerror(res->status) << "\n";
  
}

void sendcommittmp(ref<condwrite3args> cwa) {

    //signal the server to commit the tmp file
    ct = new lbfs_committmp3args;
    ct->commit_from = cwa->tmpfh;

    if (fht.setcur(cwa->h->handle)) {
      warn << "xfs_getattr: Can't find node handle\n";
      return;
    }     
    ct->commit_to = fht.getnh(fht.getcur());
    
    ex_commit3res *cres = new ex_commit3res;
    nfsc->call(lbfs_COMMITTMP, ct, cres,
	       wrap(&committmp, cwa, cres, timenow));
}

void lbfs_condwrite(ref<condwrite3args> cwa, clnt_stat err) {

  if (cwa->res != NULL) {
    if (cwa->res->status == NFS3_OK) {
      if (cwa->res->resok->count != (*cwa->cvp)[cwa->chunk_index-1]->loc.count()) {
	warn << "lbfs_condwrite: did not write the whole chunk...\n";
	return;
      } 
    } else 
      if (cwa->res->status == NFS3ERR_FPRINTNOTFOUND) 
	sendwrite(cwa);
  }

  if (cwa->chunk_index < cwa->cvp->size()) {
    warn << "cwa->chunk_index = " << cwa->chunk_index << " size = " << cwa->cvp->size() << "\n";
    sendcondwrite(cwa);
  } else {
    sendcommittmp(cwa);
  }
}

void lbfs_mktmpfile(int fd, struct xfs_message_putdata* h, 
			 ex_diropres3 *res, clnt_stat err) {

  if (res->status != NFS3_OK) {
    warn << "lbfs_mktmpfile: error: " << strerror(res->status) << "(" << res->status << ")\n";
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
  
  char fname[MAXPATHLEN];
  strcpy(fname, fht.getcache_name());

  warn << "fname = " << fname << "\n";

  ref<condwrite3args> cwa = new refcounted<condwrite3args> (fd, h, 
							    *res->resok->obj.handle);
  cwa->cvp = new vec<lbfs_chunk *>;
  cwa->chunk_index = 0;
  cwa->res = NULL;

  if (chunk_file(CHUNK_SIZES(0), cwa->cvp, (char const*)fname) < 0) {
    warn << strerror(errno) << "(" << errno << "): lbfs_mktmpfile(chunkfile)\n";
    return;
  }
  
  cwa->rfd = open(fname, O_RDONLY, 0666);
  if (cwa->rfd > -1)
    lbfs_condwrite(cwa, clnt_stat(0));
  else warn << "lbfs_mktmpfile: error: " << strerror(errno) << "(" << errno << ")\n";

}

int xfs_message_putdata (int fd, struct xfs_message_putdata *h, u_int size) {
  
  warn << "putdata !!\n";

  if (fht.setcur(h->handle)) {
    warn << "xfs_putdata: Can't find node handle\n";
    return -1;
  } 

  //get temp file handle so the update will be atomic
  mt = new lbfs_mktmpfile3args;
  mt->commit_to = fht.getnh(fht.getcur());
  xfsattr2nfsattr(h->attr, &mt->obj_attributes);

  ex_diropres3 *res = new ex_diropres3;

  nfsc->call(lbfs_MKTMPFILE, mt, res,
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

void nfs3_setattr(int fd, struct xfs_message_putattr *h, ex_wccstat3 *res, time_t rqtime,
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
		  *res->wcc->after.attributes, rqtime, &msg.node);

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
  nfsc->call(lbfs_NFSPROC3_SETATTR, sa, res, 
	     wrap(&nfs3_setattr, fd, h, res, timenow));

  return 0;
}

void nfs3_create(int fd, struct xfs_message_create *h, ex_diropres3 *res, time_t rqtime, clnt_stat err) {

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
  nfsc->call(lbfs_NFSPROC3_CREATE, ca, res, 
	     wrap(&nfs3_create, fd, h, res, timenow));

  return 0;
}

void nfs3_mkdir(int fd, struct xfs_message_mkdir *h, ex_diropres3 *res, time_t rqtime,
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
    //msg1.node.tokens = same as parent dir's

    assert(res->resok->dir_wcc.after.present);
    
    nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		   *(res->resok->dir_wcc.after.attributes), rqtime, &msg1.node);

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

    msg3.node = msg2.node;
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
  nfsc->call(lbfs_NFSPROC3_MKDIR, ma, res, 
	     wrap(&nfs3_mkdir, fd, h, res, timenow));

  return 0;
}

int xfs_message_link(int fd, struct xfs_message_link *h, u_int size) {

  return 0;
}

void nfs3_symlink(int fd, struct xfs_message_symlink *h, ex_diropres3 *res,
		  time_t rqtime, clnt_stat err) {

  if (res->status == NFS3_OK) {
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
    
  }
}

int xfs_message_symlink(int fd, struct xfs_message_symlink *h, u_int size) {

  warn << "symlimk !!\n";

  if (fht.setcur(h->parent_handle)) {
    warn << "xfs_message_symlink: Can't find parent_handle\n";
    return -1;
  }

  sla = new symlink3args;
  sla->where.dir = fht.getnh(fht.getcur());
  sla->where.name = h->name;
  xfsattr2nfsattr(h->attr, &(sla->symlink.symlink_attributes));
  sla->symlink.symlink_data.setbuf(h->contents, strlen(h->contents));

  ex_diropres3 *res = new ex_diropres3;
  nfsc->call(lbfs_NFSPROC3_SYMLINK, sla, res,
	     wrap(&nfs3_symlink, fd, h, res, timenow));
  
  return 0;
}

void remove(int fd, struct xfs_message_remove *h, ex_lookup3res *lres,
	    ex_wccstat3 *wres, time_t rqtime, clnt_stat err) {

  if (lres->status == NFS3_OK && wres->status == NFS3_OK) {

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
    
    strcpy(msg1.cache_name, fht.getcache_name());
    fhandle_t cfh;
    if (getfh(msg1.cache_name, &cfh)) {
      warn << "getfh failed\n";
      return;
    }
    memmove(&msg1.cache_handle, &cfh, sizeof(cfh));
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
      msg2.header.opcode = XFS_MSG_INSTALLATTR;
      --a.attributes->nlink;
      nfsobj2xfsnode(h->cred, fht.getnh(fht.getcur()), 
		     *(a.attributes), rqtime, &msg1.node);

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
      //fht.remove(...);
      xfs_send_message_wakeup_multiple (fd,
					h->header.sequence_num,
					0,
					h0, h0_len,
					NULL, 0);
    }
      
  } else {
    int error;
    if (lres->status != NFS3_OK)
      error = lres->status;
    else error = wres->status;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    
    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, error,
				      h0, h0_len, NULL, 0);
  }
  //warn << strerror(errno) << "(" << errno << "): nfs3_lookup in remove\n";

}

void nfs3_remove(int fd, struct xfs_message_remove *h, ex_lookup3res *lres,
		 clnt_stat err) {

  if (lres->status == NFS3_OK) {

    if (lres->resok->obj_attributes.attributes->type == NF3DIR) {
      warn << "nfs3_remove: " << strerror(lres->status) << " calling rmdir\n";
      nfs3_rmdir(fd, (struct xfs_message_rmdir *)h, lres, clnt_stat(0));
      return;
    }   
    
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
    nfsc->call(lbfs_NFSPROC3_REMOVE, doa, wres,
	       wrap(&remove, fd, h, lres, wres, timenow));

  } else {
      warn << "nfs3_remove: lres->status = " << lres->status << "\n";
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
    
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, lres->status,
					h0, h0_len, NULL, 0);
    }
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
    
  nfsc->call(lbfs_NFSPROC3_LOOKUP, doa, res, 
	     wrap (&nfs3_remove, fd, h, res));

  return 0;
}

void nfs3_rmdir(int fd, struct xfs_message_rmdir *h, ex_lookup3res *lres,
		 clnt_stat err) {

  if (lres->status == NFS3_OK) {

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
    nfsc->call(lbfs_NFSPROC3_RMDIR, doa, wres,
	       wrap(&remove, fd, (struct xfs_message_remove *)h, lres, wres, timenow));

  } else {
    warn << "nfs3_rmdir: lres->status = " << lres->status << "\n";
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    
    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, lres->status,
				      h0, h0_len, NULL, 0);
  }
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
    
  nfsc->call(lbfs_NFSPROC3_LOOKUP, doa, res, 
	     wrap (&nfs3_rmdir, fd, h, res));

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
      }
      //ac.attr_enter (xa->handle, a, NULL);
      if (fht.setcur(xa->handle)) {
	warn << "cbdispatch: Can't find handle\n";
	return;
      }      
      fht.set_nfsattr(*a);

      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
