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
#include <xfs/xfs_pioctl.h>
#include "pioctl.h"
#include "crypt.h"

u_int64_t cache_entry::nextxh;
ihash<nfs_fh3, cache_entry, &cache_entry::nh,
  &cache_entry::nlink> nfsindex;
ihash<xfs_handle, cache_entry, &cache_entry::xh,
  &cache_entry::xlink> xfsindex;


/* Non-volatile File System Info */
ex_fsinfo3resok fsinfo;

xfs_message_function rcvfuncs[] = {
NULL,						/* version */
(xfs_message_function)xfs_message_wakeup,	/* wakeup */
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
(xfs_message_function)xfs_message_open,		/* open */
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
#if 0
NULL,	                                        /* wakeup_data */
NULL,						/* updatefid */
NULL,						/* advlock */
NULL						/* gc nodes */
#endif
};

void sendwrite (ref<condwrite3args > cwa, lbfs_chunk * chunk);
void lbfs_condwrite(ref<condwrite3args> cwa, clnt_stat err);
void normal_read(ref<getfp_args> ga, uint64 offset, uint32 count);
void nfs3_rmdir(int fd, ref<struct xfs_message_rmdir> h, 
                ref<ex_lookup3res> lres, clnt_stat err);
void condwrite_chunk(ref<condwrite3args> cwa);

#define OUTSTANDING_CONDWRITES 4
int outstanding_condwrites = 0;

void 
reply_err (int fd, u_int seqnum, int err)
{
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

#if DEBUG > 0
  if (err == EIO)
    warn << "sending xfs EIO\n";
#endif
  xfs_send_message_wakeup_multiple (fd, seqnum, err, h0, h0_len, NULL, 0);
}

int
xfs_message_wakeup (int fd, ref<struct xfs_message_wakeup> h, u_int size)
{
#if DEBUG > 0
  warn << "Got xfs_message_wakeup from XFS !!!\n";
#endif
  return 0;
}

void
xfs_expire_node_cb (int fd, struct xfs_msg_node node, xfs_handle handle)
{
  cache_entry *e = xfsindex[handle];
  if (e) {
#if DEBUG > 0
    warn << "xfs_expire_node_cb " << e->nfs_attr.expire 
         << " now " << (uint32)timenow << "\n";
#endif
  }
  if (e && e->incache && e->nfs_attr.expire <= (uint32)timenow) {
#if DEBUG > 0
    warn << "expire handle (" 
         << (int) handle.a << "," << (int) handle.b << ","
         << (int) handle.c << "," << (int) handle.d << ")\n";
#endif
    
    struct xfs_message_installdata msg;
    msg.header.opcode = XFS_MSG_INSTALLDATA;
    msg.node = node;
    msg.flag = XFS_ID_INVALID_DNLC;
    strcpy (msg.cache_name, e->cache_name);
    fhandle_t cfh;
    if (getfh (msg.cache_name, &cfh)) {
#if DEBUG > 0
      warn << "getfh failed, can't invalidate\n";
#endif
      return;
    }
    memmove (&msg.cache_handle, &cfh, sizeof (cfh));

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);
    xfs_send_message_wakeup_multiple (fd, 0, 0, h0, h0_len, NULL, 0);
  }
}

void 
getrootattr (int fd, ref<struct xfs_message_getroot> h, ref<sfs_fsinfo> fsi,
	     ref<ex_getattr3res > res, time_t rqtime, clnt_stat err)
{

  struct xfs_message_installroot msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  assert (!err && res->status == NFS3_OK);

#if DEBUG > 0
  warn << "uid = " << getuid () << "\n";
#endif

  nfsobj2xfsnode 
    (h->cred, fsi->nfs->v3->root, *res->attributes, rqtime, &msg.node);

  msg.header.opcode = XFS_MSG_INSTALLROOT;
  h0 = (struct xfs_message_header *) &msg;
  h0_len = sizeof (msg);

  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);
}

void 
nfs3_fsinfo (int fd, ref<struct xfs_message_getroot> h, ref<sfs_fsinfo> fsi,
	     ref<ex_fsinfo3res > res, clnt_stat err)
{

  assert (!err && res->status == NFS3_OK);

  fsinfo = *res->resok;

  ref<ex_getattr3res > ares = New refcounted < ex_getattr3res >;
  nfsc->call (lbfs_NFSPROC3_GETATTR, &fsi->nfs->v3->root, ares,
	      wrap (&getrootattr, fd, h, fsi, ares, timenow));
}

void 
sfs_getfsinfo (int fd, ref<struct xfs_message_getroot> h, ref<sfs_fsinfo> fsi, clnt_stat err)
{

  assert (fsi->prog == ex_NFS_PROGRAM && fsi->nfs->vers == ex_NFS_V3);
  x->compress ();
  ref<ex_fsinfo3res > res = New refcounted < ex_fsinfo3res >;

  nfsc->call (lbfs_NFSPROC3_FSINFO, &fsi->nfs->v3->root, res,
	      wrap (&nfs3_fsinfo, fd, h, fsi, res));
}

int 
xfs_message_getroot (int fd, ref<struct xfs_message_getroot> h, u_int size)
{
#if DEBUG > 0
  warn << "get root!!\n";
#endif

  ref<sfs_fsinfo> fsi = New refcounted<sfs_fsinfo>;
  sfsc->call (SFSPROC_GETFSINFO, NULL, fsi,
	      wrap (&sfs_getfsinfo, fd, h, fsi), NULL, NULL);
  return 0;
}

void 
nfs3_lookup (int fd, ref<struct xfs_message_getnode> h, uint32 seqnum,
	     ref<ex_lookup3res > lres, time_t rqtime, clnt_stat err)
{

  struct xfs_message_installnode msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  ex_post_op_attr a;

  if (err || lres->status != NFS3_OK) {
    if (err) {
#if DEBUG > 0
      warn << h->header.sequence_num << ":" << err << ":nfs3_lookup\n";
#endif
      reply_err(fd, seqnum, EIO);
    }
    else {
#if DEBUG > 0
      warn << h->header.sequence_num << ":" 
	   << strerror(lres->status) << ":nfs3_lookup\n";
#endif
      reply_err(fd, seqnum, lres->status);
    }
    return;
  }
  else {
    if (lres->resok->obj_attributes.present)
      a = lres->resok->obj_attributes;
    else if (lres->resok->dir_attributes.present)
      a = lres->resok->dir_attributes;
    else {
#if DEBUG > 0
      warn << h->header.sequence_num << ":" <<"lookup: error no attr present\n";
#endif
      reply_err(fd, seqnum, ENOSYS);
      return;
    }
  }

  nfsobj2xfsnode 
    (h->cred, lres->resok->object, *a.attributes, rqtime, &msg.node);

  msg.header.opcode = XFS_MSG_INSTALLNODE;
  msg.parent_handle = h->parent_handle;
  strcpy (msg.name, h->name);
  h0 = (struct xfs_message_header *) &msg;
  h0_len = sizeof (msg);

  xfs_send_message_wakeup_multiple (fd, seqnum, 0,
				    h0, h0_len, NULL, 0);
}

int 
xfs_message_getnode (int fd, ref<struct xfs_message_getnode> h, u_int size)
{
#if DEBUG > 0
  warn << h->header.sequence_num << ":" <<"get node !! msg.parent_handle ("
    << (int) h->parent_handle.a << ","
    << (int) h->parent_handle.b << ","
    << (int) h->parent_handle.c << ","
    << (int) h->parent_handle.d << ")\n";
  warn << h->header.sequence_num << ":" <<"file name = " << h->name << "\n";
#endif

  cache_entry *e = xfsindex[h->parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << h->header.sequence_num 
         << ":" << "xfs_message_getnode: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  diropargs3 doa;
  doa.dir = e->nh;
  doa.name = h->name;
#if DEBUG > 0
  warn << h->header.sequence_num 
       << ":" << "requesting file name " << doa.name << "\n";
#endif
  ref<ex_lookup3res> res = New refcounted<ex_lookup3res>;

  nfsc->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
	      wrap (&nfs3_lookup, fd, h, h->header.sequence_num, res, timenow));

  return 0;
}

void 
write_dirfile (int fd, ref<struct xfs_message_open> h, //cache_entry *e, 
	       ref<ex_readdir3res > res, write_dirent_args args, 
	       struct xfs_message_installdata msg,
	       clnt_stat cl_err)
{

  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  if (nfsdir2xfsfile (res, &args) < 0) {
    reply_err(fd, h->header.sequence_num, ENOENT);
    return;
  }

  if (args.last)
    flushbuf (&args);
  free (args.buf);

  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "write_dirfile: Can't find handle ("        
	 << h->handle.a << ","
	 << h->handle.b << ","
	 << h->handle.c << ","
	 << h->handle.d << ")\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return;
  }
     
  if (!res->resok->reply.eof) {
    readdir3args rda;
    rda.dir = e->nh; 
    entry3 *ent = res->resok->reply.entries;
    while (ent->nextentry != NULL)
      ent = ent->nextentry;
    rda.cookie = ent->cookie;
    rda.cookieverf = res->resok->cookieverf;
    rda.count = fsinfo.dtpref;

    ref<ex_readdir3res > rdres = New refcounted < ex_readdir3res >;
    nfsc->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
		wrap (&write_dirfile, fd, h, rdres, args, msg));
  }
  else {

    close (args.fd);

    e->incache = true;
    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);

    // unsigned exp = e->nfs_attr.expire - timenow + 1;
    // delaycb(exp, wrap(&xfs_expire_node_cb, fd, msg.node, h->handle));
  }
}

void 
nfs3_readdir (int fd, ref<struct xfs_message_open> h, cache_entry *e, 
	      ref<ex_readdir3res > res,
	      time_t rqtime, clnt_stat err)
{

  if (!err && res->status == NFS3_OK) {

    struct xfs_message_installdata msg;
    struct write_dirent_args args;
    
    ex_fattr3 attr = *res->resok->dir_attributes.attributes;
    nfsobj2xfsnode (h->cred, e->nh, attr, (uint32)timenow, &msg.node, true);
    e->ltime = max(attr.mtime, attr.ctime);

    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;

    // fill in cache_name, cache_handle, flag
    strcpy (msg.cache_name, e->cache_name);
    args.fd = open (msg.cache_name, O_CREAT | O_RDWR | O_TRUNC, 0666);

    if (args.fd < 0) {
#if DEBUG > 0
      warn << "readdir failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }

    fhandle_t cfh;
    if (getfh (msg.cache_name, &cfh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg.cache_handle, &cfh, sizeof (cfh));
    write_dirfile (fd, h, res, args, msg, clnt_stat (0));

  }
  else {
    if (err) {
#if DEBUG > 0
      warn << "nfs3_readdir: " << err << "\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_readdir: " << strerror (errno) << "(" << errno << ")\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
    }
  }
}

void 
write_file (ref<getfp_args> ga, uint64 offset, uint32 count,
	    ref<ex_read3res > res)
{
#if DEBUG > 0
  warn << "filename = " << ga->out_fname << " offset = " << offset << "\n";
#endif
  int out_fd = open (ga->out_fname, O_CREAT | O_WRONLY, 0666);
  if (out_fd < 0) {
#if DEBUG > 0
    warn << "write_file1: " << ga->out_fname << " " << strerror (errno) << "\n";
#endif
    return;
  }

  int err;
  if ((err = lseek (out_fd, offset, SEEK_SET)) < 0) {
#if DEBUG > 0
    warn << "write_file2: " << ga->out_fname << " " << strerror (errno) << "\n";
#endif
    return;
  }

  if ((err = write (out_fd, res->resok->data.base (), 
	            res->resok->data.size ())) < 0) {
#if DEBUG > 0
    warn << "write_file3: " << ga->out_fname << " " << strerror (errno) << "\n";
#endif
    return;
  }
  else if (err != (int) res->resok->data.size ()) {
#if DEBUG > 0
    warn << "write error or short write!!\n";
#endif
    return;
  }
  close (out_fd);

  if (res->resok->count < count)
    normal_read (ga, offset + res->resok->count, count - res->resok->count);
  else
    ga->blocks_written++;
}

void 
nfs3_read (ref<getfp_args> ga, uint64 offset, uint32 count,
	   ref<ex_read3res > res, clnt_stat err)
{
  if (!err && res->status == NFS3_OK) {
    assert (res->resok->file_attributes.present);

    write_file (ga, offset, count, res);

    if (ga->blocks_written == ga->total_blocks && ga->eof) {

      cache_entry *e = xfsindex[ga->h->handle];
      if (!e) {
#if DEBUG > 0
	warn << "nfs3_read: Can't find node handle\n";
#endif
        reply_err (ga->fd, ga->h->header.sequence_num, ENOENT);
	return;
      }
      
      e->cache_name = ga->msg.cache_name;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;

      e->incache = true;
      ga->msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &(ga->msg);
      h0_len = sizeof (ga->msg);

      xfs_send_message_wakeup_multiple (ga->fd, ga->h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);

      // add chunk to the database
      vec <lbfs_chunk *>cvp;
      if (chunk_file(CHUNK_SIZES (0), cvp, (char const *) ga->msg.cache_name) 
	  < 0) {
#if DEBUG > 0
	warn << strerror (errno) << "(" << errno << "): nfs3_read(chunkfile)\n";
#endif
	return;
      }
      for (uint i = 0; i < cvp.size (); i++) {
#if DEBUG > 0
	warn << "adding fp = " << cvp[i]->fingerprint << " to lbfsdb\n";
#endif
	cvp[i]->loc.set_fh (e->nh);
	lbfsdb.add_entry (cvp[i]->fingerprint, &(cvp[i]->loc));
        delete cvp[i];
      }
      lbfsdb.sync ();
    }
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << "nfs3_read: " << err << "\n";
#endif
      if (ga->retries < 1) {
        normal_read (ga, offset, count);
	ga->retries++;
      } 
      else 
	reply_err (ga->fd, ga->h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_read: " << strerror (res->status) << "\n";
#endif
      reply_err (ga->fd, ga->h->header.sequence_num, res->status);
    }
  }
}

void 
normal_read (ref<getfp_args> ga, uint64 offset, uint32 count)
{
  cache_entry *e = xfsindex[ga->h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "normal_read: Can't find node handle\n";
#endif
    reply_err (ga->fd, ga->h->header.sequence_num, ENOENT);
    return;
  }

  read3args ra;
  ra.file = e->nh;
  ra.offset = offset;
  ra.count = count < NFS_MAXDATA ? count : NFS_MAXDATA;
#if DEBUG > 0
  warn << "client normal read @" << offset << " +" << ra.count << "\n";
#endif

  ref<ex_read3res > rres = New refcounted < ex_read3res >;
  nfsc->call (lbfs_NFSPROC3_READ, &ra, rres,
	      wrap (&nfs3_read, ga, offset, count, rres));
}

void 
compose_file (ref<getfp_args> ga, ref<lbfs_getfp3res> res)
{

  int err, chfd, out_fd, j;
  uint64 offset = ga->offset;	// chunk position
  fp_db::iterator * ci = NULL;
  bool found = false;
  nfs_fh3 fh;
  lbfs_chunk_loc c;
  cache_entry *e = NULL;

  for (uint i=0; i<res->resok->fprints.size(); i++) {
    found = false;
    unsigned char buf[res->resok->fprints[i].count];
    // find matching fp in the database if found, write that chunk to the
    // file, otherwise, send for a normal read of that chunk
    if (!lbfsdb.get_iterator (res->resok->fprints[i].fingerprint, &ci)) {
#if DEBUG > 0
      if (!ci)
	warn << "ci is NULL\n";
#endif
      if (ci && !(ci->get (&c))) {
	do {
	  found = true;
	  c.get_fh (fh);

	  if (c.count () != res->resok->fprints[i].count) {
#if DEBUG > 0
	    warn << "chunk size != size from server..\n";
#endif
	    found = false;
	  }
	  else {
	    //read chunk c.pos() to c.count() from fh into buf 
	    e = nfsindex[fh];
	    if (!e) {
#if DEBUG > 0
	      warn << "compose_file: null fh or Can't find node handle\n";
#endif
	      found = false;
	      goto chunk_not_found;
	    }
#if DEBUG > 0
	    warn << "reading chunks from " << e->cache_name << "\n";
#endif
	    chfd = open (e->cache_name, O_RDONLY, 0666);
	    if (chfd < 0) {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
	      found = false;
	      goto chunk_not_found;
	    }
	    if (lseek (chfd, c.pos (), SEEK_SET) < 0) {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
	      found = false;
	      goto chunk_not_found;
	    }
	    if ((err = read (chfd, buf, c.count ())) > -1) {
	      if ((uint32) err != c.count ()) {
#if DEBUG > 0
		warn << "compose_file: error: " << err << " != " 
		     << c.count () << "\n";
#endif
	        found = false;
	        goto chunk_not_found;
	      }
	      if (compare_sha1_hash (buf, c.count (),
				     res->resok->fprints[i].hash)) {
#if DEBUG > 0
		warn << "compose_file: sha1 hash mismatch\n";
#endif
		found = false;
	        goto chunk_not_found;
	      }
	    }
	    else {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
	      found = false;
	      goto chunk_not_found;
	    }
	    close (chfd);
	  }

	  if (found) {
#if DEBUG > 0
	    warn << "FOUND!! compose_file: fp = " 
	         << res->resok->fprints[i].fingerprint << " in client DB\n";
#endif
	    out_fd = open (ga->out_fname, O_CREAT | O_WRONLY, 0666);
	    if (out_fd < 0) {
#if DEBUG > 0
	      warn << "compose_file: " << strerror (errno) << "\n";
#endif
              reply_err (ga->fd, ga->h->header.sequence_num, EIO);
	      return;
	    }

	    if (lseek (out_fd, offset, SEEK_SET) < 0) {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
              reply_err (ga->fd, ga->h->header.sequence_num, EIO);
	      return;
	    }
	    if ((err = write (out_fd, buf, c.count ())) > -1) {
	      if ((uint32) err != c.count ()) {
#if DEBUG > 0
		warn << "compose_file: error: " << err << " != " 
		     << c.count () << "\n";
#endif
                reply_err (ga->fd, ga->h->header.sequence_num, EIO);
		return;
	      }
	    }
	    else {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
              reply_err (ga->fd, ga->h->header.sequence_num, EIO);
	      return;
	    }
	    close (out_fd);
	    ga->blocks_written++;
	  }
	} while (!found && !(ci->next (&c)));
      }
      delete ci;
    }
chunk_not_found:
    if (!found) {
#if DEBUG > 0
      warn << "compose_file: fp = " << res->resok->fprints[i].fingerprint 
	   << " not in DB\n";
#endif
      normal_read (ga, offset, res->resok->fprints[i].count);
    }
    offset += res->resok->fprints[i].count;
  }
  ga->offset = offset;

  if (ga->blocks_written == ga->total_blocks && ga->eof) {
    e = xfsindex[ga->h->handle];
    if (!e) {
#if DEBUG > 0
      warn << "compose_file: Can't find node handle\n";
#endif
      reply_err (ga->fd, ga->h->header.sequence_num, ENOENT);
      return;      
    }
    e->cache_name = ga->msg.cache_name;

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    e->incache = true;
    ga->msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &(ga->msg);
    h0_len = sizeof (ga->msg);

    xfs_send_message_wakeup_multiple (ga->fd, ga->h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  }
}

void 
lbfs_getfp (ref<getfp_args> ga, ref<lbfs_getfp3res > res, time_t rqtime,
	    clnt_stat err)
{
  if (!err && res->status == NFS3_OK) {
    cache_entry *e = xfsindex[ga->h->handle];
    if (!e) {
#if DEBUG > 0
      warn << "lbfs_getfp: Can't find node handle (" 
	   << ga->h->handle.a << ","
	   << ga->h->handle.b << ","
	   << ga->h->handle.c << ","
	   << ga->h->handle.d << ")\n";
#endif
      reply_err (ga->fd, ga->h->header.sequence_num, ENOENT);
      return;
    }

    ex_fattr3 attr = *(res->resok->file_attributes.attributes);
    attr.expire += rqtime;
    e->nfs_attr = attr;
    e->ltime = max(attr.mtime, attr.ctime);

    ga->total_blocks += res->resok->fprints.size ();
    ga->eof = res->resok->eof;
    compose_file (ga, res);

    if (!res->resok->eof) {
      lbfs_getfp3args gfp;
      gfp.file = e->nh;
      gfp.offset = ga->offset; 
      gfp.count = LBFS_MAXDATA;
      if (res->resok->fprints.size() == 0) 
	gfp.count *= 2;

      ref<lbfs_getfp3res > fpres = New refcounted < lbfs_getfp3res >;
      nfsc->call (lbfs_GETFP, &gfp, fpres,
		  wrap (&lbfs_getfp, ga, fpres, timenow));
    }
  }
  else {
#if DEBUG > 0
    warn << "lbfs_getfp: " << strerror (res->status) << "\n";
#endif
    reply_err (ga->fd, ga->h->header.sequence_num, res->status);
  }
}

void 
nfs3_read_exist (int fd, ref<struct xfs_message_getdata> h, cache_entry *e)
{

  struct xfs_message_installdata msg;

  nfsobj2xfsnode (h->cred, e->nh,
		  e->nfs_attr, 0, &msg.node);

  msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;

  strcpy (msg.cache_name, e->cache_name);
  fhandle_t cfh;
  if (getfh (msg.cache_name, &cfh)) {
#if DEBUG > 0
    warn << "getfh failed\n";
#endif
    reply_err (fd, h->header.sequence_num, EIO);
    return;
  }
  memmove (&msg.cache_handle, &cfh, sizeof (cfh));

  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  e->incache = true;
  msg.header.opcode = XFS_MSG_INSTALLDATA;
  h0 = (struct xfs_message_header *) &msg;
  h0_len = sizeof (msg);

  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);

}

void 
getfp (int fd, ref<struct xfs_message_open> h, cache_entry *e)
{

  struct xfs_message_installdata msg;

  nfsobj2xfsnode (h->cred, e->nh,
		  e->nfs_attr, 0, &msg.node);
  msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;

  str fhstr = armor32(e->nh.data.base(), e->nh.data.size());
  int r = rnd.getword();
  str rstr = armor32((void*)&r, sizeof(int));
  str newcache = strbuf("cache/%02X/sfslbcd.%s.%s", 
			e->xh.a >> 8, fhstr.cstr(), rstr.cstr());

  strcpy (msg.cache_name, newcache);
  int cfd = open (msg.cache_name, O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (cfd < 0) {
#if DEBUG > 0
    warn << "xfs_message_getdata: " << strerror (errno) << "\n";
#endif
    reply_err (fd, h->header.sequence_num, EIO);
    return;
  }
  close (cfd);

  fhandle_t cfh;
  if (getfh (msg.cache_name, &cfh)) {
#if DEBUG > 0
    warn << "getfh failed\n";
#endif
    reply_err (fd, h->header.sequence_num, EIO);
    return;
  }
  memmove (&msg.cache_handle, &cfh, sizeof (cfh));

  ref<getfp_args> ga = New refcounted<getfp_args> (fd, h);
  ga->msg = msg;
  strcpy (ga->out_fname, msg.cache_name);

  lbfs_getfp3args gfp;
  gfp.file = e->nh;
  gfp.offset = 0;
  gfp.count = LBFS_MAXDATA;

  ref<lbfs_getfp3res > fpres = New refcounted < lbfs_getfp3res >;

  nfsc->call (lbfs_GETFP, &gfp, fpres,
	      wrap (&lbfs_getfp, ga, fpres, timenow));
}

bool 
greater (nfstime3 a, nfstime3 b)
{
  if (a.seconds > b.seconds)
    return true;
  else if (a.seconds == b.seconds &&
	   a.nseconds > b.nseconds)
    return true;
  else
    return false;
}

void 
comp_time (int fd, ref<struct xfs_message_open> h, bool dirfile,
	   ptr < ex_getattr3res > res, time_t rqtime, clnt_stat err)
{
  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "comp_time: Can't find node handle\n";
#endif
    reply_err (fd, h->header.sequence_num, ENOENT);
    return;
  }

  if (res != NULL) {
    if (!err && res->status == NFS3_OK) {
      ex_fattr3 attr = *(res->attributes);
      attr.expire += rqtime;
      e->nfs_attr = attr;
#if DEBUG > 0
      warn << "got attribute, ltime " << e->ltime.seconds << " "
	   << e->ltime.nseconds << "\n";
#endif
    } else {
      if (err) {
#if DEBUG > 0
	warn << "comp_time: " << err << "\n";
#endif
      }
      else {
#if DEBUG > 0
	warn << "comp_time: " << strerror(res->status) << "\n";
#endif
      }
    }
  }

  nfstime3 maxtime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
#if DEBUG > 0
  warn << "got attribute, maxtime " << maxtime.seconds  
       << " " << maxtime.nseconds << "\n";
#endif
  if (greater (maxtime, e->ltime)) {
    if (dirfile) {
#if DEBUG > 0
      warn << "calling NFS readdir\n";
#endif
      readdir3args rda;
      rda.dir = e->nh;
      rda.cookie = 0;
      rda.cookieverf = cookieverf3 ();
      rda.count = fsinfo.dtpref;

      ref<ex_readdir3res > rdres = New refcounted < ex_readdir3res >;
      nfsc->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
		  wrap (&nfs3_readdir, fd, h, e, rdres, timenow));
    }
    else
      getfp (fd, h, e);
  }
  else {
    ref<struct xfs_message_getdata> hga = 
      New refcounted <struct xfs_message_getdata>;
    *hga = *(*(reinterpret_cast< ref<struct xfs_message_getdata>* >(&h)));
    nfs3_read_exist (fd, hga, e);
  }
}

void 
nfs3_readlink (int fd, ref<struct xfs_message_open> h, cache_entry *e,
	       ref<ex_readlink3res > res,
	       time_t rqtime, clnt_stat err)
{

  if (!err && res->status == NFS3_OK) {

    struct xfs_message_installdata msg;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    
    ex_fattr3 attr = *res->resok->symlink_attributes.attributes;
    nfsobj2xfsnode (h->cred, e->nh,
		    attr, rqtime, &msg.node);
    e->ltime = max(attr.mtime, attr.ctime);

    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;

    strcpy (msg.cache_name, e->cache_name);

    int lfd = open (msg.cache_name, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (lfd < 0) {
#if DEBUG > 0
      warn << "readlink failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }

    fhandle_t cfh;
    if (getfh (msg.cache_name, &cfh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }

    memmove (&msg.cache_handle, &cfh, sizeof (cfh));
    write (lfd, res->resok->data.cstr (), res->resok->data.len ());
    close (lfd);

    e->incache = true;
    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  } else {
    if (err) {
#if DEBUG > 0
      warn << "nfs3_readlink: " << err << "\n";
#endif
    }
    else {
#if DEBUG > 0
      warn << "nfs3_readlink: " << strerror(res->status) << "\n";
#endif
    }
  }
}

int 
xfs_message_getdata (int fd, ref<struct xfs_message_getdata> h, u_int size)
{
#if DEBUG > 0
  warn << "XXXX getdata!! msg.handle ("
    << (int) h->handle.a << ","
    << (int) h->handle.b << ","
    << (int) h->handle.c << ","
    << (int) h->handle.d << ")\n";
#endif

  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_getdata: Can't find node handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }
  assert(e->nfs_attr.type != NF3DIR);
  
#if 1
  assert(e->incache);
  nfs3_read_exist (fd, h, e);
#else
  ref<struct xfs_message_open> ho = New refcounted <struct xfs_message_open>;
  *ho = *(*(reinterpret_cast< ref<struct xfs_message_open>* >(&h)));
  xfs_message_open (fd, ho, size);
#endif

  return 0;
}

int
xfs_message_open (int fd, ref<struct xfs_message_open> h, u_int size)
{

#if DEBUG > 0
  warn << "XXXX open!! " << h->tokens << " msg.handle ("
    << (int) h->handle.a << ","
    << (int) h->handle.b << ","
    << (int) h->handle.c << ","
    << (int) h->handle.d << ")\n";
#endif

  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_open: Can't find node handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }
    
#if DEBUG > 0
  if (e->nfs_attr.type == NF3DIR)
    warn << "xfs_message_open on directory: " << e->writers << "\n";
#endif
  
  uint32 owriters = e->writers;
  if (h->tokens & (XFS_OPEN_NW|XFS_OPEN_EW)) {
    e->writers = 1;
#if DEBUG > 0
    warn << "open for write: " << e->writers << " writers\n";
#endif
  }

  if (e->nfs_attr.type == NF3LNK) {
#if DEBUG > 0
    warn << "reading a symlink!!\n";
#endif
    nfs_fh3 fh = e->nh;
    ref<ex_readlink3res > rlres = New refcounted < ex_readlink3res >;
    nfsc->call (lbfs_NFSPROC3_READLINK, &fh, rlres,
		wrap (&nfs3_readlink, fd, h, e, rlres, timenow));
    return 0;
  }

  if (!e->incache) {
    if (e->nfs_attr.type == NF3DIR) {
#if DEBUG > 0
      warn << "calling NFS readdir\n";
#endif
      readdir3args rda;
      rda.dir = e->nh;
      rda.cookie = 0;
      rda.cookieverf = cookieverf3 ();
      rda.count = fsinfo.dtpref;

      ref<ex_readdir3res > rdres = New refcounted < ex_readdir3res >;
      nfsc->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
		  wrap (&nfs3_readdir, fd, h, e, rdres, timenow));
    }
    else 
      if (e->nfs_attr.type == NF3REG)
	getfp (fd, h, e);
  }
  else {
#if DEBUG > 0
    if (e->nfs_attr.type == NF3DIR) 
      warn << "directory in cache, writers " << e->writers << "\n";
#endif

    // don't read if there are other writers on the cache
    if (owriters > 0) {
      ref<struct xfs_message_getdata> hga = 
        New refcounted <struct xfs_message_getdata>;
      *hga = *(*(reinterpret_cast< ref<struct xfs_message_getdata>* >(&h)));
      nfs3_read_exist (fd, hga, e);
    }

#if DEBUG > 0
    else if (e->nfs_attr.type == NF3DIR) 
      warn << "directory in cache, exp " << e->nfs_attr.expire << " " 
	   << (uint32) timenow << " ltime " << e->ltime.seconds << "\n";
#endif
    else if (e->nfs_attr.expire < (uint32) timenow) {
#if DEBUG > 0
      if (e->nfs_attr.type == NF3DIR) 
	warn << "GETATTR called\n";
#endif
      nfs_fh3 fh = e->nh;
      ptr < ex_getattr3res > res = New refcounted < ex_getattr3res >;
      nfsc->call (lbfs_NFSPROC3_GETATTR, &fh, res,
		  wrap (&comp_time, fd, h,
			e->nfs_attr.type == NF3DIR, res, timenow));
    }
    else
      comp_time (fd, h, e->nfs_attr.type == NF3DIR, NULL, 0, clnt_stat (0));
  }
  return 0;
}

void 
nfs3_getattr (int fd, ref<struct xfs_message_getattr> h, cache_entry *e,
	      ref<ex_getattr3res > res, time_t rqtime, clnt_stat err)
{

  if (!err && res->status == NFS3_OK) {

  struct xfs_message_installattr msg;
  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  nfsobj2xfsnode (h->cred, e->nh, *res->attributes, rqtime, &msg.node);
  if (e->nfs_attr.type == NF3DIR) {
    nfstime3 maxtime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
    if (!greater(maxtime, e->ltime))
      e->nfs_attr.expire += rqtime;
  }

  msg.header.opcode = XFS_MSG_INSTALLATTR;
  h0 = (struct xfs_message_header *) &msg;
  h0_len = sizeof (msg);

  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);
  } else {
#if DEBUG > 0
    if (err) 
      warn << "nfs3_getattr: " << err << "\n";
    else {
      warn << "nfs3_getattr: " << strerror(res->status) << "\n";
    }
#endif
  }
}

int 
xfs_message_getattr (int fd, ref<struct xfs_message_getattr> h, u_int size)
{
#if DEBUG > 0
  warn << "get attr !!\n";
  warn << "msg.handle ("
    << (int) h->handle.a << ","
    << (int) h->handle.b << ","
    << (int) h->handle.c << ","
    << (int) h->handle.d << ")\n";
#endif

  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_getattr: Can't find node handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  nfs_fh3 fh = e->nh;
  ref<ex_getattr3res > res = New refcounted < ex_getattr3res >;
  nfsc->call (lbfs_NFSPROC3_GETATTR, &fh, res,
	      wrap (&nfs3_getattr, fd, h, e, res, timenow));

  return 0;
}

void 
committmp (ref<condwrite3args > cwa, ref<ex_commit3res > res, 
           time_t rqtime, clnt_stat err)
{
  if (!err && res->status == NFS3_OK) {

    cache_entry *e = xfsindex[cwa->h->handle];
    if (!e) {
#if DEBUG > 0
      warn << "committmp: Can't find node handle(" 
	   << cwa->h->handle.a << ","
	   << cwa->h->handle.b << ","
	   << cwa->h->handle.c << ","
	   << cwa->h->handle.d << ")\n";
#endif

      reply_err(cwa->fd, cwa->h->header.sequence_num, ENOENT);
      return;
    }
#if DEBUG > 0
    warn << "committmp wake xfs!\n";
#endif
    ex_fattr3 attr = *(res->resok->file_wcc.after.attributes);
    attr.expire += rqtime;
    e->nfs_attr = attr;
    e->ltime = max(attr.mtime, attr.ctime);
    xfs_send_message_wakeup (cwa->fd, cwa->h->header.sequence_num, 0);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << "nfs3_committmp: " << err << "\n";
#endif
      reply_err (cwa->fd, cwa->h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_committmp: " << strerror (res->status) << "\n";
#endif
      reply_err (cwa->fd, cwa->h->header.sequence_num, res->status);
    }
  }
}

void 
sendcommittmp (ref<condwrite3args > cwa)
{
  lbfs_committmp3args ct;
  ct.commit_from = cwa->tmpfh;
  
  cache_entry *e = xfsindex[cwa->h->handle];  
  if (!e) {
#if DEBUG > 0
    warn << "xfs_getattr: Can't find node handle\n";
#endif
    reply_err(cwa->fd, cwa->h->header.sequence_num, ENOENT);
    return;
  }
  ct.commit_to = e->nh;

  cwa->commited = true;
#if DEBUG > 0
  warn << "YYYY " << cwa->h->header.sequence_num << " COMMITTMP: "
       << cwa->blocks_written << " blocks written " 
       << cwa->total_blocks << " needed, eof? "
       << cwa->eof << "\n";
#endif
  ref<ex_commit3res > cres = New refcounted < ex_commit3res >;
  nfsc->call (lbfs_COMMITTMP, &ct, cres,
	      wrap (&committmp, cwa, cres, timenow));
}

void 
nfs3_write (ref<condwrite3args > cwa, lbfs_chunk *chunk, 
            ref<ex_write3res > res, clnt_stat err)
{
  if (outstanding_condwrites > 0) outstanding_condwrites--;
  if (!err && res->status == NFS3_OK) {
#if DEBUG > 0
    warn << cwa->h->header.sequence_num << " nfs3_write: @"
         << chunk->loc.pos() << ", "
         << res->resok->count << " total needed "
	 << chunk->loc.count() << " had "
	 << chunk->aux_count << "\n";
#endif
    chunk->aux_count += res->resok->count;
    assert(chunk->aux_count <= chunk->loc.count());
    if (chunk->aux_count == chunk->loc.count()) 
      cwa->blocks_written++;
#if DEBUG > 0
    warn << cwa->h->header.sequence_num << " nfs3_write: @"
         << chunk->loc.pos() << " +"
	 << chunk->loc.count() << " "
         << cwa->blocks_written << " blocks written " 
	 << cwa->total_blocks << " needed, eof? "
	 << cwa->eof << "\n";
#endif
    if (cwa->blocks_written == cwa->total_blocks && cwa->eof)
      sendcommittmp (cwa);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << cwa->h->header.sequence_num << " nfs3_write: " << err << "\n";
#endif
      if (cwa->retries < 1) {
        sendwrite(cwa, chunk);
        cwa->retries++;
      } 
      else 
	reply_err (cwa->fd, cwa->h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_write: error: " << strerror (res->status) << "\n";
      warn << "-> " << cwa->h->header.sequence_num << " nfs3_write: "
           << cwa->blocks_written << " blocks written " 
	   << cwa->total_blocks << " needed, eof? "
	   << cwa->eof << "\n";
#endif
      reply_err (cwa->fd, cwa->h->header.sequence_num, res->status);
    }
  }
  if (!cwa->eof && outstanding_condwrites < OUTSTANDING_CONDWRITES) 
    condwrite_chunk(cwa);
}

void 
sendwrite (ref<condwrite3args > cwa, lbfs_chunk * chunk)
{
  int err, ost;
  char iobuf[NFS_MAXDATA];
  uint64 offst = chunk->loc.pos ();
  uint32 count = chunk->loc.count ();

  if (cwa->commited) {
#if DEBUG > 0
    warn << "weird: already commited, should not be sending more data!\n";
#endif
    assert(0);
  }

  int rfd = open (cwa->fname, O_RDONLY, 0666);
  if (rfd < 0) {
#if DEBUG > 0
    warn << "sendwrite: " << strerror (errno) << "\n";
#endif
    reply_err(cwa->fd, cwa->h->header.sequence_num, EIO);
    return;
  }

  while (count > 0) {
    ost = lseek (rfd, offst, SEEK_SET);
    if (count < NFS_MAXDATA)
      err = read (rfd, iobuf, count);
    else
      err = read (rfd, iobuf, NFS_MAXDATA);
    if (err < 0) {
#if DEBUG > 0
      warn << "sendwrite: error: " << strerror (errno) << "(" << errno << ")\n";
#endif
      reply_err(cwa->fd, cwa->h->header.sequence_num, EIO);
      return;
    }
    count -= err;
    offst += err;
    write3args wa;
    wa.file = cwa->tmpfh;
    wa.offset = ost;
    wa.stable = UNSTABLE;
    wa.count = err;
    wa.data.setsize (err);
    memcpy (wa.data.base (), iobuf, err);

    ref<ex_write3res > res = New refcounted < ex_write3res >;
    outstanding_condwrites++;
    nfsc->call (lbfs_NFSPROC3_WRITE, &wa, res,
		wrap (&nfs3_write, cwa, chunk, res));
  }
  close (rfd);
}

void 
lbfs_sendcondwrite (ref<condwrite3args > cwa, lbfs_chunk * chunk,
		    ref<ex_write3res > res, clnt_stat err)
{
  if (outstanding_condwrites > 0) outstanding_condwrites--;
  if (!err && res->status == NFS3_OK) {
    if (res->resok->count != chunk->loc.count ()) {
#if DEBUG > 0
      warn << "lbfs_sendcondwrite: did not write the whole chunk...\n";
#endif
      sendwrite (cwa, chunk);
      return;
    }
    chunk->aux_count += chunk->loc.count();
    cwa->blocks_written++;
#if DEBUG > 0
    warn << cwa->h->header.sequence_num << " condwrite: @"
         << chunk->loc.pos() << " +"
	 << chunk->loc.count() << " "
         << cwa->blocks_written << " blocks written " 
	 << cwa->total_blocks << " needed, eof? "
	 << cwa->eof << "\n";
#endif
    if (cwa->blocks_written == cwa->total_blocks && cwa->eof)
      sendcommittmp (cwa);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << "lbfs_sendcondwrite: " << err << "\n";
      warn << "-> " << cwa->h->header.sequence_num << " condwrite: "
           << cwa->blocks_written << " blocks written " 
	   << cwa->total_blocks << " needed, eof? "
	   << cwa->eof << "\n";
#endif
      sendwrite (cwa, chunk);
    }
    else {
      if (res->status != NFS3ERR_FPRINTNOTFOUND) {
#if DEBUG > 0
	warn << "lbfs_sendcondwrite: " << strerror (res->status) << "\n";
#endif
        reply_err (cwa->fd, cwa->h->header.sequence_num, res->status);
      }
      else 
	sendwrite (cwa, chunk);
    }
  }
  if (!cwa->eof && outstanding_condwrites < OUTSTANDING_CONDWRITES) 
    condwrite_chunk(cwa);
}

void 
sendcondwrite (ref<condwrite3args > cwa, lbfs_chunk * chunk)
{
  if (cwa->commited) {
#if DEBUG > 0
    warn << "weird: already commited, should not be sending more data!\n";
#endif
    assert(0);
  }

  lbfs_condwrite3args cw;
  cw.file = cwa->tmpfh;
  cw.offset = chunk->loc.pos ();
  cw.count = chunk->loc.count ();
  cw.fingerprint = chunk->fingerprint;

  int rfd = open (cwa->fname, O_RDONLY, 0666);
  if (rfd < 0) {
#if DEBUG > 0
    warn << "sendcondwrite: " << cwa->fname << ".." << strerror (errno) << "\n";
#endif
    sendwrite(cwa, chunk);
    return;
  }

  lseek (rfd, chunk->loc.pos (), SEEK_SET);
  char buf[cw.count];
  unsigned total_read = 0;
  while (total_read < cw.count) {
    int err = read (rfd, &buf[total_read], cw.count);
    if (err < 0) {
#if DEBUG > 0
      warn << "lbfs_condwrite: error: " << strerror (errno) 
           << "(" << errno << ")\n"; 
#endif
      sendwrite(cwa, chunk);
      return;
    }
    total_read += err;
  }
  assert(total_read == cw.count);
  sha1_hash (&cw.hash, buf, total_read);
  close (rfd);

  ref<ex_write3res > res = New refcounted < ex_write3res >;

  nfsc->call (lbfs_CONDWRITE, &cw, res,
	      wrap (&lbfs_sendcondwrite, cwa, chunk, res));
}

void 
lbfs_mktmpfile (int fd, ref<struct xfs_message_putdata> h,
		ref<ex_diropres3 > res, clnt_stat err)
{
  if (err) {
#if DEBUG > 0
    warn << "lbfs_mktmpfile: " << err << "\n";
#endif
    reply_err(fd, h->header.sequence_num, EIO);
    return;
  }
  else {
    if (res->status != NFS3_OK) {
#if DEBUG > 0
      warn << "lbfs_mktmpfile: error: " << strerror (res->status) 
	   << "(" << res->status << ")\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
      return;
    }
    else if (!res->resok->obj.present) {
#if DEBUG > 0
      warn << "tmpfile handle not present\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
      return;
    }
  }
  
  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_getattr: Can't find node handle\n";
#endif
    reply_err (fd, h->header.sequence_num, ENOENT);
    return;
  }

#if DEBUG > 0
  warn << "can do condwrite\n";
#endif
  ref<condwrite3args > cwa = 
    New refcounted < condwrite3args > (fd, h, *res->resok->obj.handle);
  strcpy (cwa->fname, e->cache_name);
  cwa->chunker = New Chunker(CHUNK_SIZES(0));
  cwa->cur_pos = 0;
  condwrite_chunk(cwa);
}

void condwrite_chunk(ref<condwrite3args> cwa)
{
  int data_fd = open (cwa->fname, O_RDONLY, 0666);
  if (data_fd < 0) {
#if DEBUG > 0
    warn << "condwrite_chunk: " << strerror (errno) << "\n";
#endif
    reply_err (cwa->fd, cwa->h->header.sequence_num, EIO);
    return;
  }
  uint index, v_size;
  index = v_size = cwa->chunker->chunk_vector().size();
  if (lseek (data_fd, cwa->cur_pos, SEEK_SET) < 0) {
#if DEBUG > 0
    warn << "condwrite_chunk: " << strerror (errno) << "\n";
#endif
    reply_err (cwa->fd, cwa->h->header.sequence_num, EIO);
    return;
  }
  uint count;
  unsigned char buf[4096];
  while ((count = read(data_fd, buf, 4096)) > 0) {
    cwa->cur_pos += count;
    cwa->chunker->chunk(buf, count);
    if (cwa->chunker->chunk_vector().size() > v_size) {
      v_size = cwa->chunker->chunk_vector().size();
      cwa->total_blocks = v_size;
      for (; index < v_size; index++) {
#if DEBUG > 0
	warn << "chindex = " << index << " size = " << v_size << "\n";
#endif
	cwa->chunker->chunk_vector()[index]->aux_count = 0;
	outstanding_condwrites++;
        sendcondwrite(cwa, cwa->chunker->chunk_vector()[index]);
	lbfsdb.add_entry (cwa->chunker->chunk_vector()[index]->fingerprint,
			  &(cwa->chunker->chunk_vector()[index]->loc));
      }
      if (outstanding_condwrites >= OUTSTANDING_CONDWRITES) break;
    }
  }
  close(data_fd);
  if (count < 0) {
#if DEBUG > 0
    warn << "condwrite_chunk: " << strerror (errno) << "\n";
#endif
    reply_err (cwa->fd, cwa->h->header.sequence_num, EIO);
    return;
  }
  if (count == 0) {
    cwa->chunker->stop();
    v_size = cwa->chunker->chunk_vector().size();
    cwa->total_blocks = cwa->chunker->chunk_vector().size();
    for (; index < v_size; index++) {
#if DEBUG > 0
      warn << "chindex = " << index << " size = " <<  cwa->total_blocks<< "\n";
#endif
      cwa->chunker->chunk_vector()[index]->aux_count = 0;
      sendcondwrite(cwa, cwa->chunker->chunk_vector()[index]);
    }
    cwa->eof = true;
  }
#if DEBUG > 0
  warn << "total_blocks = "  << cwa->total_blocks << " " 
       << count << " eof " << cwa->eof << "\n";
#endif
  if (cwa->eof && cwa->total_blocks == 0)
    sendcommittmp(cwa);
}

int 
xfs_message_putdata (int fd, ref<struct xfs_message_putdata> h, u_int size)
{

#if DEBUG > 0
  warn << "XXXX putdata!! msg.handle ("
    << (int) h->handle.a << ","
    << (int) h->handle.b << ","
    << (int) h->handle.c << ","
    << (int) h->handle.d << ")\n";
  warn << "putdata " << h->flag << "\n";
#endif

  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_putdata: Can't find node handle\n";
#endif
    reply_err (fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  // get temp file handle so the update will be atomic
  lbfs_mktmpfile3args mt;
  mt.commit_to = e->nh;
  xfsattr2nfsattr (h->header.opcode, h->attr, &mt.obj_attributes);

  ref<ex_diropres3 > res = New refcounted < ex_diropres3 >;

  nfsc->call (lbfs_MKTMPFILE, &mt, res,
	      wrap (&lbfs_mktmpfile, fd, h, res));

  // XXX - benjie: more madness... xfs doesn't really tell us if it is closing
  // the file, or simply doing a fsync and then may write some more. the code
  // says if the FSYNC bit is on, then it is doing a fsync. but then,
  // sometimes, it also never calls putdata again afterward... we just make
  // the file not open for writing. if another read occured on the same file,
  // tough luck?
  e->writers = 0;

#if 0
  if (!(h->flag&XFS_FSYNC) && e->writers>0) {
    e->writers--;
#if DEBUG > 0
    warn << "close for write: " << e->writers << " writers\n";
#endif
  }
#endif
  return 0;
}

int 
xfs_message_inactivenode (int fd, ref<struct xfs_message_inactivenode> h, 
                          u_int size)
{

#if DEBUG > 0
  warn << "inactivenode !!(" 
       << h->handle.a << ","
       << h->handle.b << ","
       << h->handle.c << ","
       << h->handle.d << ")\n";
#endif
  if (h->flag == XFS_DELETE || h->flag == XFS_NOREFS) {
    cache_entry *e = xfsindex[h->handle];
    if (e) {
      e->incache = false;
#if 0 //keep everything for now
      delete e;
#endif 
    }
  }
  return 0;
}

void 
nfs3_setattr (int fd, ref<struct xfs_message_putattr> h, ref<ex_wccstat3 > res,
	      time_t rqtime, clnt_stat err)
{
  if (!err && res->status == NFS3_OK) {

    assert (res->wcc->after.present);

    struct xfs_message_installattr msg;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    cache_entry *e = xfsindex[h->handle];
    if (!e) {
#if DEBUG > 0
      warn << "nfs3_setattr: Can't find node handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }
    nfsobj2xfsnode (h->cred, e->nh,
		    *res->wcc->after.attributes, rqtime, &msg.node);

    msg.header.opcode = XFS_MSG_INSTALLATTR;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_setattr\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << strerror (res->status) << ": nfs3_setattr\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
    }
  }
}

int 
xfs_message_putattr (int fd, ref<struct xfs_message_putattr> h, u_int size)
{

#if DEBUG > 0
  warn << "putattr !!\n";
#endif

  cache_entry *e = xfsindex[h->handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_putattr: Can't find node handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  setattr3args sa;
  sa.object = e->nh;
  xfsattr2nfsattr (h->header.opcode, h->attr, &sa.new_attributes);
  sa.guard.set_check (false);
#if 0
  if (sa->guard.check)
    sa.guard.ctime->seconds = h->attr.xa_ctime;
#endif
  if (sa.new_attributes.size.set) {
#if DEBUG > 0
    warn << "setting size to " 
         << (uint32) *(sa.new_attributes.size.val) << "\n";
#endif
    // can't depend on client set time to expire cache data
    if (*(sa.new_attributes.size.val) == 0) 
      e->ltime.seconds = 0;
  }
  ref<ex_wccstat3 > res = New refcounted < ex_wccstat3 >;
  nfsc->call (lbfs_NFSPROC3_SETATTR, &sa, res,
	      wrap (&nfs3_setattr, fd, h, res, timenow));

  return 0;
}

void 
nfs3_create (int fd, ref<struct xfs_message_create> h, ref<ex_diropres3 > res,
	     time_t rqtime, clnt_stat err)
{
  
  if (!err && res->status == NFS3_OK) {
 
    struct xfs_message_installdata msg1;	//change content of parent dir
    struct xfs_message_installnode msg2;	//New file node
    struct xfs_message_installdata msg3;	//New file content (null)
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    struct xfs_message_header *h2 = NULL;
    size_t h2_len = 0;

    assert (res->resok->obj.present && res->resok->obj_attributes.present);
    nfsobj2xfsnode (h->cred, *(res->resok->obj.handle),
	            *(res->resok->obj_attributes.attributes), 
		    rqtime, &msg2.node);

    cache_entry *e1 = nfsindex[*(res->resok->obj.handle)];
    if (!e1) {
#if DEBUG > 0
      warn << "nfs3_create: Can't find node handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }
    
    strcpy (msg3.cache_name, e1->cache_name);
    int new_fd = open (msg3.cache_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
    
    if (new_fd < 0) {
#if DEBUG > 0
      warn << "nfs3_create: " << strerror (errno) << "\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    close (new_fd);

    fhandle_t new_fh;
    if (getfh (msg3.cache_name, &new_fh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg3.cache_handle, &new_fh, sizeof (new_fh));

    // write new direntry to parent dirfile (do a readdir or just append that
    // entry?)
    cache_entry *e2 = xfsindex[h->parent_handle];
    if (!e2) {
#if DEBUG > 0
      warn << "nfs3_create: Can't find parent handle\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    
    strcpy (msg1.cache_name, e2->cache_name);
    // msg1.node.tokens = same as parent dir's

    assert (res->resok->dir_wcc.after.present);
  
    nfsobj2xfsnode (h->cred, e2->nh,
		    *(res->resok->dir_wcc.after.attributes), 
		    0, &msg1.node);
    
    // XXX - benjie: this is sad... if i was jwz, i'd be writing how i would
    // be going postal on people who implements nfs: on openbsd, we can't
    // depend on directory attributes (in particularly mtime) to be properly
    // updated after a create...
    e2->incache = false;
    msg1.flag = XFS_ID_INVALID_DNLC;
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg1;
    h0_len = sizeof (msg1);
    
    msg2.node.tokens = XFS_ATTR_R
      | XFS_OPEN_NW | XFS_OPEN_NR
      | XFS_DATA_R | XFS_DATA_W;	//override nfsobj2xfsnode?
    
    msg2.parent_handle = h->parent_handle;
    strlcpy (msg2.name, h->name, sizeof (msg2.name));
    
    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *) &msg2;
    h1_len = sizeof (msg2);

    e1->incache = true;
    e1->writers = 1;
    msg3.node = msg2.node;
    msg3.flag = 0;
    msg3.header.opcode = XFS_MSG_INSTALLDATA;
    
    h2 = (struct xfs_message_header *) &msg3;
    h2_len = sizeof (msg3);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, h2, h2_len,
				      NULL, 0);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_create\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << strerror (res->status) << ": nfs3_create\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
    }
  }
}

int 
xfs_message_create (int fd, ref<struct xfs_message_create> h, u_int size)
{

#if DEBUG > 0
  warn << "create !!\n";
#endif

  cache_entry *e = xfsindex[h->parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_create: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  create3args ca;
  ca.where.dir = e->nh;
  ca.where.name = h->name;
  ca.how.set_mode (GUARDED);
  if (ca.how.mode == UNCHECKED || ca.how.mode == GUARDED)
    xfsattr2nfsattr (h->header.opcode, h->attr, &(*ca.how.obj_attributes));
  else {
#if DEBUG > 0
    warn << "xfs_message_create: create mode not UNCHECKED or GUARDED\n";
#endif
  }

  ref<ex_diropres3 > res = New refcounted < ex_diropres3 >;
  nfsc->call (lbfs_NFSPROC3_CREATE, &ca, res,
	      wrap (&nfs3_create, fd, h, res, timenow));

  return 0;
}

void 
nfs3_mkdir (int fd, ref<struct xfs_message_mkdir> h, ref<ex_diropres3 > res,
	    time_t rqtime, clnt_stat err)
{

  if (!err && res->status == NFS3_OK) {

    struct xfs_message_installdata msg1;	//change content of parent dir
    struct xfs_message_installnode msg2;	//new dir node
    struct xfs_message_installdata msg3;	//new dir content (null)
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    struct xfs_message_header *h2 = NULL;
    size_t h2_len = 0;

    assert (res->resok->obj.present && res->resok->obj_attributes.present);
    nfsobj2xfsnode (h->cred, *(res->resok->obj.handle),
	           *(res->resok->obj_attributes.attributes), 
		   rqtime, &msg2.node);

    cache_entry *e1 = nfsindex[*(res->resok->obj.handle)];
    if (!e1) {
#if DEBUG > 0
      warn << "nfs3_mkdir: Can't find node handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }

    strcpy (msg3.cache_name, e1->cache_name);
    int new_fd = open (msg3.cache_name, O_CREAT, 0666);

    if (new_fd < 0) {
#if DEBUG > 0
      warn << "nfs3_mkdir: " << errno << " " << strerror (errno) << "\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    close (new_fd);

    fhandle_t new_fh;
    if (getfh (msg3.cache_name, &new_fh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg3.cache_handle, &new_fh, sizeof (new_fh));

    cache_entry *e2 = xfsindex[h->parent_handle];
    if (!e2) {
#if DEBUG > 0
      warn << "nfs3_mkdir: Can't find parent handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }

    strcpy (msg1.cache_name, e2->cache_name);
    fhandle_t parent_fh;
    if (getfh (msg1.cache_name, &parent_fh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg1.cache_handle, &parent_fh, sizeof (parent_fh));

    assert (res->resok->dir_wcc.after.present);
    nfsobj2xfsnode (h->cred, e2->nh,
	            *(res->resok->dir_wcc.after.attributes), 0, &msg1.node);

    msg1.flag = 0;
    e2->incache = false; // sad mtime update problem with openbsd nfsd
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg1;
    h0_len = sizeof (msg1);

    // msg2.node.tokens = same as parent dir's
    msg2.parent_handle = h->parent_handle;
    strlcpy (msg2.name, h->name, sizeof (msg2.name));

    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *) &msg2;
    h1_len = sizeof (msg2);

    msg3.node = msg2.node;
    msg3.flag = 0;
    msg3.header.opcode = XFS_MSG_INSTALLDATA;

    h2 = (struct xfs_message_header *) &msg3;
    h2_len = sizeof (msg3);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, h2, h2_len,
				      NULL, 0);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_mkdir\n";
#endif
      reply_err (fd, h->header.sequence_num, ENOENT);
    }
    else {
#if DEBUG > 0
      warn << strerror (res->status) << ": nfs3_mkdir\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
    }
  }
}

int 
xfs_message_mkdir (int fd, ref<struct xfs_message_mkdir> h, u_int size)
{

#if DEBUG > 0
  warn << "mkdir !!\n";
#endif

  cache_entry *e = xfsindex[h->parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  mkdir3args ma;
  ma.where.dir = e->nh;
  ma.where.name = h->name;
  xfsattr2nfsattr (h->header.opcode, h->attr, &ma.attributes);

  ref<ex_diropres3 > res = New refcounted < ex_diropres3 >;
  nfsc->call (lbfs_NFSPROC3_MKDIR, &ma, res,
	      wrap (&nfs3_mkdir, fd, h, res, timenow));

  return 0;
}

void 
nfs3_link (int fd, ref<struct xfs_message_link> h, ref<ex_link3res > res,
	   time_t rqtime, clnt_stat err)
{

  if (!err && res->status == NFS3_OK) {
    struct xfs_message_installdata msg1;	//update parent dir's data
    struct xfs_message_installnode msg2;	//update attr of from_handle

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;

    //change attributes of parent dir
    //in the future implement local content change too..
    cache_entry *e1 = xfsindex[h->parent_handle];
    if (!e1) {
#if DEBUG > 0
      warn << "xfs_message_link: Can't find parent_handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }
    strcpy (msg1.cache_name, e1->cache_name);
    
    fhandle_t parent_fh;
    if (getfh (msg1.cache_name, &parent_fh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg1.cache_handle, &parent_fh, sizeof (parent_fh));
    
    assert (res->res->linkdir_wcc.after.present);
    nfsobj2xfsnode (h->cred, e1->nh,
		    *(res->res->linkdir_wcc.after.attributes), 0, &msg1.node);
    
    msg1.flag = 0;
    e1->incache = false; // sad mtime update problem with openbsd nfsd
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg1;
    h0_len = sizeof (msg1);
    
    cache_entry *e2 = xfsindex[h->from_handle];
    if (!e2) {
#if DEBUG > 0
      warn << "xfs_message_link: Can't find from_handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }

    assert (res->res->file_attributes.present);
    nfsobj2xfsnode (h->cred, e2->nh,
		    *(res->res->file_attributes.attributes), 
		    rqtime, &msg2.node);
    
    msg2.node.tokens = XFS_ATTR_R;
    msg2.parent_handle = h->parent_handle;
    strcpy (msg2.name, h->name);

    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *) &msg2;
    h1_len = sizeof (msg2);
    
    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, h1, h1_len, NULL, 0);

  } else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_link\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << strerror (res->status) << ": nfs3_link\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
    }
  }
}

int 
xfs_message_link (int fd, ref<struct xfs_message_link> h, u_int size)
{

#if DEBUG > 0
  warn << "(hard) link !!\n";
#endif

  cache_entry *e1 = xfsindex[h->from_handle];
  if (!e1) {
#if DEBUG > 0
    warn << "xfs_message_link: Can't find from_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }
  link3args la;
  la.file = e1->nh;

  cache_entry *e2 = xfsindex[h->parent_handle];
  if (!e2) {
#if DEBUG > 0
    warn << "xfs_message_link: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }
  la.link.dir = e2->nh;
  la.link.name = h->name;

  ref<ex_link3res > res = New refcounted < ex_link3res >;
  nfsc->call (lbfs_NFSPROC3_LINK, &la, res,
	      wrap (&nfs3_link, fd, h, res, timenow));

  return 0;
}

void 
nfs3_symlink (int fd, ref<struct xfs_message_symlink> h, cache_entry *e, 
	      ref<ex_diropres3 > res, time_t rqtime, clnt_stat err)
{

  if (!err && res->status == NFS3_OK) {
    struct xfs_message_installdata msg1;	//install change in parent dir
    struct xfs_message_installnode msg2;	//install symlink node

    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;

    assert (res->resok->obj.present && res->resok->obj_attributes.present);
    ex_fattr3 attr = *res->resok->obj_attributes.attributes;
    nfsobj2xfsnode (h->cred, *(res->resok->obj.handle),
		    attr, rqtime, &msg2.node);

    cache_entry *e = xfsindex[h->parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << "nfs3_symlink: Can't find parent handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }
    strcpy (msg1.cache_name, e->cache_name);

    assert (res->resok->dir_wcc.after.present);
    nfsobj2xfsnode (h->cred, e->nh,
	            *(res->resok->dir_wcc.after.attributes), 0, &msg1.node);

    msg1.flag = 0;
    e->incache = false; // sad mtime update problem with openbsd nfsd
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg1;
    h0_len = sizeof (msg1);

#if DEBUG > 0
    warn << "symlink !!(" 
         << h->parent_handle.a << ","
         << h->parent_handle.b << ","
         << h->parent_handle.c << ","
         << h->parent_handle.d << ")\n";
#endif
    msg2.node.tokens = XFS_ATTR_R;
    msg2.parent_handle = h->parent_handle;
    strlcpy (msg2.name, h->name, sizeof (msg2.name));

    msg2.header.opcode = XFS_MSG_INSTALLNODE;
    h1 = (struct xfs_message_header *) &msg2;
    h1_len = sizeof (msg2);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
				      0, h0, h0_len, h1, h1_len, NULL, 0);
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_symlink\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_symlink: " << strerror (res->status) << "\n";
#endif
      reply_err (fd, h->header.sequence_num, res->status);
    }
  }
}

int 
xfs_message_symlink (int fd, ref<struct xfs_message_symlink> h, u_int size)
{

#if DEBUG > 0
  warn << "symlimk !!\n";
#endif

  cache_entry *e = xfsindex[h->parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_symlink: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  symlink3args sla;
  sla.where.dir = e->nh;
  sla.where.name = h->name;
  xfsattr2nfsattr (h->header.opcode, h->attr, 
                   &(sla.symlink.symlink_attributes));
  sla.symlink.symlink_data.setbuf (h->contents, strlen (h->contents));

  ref<ex_diropres3 > res = New refcounted < ex_diropres3 >;
  nfsc->call (lbfs_NFSPROC3_SYMLINK, &sla, res,
	      wrap (&nfs3_symlink, fd, h, e, res, timenow));

  return 0;
}

void 
remove (int fd, ref<struct xfs_message_remove> h, ref<ex_lookup3res > lres,
	ref<ex_wccstat3 > wres, time_t rqtime, clnt_stat err)
{
  cache_entry *e1 = xfsindex[h->parent_handle];
  if (!e1) {
#if DEBUG > 0
    warn << "xfs_message_remove: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return;
  }
  if (!err && wres->status == NFS3_OK) {

    assert (wres->wcc->after.present);

    struct xfs_message_installdata msg1;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installattr msg2;
    struct xfs_message_header *h1 = NULL;
    size_t h1_len = 0;
    
    strcpy (msg1.cache_name, e1->cache_name);
    fhandle_t cfh;
    if (getfh (msg1.cache_name, &cfh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg1.cache_handle, &cfh, sizeof (cfh));
    ex_fattr3 attr = *(wres->wcc->after.attributes);
    nfsobj2xfsnode (h->cred, e1->nh, attr, 0, &msg1.node);

    e1->incache = false;
    msg1.node.tokens |= XFS_DATA_R;
    msg1.flag = XFS_ID_INVALID_DNLC;
    
    msg1.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg1;
    h0_len = sizeof (msg1);

    ex_post_op_attr a;
    if (lres->resok->obj_attributes.present)
      a = lres->resok->obj_attributes;
    else if (lres->resok->dir_attributes.present)
      a = lres->resok->dir_attributes;
    else {
#if DEBUG > 0
      warn << "lookup in remove: error no attr present\n";
#endif
      reply_err(fd, h->header.sequence_num, EIO);
      return;
    }

    if ((a.attributes->type == NF3DIR && a.attributes->nlink > 2) ||
	(a.attributes->type == NF3REG && a.attributes->nlink > 1)) {

      cache_entry *e2 = nfsindex[lres->resok->object];
      if (!e2) {
#if DEBUG > 0
	warn << "xfs_message_remove: Can't find handle\n";
#endif
	return;
      }

      msg2.header.opcode = XFS_MSG_INSTALLATTR;
      --(a.attributes->nlink);
      nfsobj2xfsnode (h->cred, e2->nh, *(a.attributes), rqtime, &msg1.node);
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					0, h0, h0_len, h1, h1_len,
					NULL, 0);
    }
    else {
      //TODO: evict from cache..or save?
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					0, h0, h0_len, NULL, 0);
    }

  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_lookup in remove\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << strerror (wres->status) << ": nfs3_lookup in remove\n";
#endif
      reply_err (fd, h->header.sequence_num, wres->status);
    }
  }
}

void 
nfs3_remove (int fd, ref<struct xfs_message_remove> h, 
             ref<ex_lookup3res > lres, clnt_stat err)
{

  if (!err && lres->status == NFS3_OK) {

    cache_entry *e = xfsindex[h->parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }

    diropargs3 doa;
    doa.dir = e->nh;
    doa.name = h->name;

    ref<ex_wccstat3 > wres = New refcounted < ex_wccstat3 >;
    nfsc->call (lbfs_NFSPROC3_REMOVE, &doa, wres,
		wrap (&remove, fd, h, lres, wres, timenow));
  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_remove\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_remove: lres->status = " << strerror (lres->status) << "\n";
#endif
      reply_err (fd, h->header.sequence_num, lres->status);
    }
  }
}

int 
xfs_message_remove (int fd, ref<struct xfs_message_remove> h, u_int size)
{

#if DEBUG > 0
  warn << "remove !!\n";
#endif

  cache_entry *e = xfsindex[h->parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  diropargs3 doa;
  doa.dir = e->nh;
  doa.name = h->name;
#if DEBUG > 0
  warn << "requesting file name " << doa.name.cstr () << "\n";
#endif
  ref<ex_lookup3res > res = New refcounted < ex_lookup3res >;

  nfsc->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
	      wrap (&nfs3_remove, fd, h, res));

  return 0;
}

void 
nfs3_rmdir (int fd, ref<struct xfs_message_rmdir> h, ref<ex_lookup3res > lres,
	    clnt_stat err)
{

  if (!err && lres->status == NFS3_OK) {

    cache_entry *e = xfsindex[h->parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << "xfs_message_mkdir: Can't find parent_handle\n";
#endif
      reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }
    
    diropargs3 doa;
    doa.dir = e->nh;
    doa.name = h->name;

    ref<struct xfs_message_remove> hr = New refcounted<struct xfs_message_remove>;
    hr->header = h->header;
    hr->parent_handle = h->parent_handle;
    hr->name = h->name;
    hr->cred = h->cred;

    ref<ex_wccstat3 > wres = New refcounted < ex_wccstat3 >;
    nfsc->call (lbfs_NFSPROC3_RMDIR, &doa, wres,
		wrap (&remove, fd, /*(struct xfs_message_remove *)*/ hr, lres, wres, timenow));

  }
  else {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_rmdir\n";
#endif
      reply_err (fd, h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_rmdir: lres->status = " << lres->status << "\n";
#endif
      reply_err (fd, h->header.sequence_num, lres->status);
    }
  }
}

int 
xfs_message_rmdir (int fd, ref<struct xfs_message_rmdir> h, u_int size)
{

#if DEBUG > 0
  warn << "rmdir !!\n";
#endif

  cache_entry *e = xfsindex[h->parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_mkdir: Can't find parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  diropargs3 doa;
  doa.dir = e->nh;
  doa.name = h->name;
#if DEBUG > 0
  warn << "requesting file name " << doa.name.cstr () << "\n";
#endif
  ref<ex_lookup3res > res = New refcounted < ex_lookup3res >;

  nfsc->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
	      wrap (&nfs3_rmdir, fd, h, res));

  return 0;
}

void 
update_attr (cache_entry *e, ex_fattr3 attr1, ex_fattr3 attr2, 
	     time_t rqtime1, time_t rqtime2)
{
  nfstime3 cache_time = e->ltime;
  attr2.expire += rqtime2;
  e->nfs_attr = attr2;

  if (!greater(attr2.mtime, attr1.mtime) && !greater(attr1.mtime, cache_time))
    e->ltime = attr2.mtime;
#if 0
  if (greater (attr1.mtime, cache_time) || greater (attr1.ctime, cache_time)) {
    attr2.expire += rqtime1;
    e->nfs_attr = attr2;
  }
  else if (greater (attr2.mtime, attr1.mtime)) {
    attr2.expire += rqtime2;
    e->nfs_attr = attr2;
  }
  else {
    e->ltime = max(attr2.mtime, attr2.ctime);
    attr2.expire += rqtime2;
    e->nfs_attr = attr2;
  }
#endif
}

void 
nfs3_rename_getattr (ref<rename_args> rena, time_t rqtime2, clnt_stat err)
{


  if (err || rena->gares->status != NFS3_OK) {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_rename_getattr\n";
#endif
      reply_err (rena->fd, rena->h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_rename_getattr: gares->status = " 
	   << strerror (rena->gares->status) << "\n";
#endif
      reply_err (rena->fd, rena->h->header.sequence_num, rena->gares->status);
    }
    return;
  }

  struct xfs_message_installnode msg1;	//update attr of file renamed 
  struct xfs_message_installdata msg2;	//new parent dir content
  struct xfs_message_installdata msg3;	//old parent dir content
  struct xfs_message_header *h1 = NULL;
  size_t h1_len = 0;
  struct xfs_message_header *h2 = NULL;
  size_t h2_len = 0;
  struct xfs_message_header *h3 = NULL;
  size_t h3_len = 0;
  nfs_fh3 file = rena->lres->resok->object;

  cache_entry *e1 = nfsindex[file];
  if (!e1) {
#if DEBUG > 0
    warn << "nfs3_rename_getattr: Can't find file handle\n";
#endif
    reply_err(rena->fd, rena->h->header.sequence_num, ENOENT);
    return;
  }

  if (rena->lres->resok->obj_attributes.present)
    update_attr (e1, *(rena->lres->resok->obj_attributes.attributes),
		 *(rena->gares->attributes), rena->rqtime1, rqtime2);
  else if (rena->lres->resok->dir_attributes.present)
    update_attr (e1, *(rena->lres->resok->dir_attributes.attributes),
		 *(rena->gares->attributes), rena->rqtime1, rqtime2);

  nfsobj2xfsnode (rena->h->cred, file, e1->nfs_attr, 0, &msg1.node);

  msg1.parent_handle = rena->h->new_parent_handle;
  strlcpy (msg1.name, rena->h->new_name, sizeof (msg1.name));

  msg1.header.opcode = XFS_MSG_INSTALLNODE;
  h1 = (struct xfs_message_header *) &msg1;
  h1_len = sizeof (msg1);

  cache_entry *e2 = xfsindex[rena->h->new_parent_handle];
  if (!e2) {
#if DEBUG > 0
    warn << "nfs3_rename_getattr: Can't find file new_parent_handle(" 
	 << rena->h->new_parent_handle.a << ","
	 << rena->h->new_parent_handle.b << ","
	 << rena->h->new_parent_handle.c << ","
	 << rena->h->new_parent_handle.d << ")\n";
#endif
    reply_err(rena->fd, rena->h->header.sequence_num, ENOENT);
    return;
  }
  strcpy (msg2.cache_name, e2->cache_name);
  fhandle_t parent_fh;
  if (getfh (msg2.cache_name, &parent_fh)) {
#if DEBUG > 0
    warn << "getfh failed\n";
#endif
    reply_err(rena->fd, rena->h->header.sequence_num, EIO);
    return;
  }
  memmove (&msg2.cache_handle, &parent_fh, sizeof (parent_fh));
  assert (rena->rnres->res->todir_wcc.after.present);
  nfsobj2xfsnode (rena->h->cred, e2->nh,
		  *(rena->rnres->res->todir_wcc.after.attributes),
		  0, &msg2.node);

  e2->incache = false; // sad mtime update problem with openbsd nfsd
  msg2.flag = XFS_ID_INVALID_DNLC;
  msg2.header.opcode = XFS_MSG_INSTALLDATA;
  h2 = (struct xfs_message_header *) &msg2;
  h2_len = sizeof (msg2);

  if (!xfs_handle_eq (&rena->h->old_parent_handle,
	              &rena->h->new_parent_handle)) {
    cache_entry *e3 = xfsindex[rena->h->old_parent_handle];
    if (!e3) {
#if DEBUG > 0
      warn << "nfs3_rename_getattr: Can't find file old_parent_handle\n";
#endif
      reply_err(rena->fd, rena->h->header.sequence_num, ENOENT);
      return;
    }
    strcpy (msg3.cache_name, e3->cache_name);
    //change content of new parent dir (later)
    if (getfh (msg3.cache_name, &parent_fh)) {
#if DEBUG > 0
      warn << "getfh failed\n";
#endif
      reply_err(rena->fd, rena->h->header.sequence_num, EIO);
      return;
    }
    memmove (&msg3.cache_handle, &parent_fh, sizeof (parent_fh));
    assert (rena->rnres->res->fromdir_wcc.after.present);
    nfsobj2xfsnode (rena->h->cred, e3->nh,
		    *(rena->rnres->res->fromdir_wcc.after.attributes),
		    0, &msg3.node);

    e3->incache = false; // sad mtime update problem with openbsd nfsd
    msg3.flag = XFS_ID_INVALID_DNLC;
    msg3.header.opcode = XFS_MSG_INSTALLDATA;
    h3 = (struct xfs_message_header *) &msg3;
    h3_len = sizeof (msg3);
  }
  
  xfs_send_message_wakeup_multiple (rena->fd, rena->h->header.sequence_num,
				    0, h1, h1_len, h2, h2_len,
                                    h3, h3_len, NULL, 0);
}

void 
nfs3_rename_rename (ref<rename_args> rena, clnt_stat err)
{

#if DEBUG > 0
  warn << "rename_rename !!\n";
#endif
  if (!err) {
    nfs_fh3 fh = rena->lres->resok->object;
    nfsc->call (lbfs_NFSPROC3_GETATTR, &fh, rena->gares,
		wrap (&nfs3_rename_getattr, rena, timenow));
  } else {
#if DEBUG > 0
    if (err)
      warn << err << ": nfs3_rename_rename\n";
#endif
  }
}

void 
nfs3_rename_lookup (ref<rename_args> rena, time_t rqtime, clnt_stat err)
{

#if DEBUG > 0
  warn << "rename_lookup !!\n";
#endif

  if (err || rena->lres->status != NFS3_OK) {
    if (err) {
#if DEBUG > 0
      warn << err << ": nfs3_rename_lookup\n";
#endif
      reply_err (rena->fd, rena->h->header.sequence_num, EIO);
    }
    else {
#if DEBUG > 0
      warn << "nfs3_rename_lookup: lres->status = " 
	   << strerror (rena->lres->status) << "\n";
#endif
      reply_err (rena->fd, rena->h->header.sequence_num, rena->lres->status);
    }
    return;
  }

  cache_entry *e1 = xfsindex[rena->h->old_parent_handle];
  if (!e1) {
#if DEBUG > 0
    warn << "xfs_message_rename: Can't find old_parent_handle\n";
#endif
    reply_err(rena->fd, rena->h->header.sequence_num, ENOENT);
    return;
  }

  rename3args rna;
  rna.from.dir = e1->nh;
  rna.from.name = rena->h->old_name;

  cache_entry *e2 = xfsindex[rena->h->new_parent_handle];
  if (!e2) {
#if DEBUG > 0
    warn << "xfs_message_rename: Can't find new_parent_handle\n";
#endif
    reply_err(rena->fd, rena->h->header.sequence_num, ENOENT);
    return;
  }

  rna.to.dir = e2->nh;
  rna.to.name = rena->h->new_name;

  rena->rqtime1 = rqtime;
  //rena->rnres = New refcounted<ex_rename3res>;

  nfsc->call (lbfs_NFSPROC3_RENAME, &rna, rena->rnres,
	      wrap (&nfs3_rename_rename, rena));
}

int 
xfs_message_rename (int fd, ref<struct xfs_message_rename> h, u_int size)
{

#if DEBUG > 0
  warn << "rename !!\n";
#endif

  cache_entry *e = xfsindex[h->old_parent_handle];
  if (!e) {
#if DEBUG > 0
    warn << "xfs_message_rename: Can't find old_parent_handle\n";
#endif
    reply_err(fd, h->header.sequence_num, ENOENT);
    return -1;
  }

  diropargs3 doa;
  doa.dir = e->nh;
  doa.name = h->old_name;

  ref<rename_args> rena = New refcounted<rename_args> (fd, h);
  //rena->lres = New ex_lookup3res;
  nfsc->call (lbfs_NFSPROC3_LOOKUP, &doa, rena->lres,
	      wrap (&nfs3_rename_lookup, rena, timenow));
  return 0;
}

int
xfs_message_pioctl (int fd, ref<struct xfs_message_pioctl> h, u_int size) {
  
#if DEBUG > 0
  warn << "pioctl!! return 0 no matter what!!!\n";
#endif

#if DEBUG > 0
  warn << "pioctl: opcode = " << h->opcode << "\n";
#endif
  
  int error = 0;

  switch(h->opcode) {
#ifdef KERBEROS
#ifdef VIOCSETTOK_32
  case VIOCSETTOK_32:
  case VIOCSETTOK_64:
#else
  case VIOCSETTOK:
#endif
    error = viocsettok (fd, h, size);
    break;
#ifdef VIOCGETTOK_32
  case VIOCGETTOK_32:
  case VIOCGETTOK_64:
#else
  case VIOCGETTOK :
#endif
    return viocgettok (fd, h, size);
#ifdef VIOCUNPAG_32
  case VIOCUNPAG_32:
  case VIOCUNPAG_64:
#else
  case VIOCUNPAG:
#endif
#ifdef VIOCUNLOG_32
  case VIOCUNLOG_32:
  case VIOCUNLOG_64:
#else
  case VIOCUNLOG:
#endif
    error = viocunlog (fd, h, size);
    break;
#endif /* KERBEROS */
#ifdef VIOCCONNECTMODE_32
  case VIOCCONNECTMODE_32:
  case VIOCCONNECTMODE_64:
#else
  case VIOCCONNECTMODE:
#endif
    return viocconnect(fd, h, size);
#ifdef VIOCFLUSH_32
  case VIOCFLUSH_32:
  case VIOCFLUSH_64:
#else
  case VIOCFLUSH:
#endif
    error = viocflush(fd, h, size);
    break;
#ifdef VIOC_FLUSHVOLUME_32
  case VIOC_FLUSHVOLUME_32:
  case VIOC_FLUSHVOLUME_64:
#else
  case VIOC_FLUSHVOLUME:
#endif
    error = viocflushvolume(fd, h, size);
    break;
#ifdef VIOCGETFID_32
  case VIOCGETFID_32:
  case VIOCGETFID_64:
#else
  case VIOCGETFID:
#endif
    return viocgetfid (fd, h, size);
#ifdef VIOCGETAL_32
  case VIOCGETAL_32:
  case VIOCGETAL_64:
#else
  case VIOCGETAL:
#endif
    return viocgetacl(fd, h, size);
#ifdef VIOCSETAL_32
  case VIOCSETAL_32:
  case VIOCSETAL_64:
#else
  case VIOCSETAL:
#endif
    return viocsetacl(fd, h, size);
#ifdef VIOCGETVOLSTAT_32
  case VIOCGETVOLSTAT_32:
  case VIOCGETVOLSTAT_64:
#else
  case VIOCGETVOLSTAT:
#endif
    return viocgetvolstat(fd, h, size);
#ifdef VIOCSETVOLSTAT_32
  case VIOCSETVOLSTAT_32:
  case VIOCSETVOLSTAT_64:
#else
  case VIOCSETVOLSTAT:
#endif
    error = viocsetvolstat(fd, h, size);
    break;
#ifdef VIOC_AFS_STAT_MT_PT_32
  case VIOC_AFS_STAT_MT_PT_32:
  case VIOC_AFS_STAT_MT_PT_64:
#else
  case VIOC_AFS_STAT_MT_PT:
#endif
    return vioc_afs_stat_mt_pt(fd, h, size);
#ifdef VIOC_AFS_DELETE_MT_PT_32
  case VIOC_AFS_DELETE_MT_PT_32:
  case VIOC_AFS_DELETE_MT_PT_64:
#else
  case VIOC_AFS_DELETE_MT_PT:
#endif
    return vioc_afs_delete_mt_pt(fd, h, size);
#ifdef VIOCWHEREIS_32
  case VIOCWHEREIS_32:
  case VIOCWHEREIS_64:
#else
  case VIOCWHEREIS:
#endif
    return viocwhereis(fd, h, size);
#ifdef VIOCNOP_32
  case VIOCNOP_32:
  case VIOCNOP_64:
#else
  case VIOCNOP:
#endif
    error = EINVAL;
    break;
#ifdef VIOCGETCELL_32
  case VIOCGETCELL_32:
  case VIOCGETCELL_64:
#else
  case VIOCGETCELL:
#endif
    return vioc_get_cell(fd, h, size);
#ifdef VIOC_GETCELLSTATUS_32
  case VIOC_GETCELLSTATUS_32:
  case VIOC_GETCELLSTATUS_64:
#else
  case VIOC_GETCELLSTATUS:
#endif
    return vioc_get_cellstatus(fd, h, size);
#ifdef VIOC_SETCELLSTATUS_32
  case VIOC_SETCELLSTATUS_32:
  case VIOC_SETCELLSTATUS_64:
#else
  case VIOC_SETCELLSTATUS:
#endif
    return vioc_set_cellstatus(fd, h, size);
#ifdef VIOCNEWCELL_32
  case VIOCNEWCELL_32:
  case VIOCNEWCELL_64:
#else
  case VIOCNEWCELL:
#endif
    return vioc_new_cell(fd, h, size);
#ifdef VIOC_VENUSLOG_32
  case VIOC_VENUSLOG_32:
  case VIOC_VENUSLOG_64:
#else
    case VIOC_VENUSLOG:
#endif
	error = viocvenuslog (fd, h, size);
	break;
#ifdef VIOC_AFS_SYSNAME_32
    case VIOC_AFS_SYSNAME_32:
    case VIOC_AFS_SYSNAME_64:
#else
    case VIOC_AFS_SYSNAME:
#endif
	return vioc_afs_sysname (fd, h, size);
#ifdef VIOC_FILE_CELL_NAME_32
    case VIOC_FILE_CELL_NAME_32:
    case VIOC_FILE_CELL_NAME_64:
#else
    case VIOC_FILE_CELL_NAME:
#endif
	return viocfilecellname (fd, h, size);
#ifdef VIOC_GET_WS_CELL_32
    case VIOC_GET_WS_CELL_32:
    case VIOC_GET_WS_CELL_64:
#else
    case VIOC_GET_WS_CELL:
#endif
	return viocgetwscell (fd, h, size);
#ifdef VIOCSETCACHESIZE_32
    case VIOCSETCACHESIZE_32:
    case VIOCSETCACHESIZE_64:
#else
    case VIOCSETCACHESIZE:
#endif
	error = viocsetcachesize (fd, h, size);
	break;
#ifdef VIOCCKSERV_32
    case VIOCCKSERV_32:
    case VIOCCKSERV_64:
#else
    case VIOCCKSERV:
#endif
	return viocckserv (fd, h, size);
#ifdef VIOCGETCACHEPARAMS_32
    case VIOCGETCACHEPARAMS_32:
    case VIOCGETCACHEPARAMS_64:
#else
    case VIOCGETCACHEPARAMS:
#endif
	return viocgetcacheparms (fd, h, size);
#ifdef VIOC_GETRXKCRYPT_32
    case VIOC_GETRXKCRYPT_32:
    case VIOC_GETRXKCRYPT_64:
#else
    case VIOC_GETRXKCRYPT:
#endif
	return getrxkcrypt(fd, h, size);
#ifdef VIOC_SETRXKCRYPT_32
    case VIOC_SETRXKCRYPT_32:
    case VIOC_SETRXKCRYPT_64:
#else
    case VIOC_SETRXKCRYPT:
#endif
	error = setrxkcrypt(fd, h, size);
	break;
#ifdef VIOC_FPRIOSTATUS_32
    case VIOC_FPRIOSTATUS_32:
    case VIOC_FPRIOSTATUS_64:
#else
    case VIOC_FPRIOSTATUS:
#endif
	error = vioc_fpriostatus(fd, h, size);
	break;
#ifdef VIOC_AVIATOR_32
    case VIOC_AVIATOR_32:
    case VIOC_AVIATOR_64:
#else
    case VIOC_AVIATOR:
#endif
	return viocaviator (fd, h, size);
#ifdef VIOC_ARLADEBUG_32
    case VIOC_ARLADEBUG_32:
    case VIOC_ARLADEBUG_64:
#else
    case VIOC_ARLADEBUG:
#endif
	return vioc_arladebug (fd, h, size);
#ifdef VIOC_GCPAGS_32
    case VIOC_GCPAGS_32:
    case VIOC_GCPAGS_64:
#else
    case VIOC_GCPAGS:
#endif
	error = vioc_gcpags (fd, h, size);
	break;
#ifdef VIOC_CALCULATE_CACHE_32
    case VIOC_CALCULATE_CACHE_32:
    case VIOC_CALCULATE_CACHE_64:
#else
    case VIOC_CALCULATE_CACHE:
#endif
	return vioc_calculate_cache (fd, h, size);
#ifdef VIOC_BREAKCALLBACK_32
    case VIOC_BREAKCALLBACK_32:
    case VIOC_BREAKCALLBACK_64:
#else
    case VIOC_BREAKCALLBACK:
#endif	
	error = vioc_breakcallback (fd, h, size);
	break;
    default:
	warn << "unknown pioctl call \n";
      error = EINVAL ;
  }

  xfs_send_message_wakeup (fd, h->header.sequence_num, error);
    
  return 0;

}

void 
cbdispatch (svccb * sbp)
{
#if DEBUG > 0
  warn << "cbdispatch called\n";
#endif
  if (!sbp)
    return;

  switch (sbp->proc ()) {
  case ex_NFSCBPROC3_NULL:
    sbp->reply (NULL);
    break;
  case ex_NFSCBPROC3_INVALIDATE:
    {
      ex_invalidate3args *xa = sbp->template getarg < ex_invalidate3args > ();
      ex_fattr3 *a = NULL;
      if (xa->attributes.present && xa->attributes.attributes->expire) {
	a = xa->attributes.attributes.addr ();
	a->expire += timenow;
	cache_entry *e = nfsindex[xa->handle];
	if (!e) {
	  warn << "cbdispatch: Can't find handle\n";
	  return;
	}
	e->nfs_attr = *a;
      }
      //delete a; should we delete this?
      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
