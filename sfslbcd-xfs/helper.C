#include "sfslbcd.h"
#include "sfsclient.h"
#include "xfs.h"
#include "xfs-sfs.h"
#include "xfs-nfs.h"
#include "cache.h"
#include "../sfslbsd/sfsrwsd.h"

AUTH *auth_root = authunix_create ("localhost", 0, 0, 0, NULL);
AUTH *auth_default = 
  authunix_create ("localhost", (uid_t) 14228, (gid_t) 100, 0, NULL);

void 
installattr (int fd, uint seqnum, xfs_cred cred, nfs_fh3 fh, ex_fattr3 attr, 
	     time_t rqt, clnt_stat err) 
{
  if (!err) {
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installattr msg;

    msg.header.opcode = XFS_MSG_INSTALLATTR;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    bool update_dir_expire = false;
    cache_entry *e = update_cache (fh, attr);
    if (e->nfs_attr.type == NF3DIR) {
      nfstime3 maxtime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
      if (!greater(maxtime, e->ltime))
	update_dir_expire = true;
    }
    e->set_exp (rqt, update_dir_expire);
    nfsobj2xfsnode (cred, e, &msg.node);

    xfs_send_message_wakeup_multiple (fd, seqnum,
				      0, h0, h0_len, NULL, 0);
  } else 
    xfs_reply_err (fd, seqnum, err);
}

struct getattr_obj {
  
  typedef callback<void, ptr<ex_getattr3res>, time_t, clnt_stat>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;

  const struct xfs_message_getattr h;
  sfs_aid sa;
  const nfs_fh3 fh;
  ptr<ex_getattr3res> res;
  
  void installattr (ptr<ex_getattr3res> res, time_t rqt, clnt_stat err)
  {
#if 0
    if (!err) {
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_installattr msg;

      msg.header.opcode = XFS_MSG_INSTALLATTR;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      bool update_dir_expire = false;
      cache_entry *e = update_cache (fh, attr);
      if (e->nfs_attr.type == NF3DIR) {
	nfstime3 maxtime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
	if (!greater(maxtime, e->ltime))
	  update_dir_expire = true;
      }
      e->set_exp (rqt, update_dir_expire);
      nfsobj2xfsnode (cred, e, &msg.node);
      
      xfs_send_message_wakeup_multiple (fd, seqnum,
					0, h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, seqnum, err);
#endif
  }
  
  void gotattr (time_t rqt, clnt_stat err) 
  {
    if (!cb)
      installattr (res, rqt, err);
    else
      if (!err)
	(*cb) (res, rqt, err);
      else
	(*cb) (NULL, 0, err);
    delete this;
  }

  void getattr () 
  {
    res = New refcounted<ex_getattr3res>; 
    c->call (lbfs_NFSPROC3_GETATTR, &fh, res,
	     wrap (this, &getattr_obj::gotattr, timenow), 
	     lbfs_authof (sa));
  }
  
  getattr_obj (int fd1, const xfs_message_getattr &h1, sfs_aid sa1,
	       const nfs_fh3 &fh1, ref<aclnt> c1, cb_t cb1) : 
    cb(cb1), fd(fd1), c(c1), h(h1), sa(sa1), fh(fh1) 
  {
    getattr ();
  }
  
};

void 
l_getattr (int fd, const xfs_message_getattr &h, sfs_aid sa, const nfs_fh3 &fh, 
	   ref<aclnt> c, getattr_obj::cb_t cb)
{
  vNew getattr_obj (fd, h, sa, fh, c, cb);
}

void 
lbfs_getattr (int fd, const xfs_message_getattr &h, sfs_aid sa, const nfs_fh3 &fh, 
	      ref<aclnt> c)
{
  //  getattr_obj::cb_t cb;
  //  vNew getattr_obj (fd, h, sa, fh, c, cb);
}

struct getroot_obj {
  int fd; 
  ref<aclnt> sc;
  ref<aclnt> nc;
  
  const struct xfs_message_getroot h;
  sfs_aid sa;
  bool gotnfs_fsi;
  bool gotroot_attr;
  time_t rqtime;
  ptr<sfs_fsinfo> sfs_fsi;
  ptr<ex_fsinfo3res> nfs_fsi;
  ptr<ex_getattr3res> root_attr;

  void installroot () 
  {
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installroot msg;
    msg.header.opcode = XFS_MSG_INSTALLROOT;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    cache_entry *e = update_cache (sfs_fsi->nfs->v3->root, 
				   *root_attr->attributes);
    e->set_exp (rqtime);
    nfsobj2xfsnode (h.cred, e, &msg.node);

    xfs_send_message_wakeup_multiple (fd, h.header.sequence_num,	
     				      0, h0, h0_len, NULL, 0);
    delete this;
  }

  void gotnfs_fsinfo (clnt_stat err) 
  {    
    assert (!err && nfs_fsi->status == NFS3_OK);
    nfs_fsinfo = *nfs_fsi->resok;
    gotnfs_fsi = true;
    if (gotroot_attr)
      installroot ();
  }

  void gotattr (ptr<ex_getattr3res> attr, time_t rqt, clnt_stat err) 
  {
    assert (!err && attr->status == NFS3_OK);
    root_attr = attr;
    rqtime = rqt;
    gotroot_attr = true;
    if (gotnfs_fsi) 
      installroot ();
  }

  void getnfs_fsinfo (clnt_stat err) 
  {
    assert (!err && sfs_fsi->prog == ex_NFS_PROGRAM && 
	    sfs_fsi->nfs->vers == ex_NFS_V3);

    x->compress();
    nfs_fsi = New refcounted<ex_fsinfo3res>;
    nc->call (lbfs_NFSPROC3_FSINFO, &sfs_fsi->nfs->v3->root, nfs_fsi,
	      wrap (this, &getroot_obj::gotnfs_fsinfo), lbfs_authof (sa));
    const struct xfs_message_getattr *h1 = (xfs_message_getattr *) &h;
    l_getattr (fd, *h1, sa, sfs_fsi->nfs->v3->root, 
		  nc, wrap (this, &getroot_obj::gotattr));
  }

  void getsfs_fsinfo () 
  {
    sfs_fsi = New refcounted<sfs_fsinfo>;
    sfsc->call (SFSPROC_GETFSINFO, NULL, sfs_fsi,
		wrap (this, &getroot_obj::getnfs_fsinfo), lbfs_authof (sa));
  }

  getroot_obj (int fd1, xfs_message_getroot &h1, sfs_aid sa1,  
	       ref<aclnt> sc1, ref<aclnt> nc1) : 
    fd(fd1), sc(sc1), nc(nc1), h(h1), sa(sa1), gotnfs_fsi(false), 
    gotroot_attr(false) 
  {
    getsfs_fsinfo ();
  }
};

void 
lbfs_getroot (int fd1, xfs_message_getroot &h1, sfs_aid sa1, 
	      ref<aclnt> sc1, ref<aclnt> nc1) 
{
  vNew getroot_obj (fd1, h1, sa1, sc1, nc1);
}

struct lookup_obj {
  
  typedef callback<void, ptr<ex_lookup3res>, time_t, clnt_stat>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;

  const struct xfs_message_getnode h;
  sfs_aid sa;
  const nfs_fh3 parent_fh;
  const char *name;
  ptr<ex_lookup3res> res;

  void found (time_t rqt, clnt_stat err) 
  {
    if (!err) 
      (*cb) (res, rqt, err);
    else 
      (*cb) (NULL, 0, err);
    delete this;
  }

  void lookup () 
  {
    diropargs3 doa;
    doa.dir = parent_fh;
    doa.name = name;
#if DEBUG > 0
    warn << h.header.sequence_num 
	 << ":" << "requesting file name " << doa.name << "\n";
#endif
    res = New refcounted<ex_lookup3res>;

    c->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
	        wrap (this, &lookup_obj::found, timenow), lbfs_authof (sa));
  }

  lookup_obj (int fd1, const struct xfs_message_getnode &h1, sfs_aid sa1, 
	      const nfs_fh3 &parent_fh1, const char *name1, 
	      ref<aclnt> c1, cb_t cb1) : cb(cb1), fd(fd1), c(c1), 
		h(h1), sa(sa1), parent_fh(parent_fh1), name(name1)
  {
    lookup ();
  }
  
};

void lbfs_lookup (int fd, const struct xfs_message_getnode &h, sfs_aid sa,
		  const nfs_fh3 &parent_fh, const char *name, 
		  ref<aclnt> c, lookup_obj::cb_t cb) 
{
  vNew lookup_obj (fd, h, sa, parent_fh, name, c, cb);
}

struct getnode_obj {
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_getnode h;
  sfs_aid sa;
  
  void installnode (ptr<ex_lookup3res> lres, time_t rqt, clnt_stat err) 
  {
    if (!err && lres->status == NFS3_OK) {
      ex_fattr3 a;

      if (lres->resok->obj_attributes.present)
	a = *lres->resok->obj_attributes.attributes;
      else if (lres->resok->dir_attributes.present)
	a = *lres->resok->dir_attributes.attributes;
      else {
#if DEBUG > 0
	warn << h.header.sequence_num << ":getnode: error no attr present\n";
#endif
	xfs_reply_err(fd, h.header.sequence_num, ENOSYS); //Why ENOSYS??
	delete this;
	return;
      }

      struct xfs_message_installnode msg;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      cache_entry *e = update_cache (lres->resok->object, a);

      e->set_exp (rqt);
      nfsobj2xfsnode (h.cred, e, &msg.node);
      msg.header.opcode = XFS_MSG_INSTALLNODE;
      msg.parent_handle = h.parent_handle;
      strcpy (msg.name, h.name);
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num, 0,
					h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h.header.sequence_num, err ? EIO : lres->status);
    
    delete this;
  }

  getnode_obj (int fd1, xfs_message_getnode &h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1), sa(sa1)
  {  
    cache_entry *e = xfsindex[h.parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << h.header.sequence_num << ":" 
	   << "xfs_getnode: Can't find parent_handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    } else 
      lbfs_lookup (fd, h, sa, e->nh, h.name, c, 
		   wrap (this, &getnode_obj::installnode));
  }
};

void 
lbfs_getnode (int fd, xfs_message_getnode &h, sfs_aid sa, ref<aclnt> c)
{  
  vNew getnode_obj (fd, h, sa, c);
}

struct readlink_obj {

  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_open h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_readlink3res> rlres;
  
  void install_link (time_t rqt, clnt_stat err) 
  {
    if (!err && rlres->status == NFS3_OK) {
      
      struct xfs_message_installdata msg;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;

      e->nfs_attr = *rlres->resok->symlink_attributes.attributes;
      e->set_exp (rqt);
      e->ltime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
      nfsobj2xfsnode (h.cred, e, &msg.node);
      msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;

      int lfd = assign_cachefile (fd, h.header.sequence_num, e, 
				  msg.cache_name, &msg.cache_handle);

      write (lfd, rlres->resok->data.cstr (), rlres->resok->data.len ());
      close (lfd);
      e->incache = true;

      msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num, 0,
					h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h.header.sequence_num, err ? EIO : rlres->status);      

    (*cb) ();
    delete this;    
  }

  readlink_obj (int fd1, const xfs_message_open &h1, cache_entry *e1, sfs_aid sa1, 
		ref<aclnt> c1, readlink_obj::cb_t cb1) : 
    cb(cb1), fd(fd1), c(c1), h(h1), sa(sa1), e(e1)
  {
    rlres = New refcounted <ex_readlink3res>;
    c->call (lbfs_NFSPROC3_READLINK, &e->nh, rlres,
	     wrap (this, &readlink_obj::install_link, timenow), lbfs_authof (sa));
  }
};

void 
lbfs_readlink (int fd, const xfs_message_open &h, cache_entry *e, sfs_aid sa, 
	       ref<aclnt> c, readlink_obj::cb_t cb) 
{
  vNew readlink_obj (fd, h, e, sa, c, cb);
}

void 
lbfs_readexist (int fd, const xfs_message_getdata &h, cache_entry *e) 
{
  struct xfs_message_installdata msg;

  nfsobj2xfsnode (h.cred, e, &msg.node);
  msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R; // | XFS_OPEN_NW | XFS_DATA_W;
  assign_cachefile (fd, h.header.sequence_num, e, 
		    msg.cache_name, &msg.cache_handle);

  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  e->incache = true;
  msg.header.opcode = XFS_MSG_INSTALLDATA;
  h0 = (struct xfs_message_header *) &msg;
  h0_len = sizeof (msg);

  xfs_send_message_wakeup_multiple (fd, h.header.sequence_num, 0,
				    h0, h0_len, NULL, 0);  
}

struct readdir_obj {

  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_open h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_readdir3res> rdres;
  
  void write_dirfile (write_dirent_args args, xfs_message_installdata msg,
		      clnt_stat err)
  {
    if (nfsdir2xfsfile (rdres, &args) < 0) {
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      return;
    }

    if (args.last)
      flushbuf (&args);
    free (args.buf);

    if (!rdres->resok->reply.eof) {
      readdir3args rda;
      rda.dir = e->nh; 
      entry3 *ent = rdres->resok->reply.entries;
      while (ent->nextentry != NULL)
	ent = ent->nextentry;
      rda.cookie = ent->cookie;
      rda.cookieverf = rdres->resok->cookieverf;
      rda.count = nfs_fsinfo.dtpref;

      rdres = New refcounted <ex_readdir3res>;
      c->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
	       wrap (this, &readdir_obj::write_dirfile, args, msg),
	       lbfs_authof(sa));
    } else {
      close (args.fd);
      e->incache = true;

      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);
      
      warn << "write_dirfile: seq_num = " << h.header.sequence_num << "\n";
      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num, 0,
					h0, h0_len, NULL, 0);
      (*cb) ();
      delete this;
    }      
  }

  void installdir (time_t rqt, clnt_stat err) 
  {
    if (!err && rdres->status == NFS3_OK) {

      struct xfs_message_installdata msg;
      struct write_dirent_args args;

      e->nfs_attr = *rdres->resok->dir_attributes.attributes;
      e->set_exp (rqt, true);
      e->ltime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
      nfsobj2xfsnode (h.cred, e, &msg.node);
      msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;
      
      warn << "installdir: seq_num = " << h.header.sequence_num << "\n";
      args.fd = assign_cachefile (fd, h.header.sequence_num, e, 
				  msg.cache_name, &msg.cache_handle);
      write_dirfile (args, msg, clnt_stat (0));
    } else {
      xfs_reply_err (fd, h.header.sequence_num, err ? err : rdres->status);      
      delete this;
    }
  }
  
  void readdir () {
    readdir3args rda;
    rda.dir = e->nh;
    rda.cookie = 0;
    rda.cookieverf = cookieverf3 ();
    rda.count = nfs_fsinfo.dtpref;

    rdres = New refcounted <ex_readdir3res>;
    c->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
	     wrap (this, &readdir_obj::installdir, timenow), 
	     lbfs_authof (sa));
  }

  void get_updated_copy (ptr<ex_getattr3res> res, time_t rqt, clnt_stat err) {
    if (res) {
      if (!err && res->status == NFS3_OK) {
	e->nfs_attr = *(res->attributes);
	e->set_exp (rqt, true);
      } else {
	xfs_reply_err (fd, h.header.sequence_num, err ? err : res->status);
	delete this;
      }
    } 
    nfstime3 maxtime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
    if (greater (maxtime, e->ltime)) 
      readdir ();
    else {
      xfs_message_getdata *h1 = (xfs_message_getdata *) &h;
      lbfs_readexist (fd, *h1, e);
      delete this;
    }
  }

  readdir_obj (int fd1, const xfs_message_open &h1, cache_entry *e1, sfs_aid sa1, 
     	       ref<aclnt> c1, readdir_obj::cb_t cb1) :
    cb(cb1), fd(fd1), c(c1), h(h1), sa(sa1), e(e1)    
  {
#if DEBUG > 0
    warn << h.header.sequence_num << ":"
	 << "xfs_message_open on directory: " << e->writers << "\n";
#endif
    uint32 owriters = e->writers;
    if ((h.tokens & XFS_OPEN_MASK) & (XFS_OPEN_NW|XFS_OPEN_EW)) {
      e->writers = 1;
#if DEBUG > 0
      warn << h.header.sequence_num << ":"  
	   << "open for write: " << e->writers << " writers\n";
#endif      
    }
    if (!e->incache) {
      readdir ();
    } else {
#if DEBUG > 0
      warn << "directory in cache, writers " << e->writers << "\n";
#endif
      if (owriters > 0) {
	xfs_message_getdata *h1 = (xfs_message_getdata *) &h;
	lbfs_readexist (fd, *h1, e);
	delete this;
      } else {
#if DEBUG > 0
	warn << "directory in cache, exp " << e->nfs_attr.expire << " " 
	     << (uint32) timenow << " ltime " << e->ltime.seconds << "\n";
#endif
	if (e->nfs_attr.expire < (uint32) timenow) {
	  const struct xfs_message_getattr *h1 = (xfs_message_getattr *) &h;
	  l_getattr (fd, *h1, sa, e->nh, c, 
			wrap (this, &readdir_obj::get_updated_copy));
	} else get_updated_copy (NULL, 0, clnt_stat (0));
      }
    }   
  }
};

void 
lbfs_readdir (int fd, const xfs_message_open &h, cache_entry *e, sfs_aid sa, 
	      ref<aclnt> c, readdir_obj::cb_t cb)
{
  vNew readdir_obj (fd, h, e, sa, c, cb);
}

struct getfp_obj {
  typedef callback<void>::ref cb_t;
  cb_t cb;  
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_open h;
  sfs_aid sa;
  cache_entry *e;
  lbfs_getfp3args gfa;
  uint64 offset; 
  int out_fd;
  str out_fname;
  uint blocks_written;
  uint total_blocks;
  bool eof;
  int retries;
  ptr<lbfs_getfp3res> fpres;

  void installdata () 
  {
    struct xfs_message_installdata msg;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;

    e->cache_name = out_fname;
    e->incache = true;

    assign_cachefile (fd, h.header.sequence_num, e, 
		      msg.cache_name, &msg.cache_handle);

    nfsobj2xfsnode (h.cred, e, &msg.node);
    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R; // | XFS_OPEN_NW | XFS_DATA_W;
    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    xfs_send_message_wakeup_multiple (fd, h.header.sequence_num, 0,
				      h0, h0_len, NULL, 0);
  }

  void write_file (uint64 cur_offst, uint32 size, ref<ex_read3res> rres)
  {
#if DEBUG > 0
    warn << "filename = " << out_fname << " offset = " << cur_offst << "\n";
#endif

    int err;
    if ((err = lseek (out_fd, cur_offst, SEEK_SET)) < 0) {
#if DEBUG > 0
      warn << "getfp_obj::write_file1: " << out_fname << " " << strerror (errno) << "\n";
#endif
      delete this;
    }
    if ((err = write (out_fd, rres->resok->data.base (), 
		      rres->resok->data.size ())) < 0) {
#if DEBUG > 0
      warn << "getfp_obj::write_file2: " << out_fname << " " << strerror (errno) << "\n";
#endif
      delete this;
    }
    else if (err != (int) rres->resok->data.size ()) {
#if DEBUG > 0
      warn << "write error or short write!!\n";
#endif
      delete this;
    }

    if (rres->resok->count < size)
      nfs3_read (cur_offst + rres->resok->count, size - rres->resok->count);
    else
      blocks_written++;
  }
  
  void gotdata (uint64 cur_offst, uint32 size, ref<ex_read3res> rres, clnt_stat err) 
  {
    if (!err && rres->status == NFS3_OK) {
      assert (rres->resok->file_attributes.present);
      write_file (cur_offst, size, rres);
      if (blocks_written == total_blocks && eof) {
	installdata ();
	// add chunk to the database --Why add every chunk in the file??
	vec <chunk *> cvp;
	if (chunk_file(cvp, (char const *) out_fname) < 0) {
#if DEBUG > 0
	  warn << "getfp::gotdata: " << strerror (errno) << "(" << errno << ")\n";
#endif
	  delete this;
	}
	for (uint i = 0; i < cvp.size (); i++) {
#if DEBUG > 0
	  warn << "adding fp = " << cvp[i]->fingerprint() << " to lbfsdb\n";
#endif
	  cvp[i]->location().set_fh (e->nh);
	  lbfsdb.add_entry (cvp[i]->fingerprint(), &(cvp[i]->location()));
	  delete cvp[i];
	}
	lbfsdb.sync ();
      }
    } else {
      if (err && (retries++ < 1))
	nfs3_read (cur_offst, size);
      else {
	xfs_reply_err (fd, h.header.sequence_num, err ? err : rres->status);
	delete this;
      }
    }
  }

  void nfs3_read (uint64 cur_offst, uint32 size) 
  {
    read3args ra;
    ra.file = e->nh;
    ra.offset = cur_offst;
    ra.count = size < NFS_MAXDATA ? size : NFS_MAXDATA;
#if DEBUG > 0
    warn << "getfp_obj::nfs3_read @" << offset << " +" << ra.count << "\n";
#endif

    ref<ex_read3res> rres = New refcounted <ex_read3res>;
    c->call (lbfs_NFSPROC3_READ, &ra, rres,
	     wrap (this, &getfp_obj::gotdata, cur_offst, size, rres),
	     lbfs_authof (sa));
  }

  void copy_block (uint64 cur_offst, unsigned char *buf, chunk_location *c) 
  {
    int out_fd = open (out_fname, O_CREAT | O_WRONLY, 0666);
    if (out_fd < 0) {
#if DEBUG > 0
      warn << "getfp_obj::copy_block: " << strerror (errno) << "\n";
#endif
      xfs_reply_err (fd, h.header.sequence_num, EIO);
      delete this;
    }

    if (lseek (out_fd, cur_offst, SEEK_SET) < 0) {
#if DEBUG > 0
      warn << "compose_file: error: " << strerror (errno) 
	   << "(" << errno << ")\n";
#endif
      xfs_reply_err (fd, h.header.sequence_num, EIO);
      delete this;
    }
    int err;
    if ((err = write (out_fd, buf, c->count ())) < 0
	|| ((uint32) err != c->count ())) {
#if DEBUG > 0
      warn << "getfp_obj::copy_block: error: " << err << " != " 
	   << c->count () << " or " << strerror (errno) << "\n";
#endif
      xfs_reply_err (fd, h.header.sequence_num, EIO);
      delete this;
    }     
    close (out_fd);
    blocks_written++;
  }

  void compose_file () 
  {
    int err, chfd; 
    uint64 cur_offst = offset;
    fp_db::iterator * ci = NULL;
    bool found = false;
    nfs_fh3 fh;
    chunk_location c;
    cache_entry *e = NULL;
    
    for (uint i=0; i<fpres->resok->fprints.size(); i++) {
      found = false;
      unsigned char buf[fpres->resok->fprints[i].count];
      if (!lbfsdb.get_iterator (fpres->resok->fprints[i].fingerprint, &ci)) {
	if (ci && !(ci->get (&c))) {
	  do {
	    found = true;
	    c.get_fh (fh);
	    if (c.count () != fpres->resok->fprints[i].count) {
#if DEBUG > 0
	      warn << "chunk size != size from server..\n";
#endif
	      continue;
	    }
	    e = nfsindex[fh];
	    if (!e) {
#if DEBUG > 0
	      warn << "compose_file: null fh or Can't find node handle\n";
#endif
	      found = false;
	      continue;
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
	      continue;
	    }
	    if (lseek (chfd, c.pos (), SEEK_SET) < 0) {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
	      found = false;
	      continue;
	    }
	    if ((err = read (chfd, buf, c.count ())) > -1) {
	      if ((uint32) err != c.count ()) {
#if DEBUG > 0
		warn << "compose_file: error: " << err << " != " 
		     << c.count () << "\n";
#endif
	        found = false;
	        continue;
	      }
	      if (compare_sha1_hash (buf, c.count (),
				     fpres->resok->fprints[i].hash)) {
#if DEBUG > 0
		warn << "compose_file: sha1 hash mismatch\n";
#endif
		found = false;
	        continue;
	      }
	    } else {
#if DEBUG > 0
	      warn << "compose_file: error: " << strerror (errno) 
		   << "(" << errno << ")\n";
#endif
	      found = false;
	      continue;
	    }
	    close (chfd);
	    if (found) {
#if DEBUG > 0
	      warn << "FOUND!! getfp_obj::copy_block: fp = " 
		   << fpres->resok->fprints[i].fingerprint << " in client DB\n";
#endif
	      copy_block (cur_offst, buf, &c);
	    }
	  } while (!found && !(ci->next (&c)));
	}
	delete ci;
      }
      if (!found) {
#if DEBUG > 0
	warn << "compose_file: fp = " << fpres->resok->fprints[i].fingerprint 
	     << " not in DB\n";
#endif
	nfs3_read (cur_offst, fpres->resok->fprints[i].count);
      }
      cur_offst += fpres->resok->fprints[i].count;
    }
    offset = cur_offst;
    if (blocks_written == total_blocks && eof) {
      installdata ();
    }
  }

  void gotfp (time_t rqt, clnt_stat err) 
  {
    if (!err && fpres->status == NFS3_OK) {
      e->nfs_attr = *(fpres->resok->file_attributes.attributes);
      e->set_exp (rqt);
      e->ltime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
      
      total_blocks += fpres->resok->fprints.size ();
      eof = fpres->resok->eof;
      compose_file ();
      
      if (!eof) {
	gfa.offset = offset;
	if (fpres->resok->fprints.size () == 0)
	  gfa.count *= 2;
	fpres = New refcounted <lbfs_getfp3res>;
	c->call (lbfs_GETFP, &gfa, fpres, 
		 wrap (this, &getfp_obj::gotfp, timenow), lbfs_authof (sa));
      }
    } else {
      xfs_reply_err (fd, h.header.sequence_num, err ? err : fpres->status);      
      delete this;
    }
  }

  ~getfp_obj () 
  { 
    if (out_fd) close(out_fd);
    (*cb) ();
  }

  getfp_obj (int fd1, const xfs_message_open &h1, cache_entry *e1, sfs_aid sa1, 
	     ref<aclnt> c1, getfp_obj::cb_t cb1) :
    cb(cb1), fd(fd1), c(c1), h(h1), sa(sa1), e(e1), offset(0), blocks_written(0), 
    total_blocks(0), eof(false), retries(0)    
  {
    str fhstr = armor32(e->nh.data.base(), e->nh.data.size());
    int r = rnd.getword();
    str rstr = armor32((void*)&r, sizeof(int));
    out_fname = strbuf("cache/%02X/sfslbcd.%s.%s", 
		       e->xh.a >> 8, fhstr.cstr(), rstr.cstr());

    out_fd = open (out_fname, O_CREAT | O_WRONLY, 0666);
    if (out_fd < 0) {
#if DEBUG > 0
      warn << "getfp_obj::getfp_obj: " << strerror (errno) << "\n";
#endif
      xfs_reply_err (fd, h.header.sequence_num, EIO);
      delete this;
    }
    gfa.file = e->nh;
    gfa.offset = 0;
    gfa.count = LBFS_MAXDATA;

    fpres = New refcounted <lbfs_getfp3res>;
    c->call (lbfs_GETFP, &gfa, fpres,
	     wrap (this, &getfp_obj::gotfp, timenow),
	     lbfs_authof (sa));
  }
};

void
lbfs_getfp (int fd, const xfs_message_open &h, cache_entry *e, sfs_aid sa, 
	    ref<aclnt> c, getfp_obj::cb_t cb)
{
  vNew getfp_obj (fd, h, e, sa, c, cb);
}

struct readfile_obj {
  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_open h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_read3res> res;

  void done () 
  {
    delete this;
  }

  void get_updated_copy (ptr<ex_getattr3res> res, time_t rqt, clnt_stat err) {
    if (res) {
      if (!err && res->status == NFS3_OK) {
	e->nfs_attr = *(res->attributes);
	e->set_exp (rqt, true);
      } else {
	xfs_reply_err (fd, h.header.sequence_num, err ? err : res->status);
	delete this;
      }
    } 
    nfstime3 maxtime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
    if (greater (maxtime, e->ltime)) 
      lbfs_getfp (fd, h, e, sa, c, wrap (this, &readfile_obj::done));
    else {
      xfs_message_getdata *h1 = (xfs_message_getdata *) &h;
      lbfs_readexist (fd, *h1, e);
      delete this;
    }
  }
  
  readfile_obj (int fd1, const xfs_message_open &h1, cache_entry *e1, sfs_aid sa1, 
		ref<aclnt> c1, readfile_obj::cb_t cb1) :
    cb(cb1), fd(fd1), c(c1), h(h1), sa(sa1), e(e1)    
  {
    uint32 owriters = e->writers;
    if ((h.tokens & XFS_OPEN_MASK) & (XFS_OPEN_NW|XFS_OPEN_EW)) {
      e->writers = 1;
#if DEBUG > 0
      warn << "open for write: " << e->writers << " writers\n";
#endif
    }
    
    if (!e->incache)
      lbfs_getfp (fd, h, e, sa, c, wrap (this, &readfile_obj::done));
    else {
      if (owriters > 0) {
	xfs_message_getdata *h1 = (xfs_message_getdata *) &h;
	lbfs_readexist (fd, *h1, e);
	delete this;
      } else 
	if (e->nfs_attr.expire < (uint32) timenow) {
	  const struct xfs_message_getattr *h1 = (xfs_message_getattr *) &h;
	  l_getattr (fd, *h1, sa, e->nh, c,
	  		wrap (this, &readfile_obj::get_updated_copy));
	} else get_updated_copy (NULL, 0, clnt_stat (0));
    }
  }
};

void 
lbfs_readfile (int fd, const xfs_message_open &h, cache_entry *e, sfs_aid sa, 
	      ref<aclnt> c, readfile_obj::cb_t cb) 
{
  vNew readfile_obj (fd, h, e, sa, c, cb);
}

struct open_obj {
  int fd;
  ref<aclnt> c;

  const struct xfs_message_open h;
  sfs_aid sa;
  cache_entry *e;

  void done () 
  {
    delete this;
  }

  open_obj (int fd1, const xfs_message_open &h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1), sa(sa1)
  {
    e = xfsindex[h.handle];
    if (!e) {
#if DEBUG > 0
      warn << h.header.sequence_num << ":"  
	   << "open_obj: Can't find node handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    } else {
      switch (e->nfs_attr.type) {
      case NF3DIR:
	lbfs_readdir (fd, h, e, sa, c, wrap (this, &open_obj::done));
	break;
      case NF3LNK:
	lbfs_readlink (fd, h, e, sa, c, wrap (this, &open_obj::done)); 
	break;
      case NF3REG:
	lbfs_readfile (fd, h, e, sa, c, wrap (this, &open_obj::done)); 
	break;
      default:
#if DEBUG > 0
	warn << h.header.sequence_num << ":"  
	     << "open_obj: File type " << e->nfs_attr.type << " not handled\n";
#endif	
	break;
      }
    }
  }
};

void 
lbfs_open (int fd, const xfs_message_open &h, sfs_aid sa, ref<aclnt> c) 
{
  vNew open_obj (fd, h, sa, c);
}

struct create_obj {
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_create h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_diropres3> res;
  
  void install (time_t rqt, clnt_stat err) 
  {
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

      assert (res->resok->obj.present && res->resok->obj_attributes.present);
      cache_entry *e1 = update_cache (*res->resok->obj.handle, 
				      *res->resok->obj_attributes.attributes);
      e1->set_exp (rqt);
      xfs_cred cred = h.cred;
      if (e1->nfs_attr.type == NF3DIR) {
	struct xfs_message_mkdir *hm = (xfs_message_mkdir *) &h;
	cred = hm->cred;
      }
      nfsobj2xfsnode (cred, e1, &msg2.node);
    
      assign_cachefile (fd, h.header.sequence_num, e, 
			msg3.cache_name, &msg3.cache_handle);
      
      strcpy (msg1.cache_name, e->cache_name);
      assert (res->resok->dir_wcc.after.present);
      e->nfs_attr = *(res->resok->dir_wcc.after.attributes);
      e->set_exp (rqt);
      nfsobj2xfsnode (cred, e, &msg1.node);
      //messages.C: benjie's rant.
      e->incache = false;

      msg1.flag = XFS_ID_INVALID_DNLC;
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);
      
      msg2.node.tokens = XFS_ATTR_R
	| XFS_OPEN_NW | XFS_OPEN_NR
	| XFS_DATA_R | XFS_DATA_W;      
      msg2.parent_handle = h.parent_handle;
      strlcpy (msg2.name, h.name, sizeof (msg2.name));
    
      msg2.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);

      if (e1->nfs_attr.type == NF3REG) {
	e1->incache = true;
	e1->writers = 1;
      }
      msg3.node = msg2.node;
      msg3.flag = 0;
      msg3.header.opcode = XFS_MSG_INSTALLDATA;
      
      h2 = (struct xfs_message_header *) &msg3;
      h2_len = sizeof (msg3);

      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num,
					0, h0, h0_len, h1, h1_len, h2, h2_len,
					NULL, 0);
    } else xfs_reply_err (fd, h.header.sequence_num, err ? err : res->status);
    delete this;
  }

  void do_mkdir ()
  {
    struct xfs_message_mkdir *h1 = (xfs_message_mkdir *) &h;    
    mkdir3args ma;
    ma.where.dir = e->nh;
    ma.where.name = h1->name;
    xfsattr2nfsattr (h1->header.opcode, h1->attr, &ma.attributes);

    res = New refcounted <ex_diropres3>;
    c->call (lbfs_NFSPROC3_MKDIR, &ma, res,
	     wrap (this, &create_obj::install, timenow));
  }

  void do_create ()
  {
    create3args ca;
    ca.where.dir = e->nh;
    ca.where.name = h.name;
    ca.how.set_mode (GUARDED);
    assert (ca.how.mode == UNCHECKED || ca.how.mode == GUARDED);
    xfsattr2nfsattr (h.header.opcode, h.attr, &(*ca.how.obj_attributes));

    res = New refcounted <ex_diropres3>;
    c->call (lbfs_NFSPROC3_CREATE, &ca, res,
		wrap (this, &create_obj::install, timenow));
  }

  create_obj (int fd1, const xfs_message_create &h1, sfs_aid sa1, 
	      ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1), sa(sa1)
  {

    warn << h.header.sequence_num << ":" <<" parent_handle ("
	 << (int) h.parent_handle.a << ","
	 << (int) h.parent_handle.b << ","
	 << (int) h.parent_handle.c << ","
	 << (int) h.parent_handle.d << ")\n";
    warn << "file name: " << h.name << "\n";
    e = xfsindex[h.parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << "create_obj: Can't find parent_handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    }

    switch (h.header.opcode) {
    case XFS_MSG_CREATE:
      do_create ();
      break;
    case XFS_MSG_MKDIR:
      do_mkdir ();
      break;
    }
  }
};

void 
lbfs_create (int fd, const xfs_message_create &h, sfs_aid sa, ref<aclnt> c)
{
  vNew create_obj (fd, h, sa, c);
}

struct link_obj {
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_link h;
  sfs_aid sa;
  cache_entry *e1, *e2;
  ptr<ex_link3res> res;

  void do_link (time_t rqt, clnt_stat err) 
  {
    if (!err && res->status == NFS3_OK) {
      struct xfs_message_installdata msg1; //update parent dir's data
      struct xfs_message_installnode msg2; //update attr of from_handle
      
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;

      //change attributes of parent dir
      //in the future implement local content change too..
      assign_cachefile (fd, h.header.sequence_num, e2, 
			msg1.cache_name, &msg1.cache_handle);

      assert (res->res->linkdir_wcc.after.present);
      e2->nfs_attr = *(res->res->linkdir_wcc.after.attributes);
      e2->set_exp (rqt, true);
      nfsobj2xfsnode (h.cred, e2, &msg1.node);
      msg1.flag = 0;

      e1->incache = false; // sad mtime update problem with openbsd nfsd
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);

      assert (res->res->file_attributes.present);
      e1->nfs_attr = *(res->res->file_attributes.attributes);
      e1->set_exp (rqt, e1->nfs_attr.type == NF3DIR ? true : false);
      nfsobj2xfsnode (h.cred, e1, &msg2.node);

      msg2.node.tokens = XFS_ATTR_R;
      msg2.parent_handle = h.parent_handle;
      strcpy (msg2.name, h.name);

      msg2.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);
    
      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num, 0,
					h0, h0_len, h1, h1_len, NULL, 0);
    } else xfs_reply_err (fd, h.header.sequence_num, err ? err : res->status);
    delete this;
  }

  link_obj (int fd1, const xfs_message_link &h1, sfs_aid sa1, 
	      ref<aclnt> c1) : fd(fd1), c(c1), h(h1), sa(sa1) 
  {
    cache_entry *e1 = xfsindex[h.from_handle];
    if (!e1) {
#if DEBUG > 0
      warn << "link_obj: Can't find from_handle\n";
#endif
      xfs_reply_err (fd, h.header.sequence_num, ENOENT);
      delete this;
    }
    link3args la;
    la.file = e1->nh;

    e2 = xfsindex[h.parent_handle];
    if (!e2) {
#if DEBUG > 0
      warn << "xfs_message_link: Can't find parent_handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    }
    la.link.dir = e2->nh;
    la.link.name = h.name;

    res = New refcounted < ex_link3res >;
    nfsc->call (lbfs_NFSPROC3_LINK, &la, res,
		wrap (this, &link_obj::do_link, timenow), lbfs_authof (sa));    
  }

};

void 
lbfs_link (int fd, const xfs_message_link &h, sfs_aid sa, ref<aclnt> c)
{
  vNew link_obj (fd, h, sa, c);
}

struct symlink_obj {
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_symlink h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_diropres3> res;
  
  void do_symlink (time_t rqt, clnt_stat err) 
  {
    if (!err && res->status == NFS3_OK) {
      struct xfs_message_installdata msg1;	//install change in parent dir
      struct xfs_message_installnode msg2;	//install symlink node

      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;

      assign_cachefile (fd, h.header.sequence_num, e, 
			msg1.cache_name, &msg1.cache_handle);

      assert (res->resok->dir_wcc.after.present);
      e->nfs_attr = *(res->resok->dir_wcc.after.attributes);
      nfsobj2xfsnode (h.cred, e, &msg1.node);

      msg1.flag = 0;
      e->incache = false; // sad mtime update problem with openbsd nfsd
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);
      
      assert (res->resok->obj.present && res->resok->obj_attributes.present);
      cache_entry *e2 = update_cache (*(res->resok->obj.handle), 
				      *res->resok->obj_attributes.attributes);
      e2->set_exp (rqt);
      nfsobj2xfsnode (h.cred, e2, &msg2.node);
      msg2.node.tokens = XFS_ATTR_R;
      msg2.parent_handle = h.parent_handle;
      strlcpy (msg2.name, h.name, sizeof (msg2.name));
      
      msg2.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);

      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num,
					0, h0, h0_len, h1, h1_len, NULL, 0);      
    } else {
      xfs_reply_err (fd, h.header.sequence_num, err ? err : res->status);
    }
    delete this;
  }

  symlink_obj (int fd1, const xfs_message_symlink &h1, sfs_aid sa1, 
	       ref<aclnt> c1) : fd(fd1), c(c1), h(h1), sa(sa1) 
  {
    e = xfsindex[h.parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << "symlink_obj: Can't find parent handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    }

    symlink3args sla;
    sla.where.dir = e->nh;
    sla.where.name = h.name;
    xfsattr2nfsattr (h.header.opcode, h.attr, 
		     &(sla.symlink.symlink_attributes));
    sla.symlink.symlink_data.setbuf (h.contents, strlen (h.contents));
    res = New refcounted <ex_diropres3>;
    c->call (lbfs_NFSPROC3_SYMLINK, &sla, res,
	     wrap (this, &symlink_obj::do_symlink, timenow), lbfs_authof (sa));
  }
};

void 
lbfs_symlink (int fd, const xfs_message_symlink &h, sfs_aid sa, ref<aclnt> c)
{
  vNew symlink_obj (fd, h, sa, c);
}

struct setattr_obj {
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_putattr h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_wccstat3> res;

  setattr_obj (int fd1, const xfs_message_putattr &h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1), sa(sa1) 
  {
    e = xfsindex[h.handle];
    if (!e) {
#if DEBUG > 0
      warn << "putattr_obj: Can't find node handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    }

    setattr3args sa;
    sa.object = e->nh;
    xfsattr2nfsattr (h.header.opcode, h.attr, &sa.new_attributes);
    sa.guard.set_check (false);

    if (sa.new_attributes.size.set) {
#if DEBUG > 0
      warn << "setting size to " 
	   << (uint32) *(sa.new_attributes.size.val) << "\n";
#endif
      // can't depend on client set time to expire cache data
      truncate(e->cache_name, *(sa.new_attributes.size.val));
    }
    res = New refcounted <ex_wccstat3>;
#if 0
    c->call (lbfs_NFSPROC3_SETATTR, &sa, res,
	     wrap (&installattr, fd, h.header.sequence_num, h.cred, e->nh, 
		   res->wcc.after***, timenow));  
#endif
  }
};

void 
lbfs_setattr (int fd, const xfs_message_putattr &h, sfs_aid sa, ref<aclnt> c)
{
  vNew setattr_obj (fd, h, sa, c);
}

struct remove_obj {
  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_remove h;
  sfs_aid sa;
  time_t rqt1;
  cache_entry *e1, *e2;
  diropargs3 doa;
  ptr<ex_lookup3res> lres;
  ptr<ex_wccstat3> wres;

  void install (time_t rqt, clnt_stat err) 
  {
    if (!err && wres->status == NFS3_OK) {

      assert (wres->wcc->after.present);

      struct xfs_message_installdata msg1;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_installattr msg2;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;
      
      assign_cachefile (fd, h.header.sequence_num, e1, 
			msg1.cache_name, &msg1.cache_handle);
      nfsobj2xfsnode (h.cred, e1, &msg1.node);
      e1->incache = false; //TODO:try to remove entry from dirfile locally
      e1->nfs_attr = *(wres->wcc->after.attributes);
      e1->set_exp (rqt, true);
      msg1.node.tokens |= XFS_DATA_R;
      msg1.flag = XFS_ID_INVALID_DNLC;
    
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);

      assert (lres->resok->obj_attributes.present || lres->resok->dir_attributes.present);
      ex_post_op_attr a = lres->resok->obj_attributes.present ? 
	lres->resok->obj_attributes : lres->resok->dir_attributes;
      if ((a.attributes->type == NF3DIR && a.attributes->nlink > 2) ||
	  (a.attributes->type == NF3REG && a.attributes->nlink > 1)) {
	e2 = nfsindex[lres->resok->object];
	if (!e2) {
#if DEBUG > 0
	  warn << "remove_obj::install: Can't find handle\n";
#endif
	  delete this;
	}
	
	msg2.header.opcode = XFS_MSG_INSTALLATTR;
	--(a.attributes->nlink);
	e2->nfs_attr = *a.attributes;
	e2->set_exp (rqt1, e2->nfs_attr.type == NF3DIR); 
	nfsobj2xfsnode (h.cred, e2, &msg2.node);
	h1 = (struct xfs_message_header *) &msg2;
	h1_len = sizeof (msg2);
	
	xfs_send_message_wakeup_multiple (fd, h.header.sequence_num,
					  0, h0, h0_len, h1, h1_len,
					  NULL, 0);
      } else //TODO: bump it off the hash table
	xfs_send_message_wakeup_multiple (fd, h.header.sequence_num,
					  0, h0, h0_len, NULL, 0);
      
    } else xfs_reply_err (fd, h.header.sequence_num, err ? err : wres->status);

    delete this;
  }

  void do_remove (time_t rqt, clnt_stat err) 
  {
    if (!err && lres->status == NFS3_OK) {
      rqt1 = rqt;
      wres =  New refcounted <ex_wccstat3>;
      c->call (lbfs_NFSPROC3_REMOVE, &doa, wres,
	       wrap (this, &remove_obj::install, timenow));
    } else {
      xfs_reply_err (fd, h.header.sequence_num, err ? err : wres->status);
      delete this;
    }
  }

  remove_obj (int fd1, const xfs_message_remove &h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1), sa(sa1) 
  {
    e1 = xfsindex[h.parent_handle];
    if (!e1) {
#if DEBUG > 0
      warn << "xfs_message_remove: Can't find parent_handle\n";
#endif
      xfs_reply_err(fd, h.header.sequence_num, ENOENT);
      delete this;
    }

    doa.dir = e1->nh;
    doa.name = h.name;

    lres = New refcounted <ex_lookup3res>;
    c->call (lbfs_NFSPROC3_LOOKUP, &doa, lres,
	     wrap (this, &remove_obj::do_remove, timenow));
  }
};

void
lbfs_remove (int fd, const xfs_message_remove &h, sfs_aid sa, ref<aclnt> c)
{
  vNew remove_obj (fd, h, sa, c);
}
