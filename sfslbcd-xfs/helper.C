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
    if (!err && res->status == NFS3_OK) {
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_installattr msg;

      msg.header.opcode = XFS_MSG_INSTALLATTR;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      bool update_dir_expire = false;
      cache_entry *e = update_cache (fh, *res->attributes);
      if (e->nfs_attr.type == NF3DIR) {
	nfstime3 maxtime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
	if (!greater(maxtime, e->ltime))
	  update_dir_expire = true;
      }
      e->set_exp (rqt, update_dir_expire);
      nfsobj2xfsnode (h.cred, e, &msg.node);

      xfs_send_message_wakeup_multiple (fd, h.header.sequence_num,	
					0, h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h.header.sequence_num, err ? EIO : res->status);
  }

  void gotattr (time_t rqt, clnt_stat err) 
  {
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
lbfs_getattr(int fd, const xfs_message_getattr &h, sfs_aid sa, const nfs_fh3 &fh, 
	     ref<aclnt> c, getattr_obj::cb_t cb) 
{
  vNew getattr_obj (fd, h, sa, fh, c, cb);
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
    lbfs_getattr (fd, *h1, sa, sfs_fsi->nfs->v3->root, 
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

    nfsc->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
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
      msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;

      int lfd = assign_cachefile (fd, h.header.sequence_num, e, 
				  msg.cache_name, &msg.cache_handle);
      if (lfd < 0) 
	return;
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
  int cfd = assign_cachefile (fd, h.header.sequence_num, e, 
				  msg.cache_name, &msg.cache_handle);
  assert (cfd >= 0);
  close (cfd);

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
		  wrap (this, &readdir_obj::write_dirfile, args, msg));
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
      if (args.fd < 0)
	delete this;
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
	  lbfs_getattr (fd, *h1, sa, e->nh, c, 
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

  int fd;
  ref<aclnt> c;
  
  const struct xfs_message_open h;
  sfs_aid sa;
  cache_entry *e;
  ptr<lbfs_getfp3res> res;
  
  void gotfp (time_t rqt, clnt_stat err) 
  {
    if (!err && res->status == NFS3_OK) {
      
    } 
  }

  getfp_obj (int fd1, const xfs_message_open &h1, cache_entry *e1, sfs_aid sa1, 
	     ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1), sa(sa1), e(e1)    
  {
#if 0
    nfsobj2xfsnode (h->cred, e, &msg.node);
    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R; // | XFS_OPEN_NW | XFS_DATA_W;
    int cfd = assign_cachefile ();
				
#endif
    lbfs_getfp3args gfa;
    gfa.file = e->nh;
    gfa.offset = 0;
    gfa.count = LBFS_MAXDATA;

    res = New refcounted <lbfs_getfp3res>;

    nfsc->call (lbfs_GETFP, &gfa, res,
		wrap (this, &getfp_obj::gotfp, timenow));
  }
};

void
lbfs_getfp (int fd, const xfs_message_open &h, cache_entry *e, sfs_aid sa, 
	    ref<aclnt> c)
{
  vNew getfp_obj (fd, h, e, sa, c);
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
      ;
      //getfp ();
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
      ;
      //getfp ();
    else {
      if (owriters > 0) {
	xfs_message_getdata *h1 = (xfs_message_getdata *) &h;
	lbfs_readexist (fd, *h1, e);
	delete this;
      } else 
	if (e->nfs_attr.expire < (uint32) timenow) {
	  const struct xfs_message_getattr *h1 = (xfs_message_getattr *) &h;
	  lbfs_getattr (fd, *h1, sa, e->nh, c,
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



