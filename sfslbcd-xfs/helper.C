#include "sfsclient.h"
#include "xfs.h"
#include "xfs-sfs.h"
#include "xfs-nfs.h"
#include "cache.h"

AUTH *auth_root = authunix_create ("localhost", 0, 0, 0, NULL);
AUTH *auth_default = 
  authunix_create ("localhost", (uid_t) 14228, (gid_t) 100, 0, NULL);

struct getattr_obj {
  
  typedef callback<void, ptr<ex_getattr3res>, time_t, clnt_stat>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;

  struct xfs_message_getattr *h;
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
      nfsobj2xfsnode (h->cred, e, &msg.node);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,	
					0, h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h->header.sequence_num, err ? EIO : res->status);
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
	      auth_default);    
  }
  
  getattr_obj (int fd1, ref<aclnt> c1, xfs_message_getattr *h1,
	       const nfs_fh3 &fh1, cb_t cb1) : 
    cb(cb1), fd(fd1), c(c1), h(h1), fh(fh1) 
  {
    getattr ();
  }
  
};

void 
lbfs_getattr(int fd, xfs_message_getattr *h, const nfs_fh3 &fh, 
	     ref<aclnt> c, getattr_obj::cb_t cb) 
{
  vNew getattr_obj (fd, c, h, fh, cb);
}

struct getroot_obj {
  int fd; 
  ref<aclnt> sc;
  ref<aclnt> nc;
  
  struct xfs_message_getroot *h;
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
    nfsobj2xfsnode (h->cred, e, &msg.node);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,	
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
    lbfs_getattr (fd, (xfs_message_getattr *) h, sfs_fsi->nfs->v3->root, 
		  nc, wrap (this, &getroot_obj::gotattr));
  }

  void getsfs_fsinfo () 
  {
    sfs_fsi = New refcounted<sfs_fsinfo>;
    sfsc->call (SFSPROC_GETFSINFO, NULL, sfs_fsi,
		wrap (this, &getroot_obj::getnfs_fsinfo), lbfs_authof (sa));
  }

  getroot_obj (int fd1, xfs_message_getroot *h1, sfs_aid sa1,  
	       ref<aclnt> sc1, ref<aclnt> nc1) : 
    fd(fd1), sc(sc1), nc(nc1), h(h1), sa(sa1), gotnfs_fsi(false), 
    gotroot_attr(false) 
  {
    getsfs_fsinfo ();
  }
};

void 
lbfs_getroot (int fd1, xfs_message_getroot *h1, sfs_aid sa1, 
	      ref<aclnt> sc1, ref<aclnt> nc1) 
{
  vNew getroot_obj (fd1, h1, sa1, sc1, nc1);
}

struct lookup_obj {
  
  typedef callback<void, ptr<ex_lookup3res>, time_t, clnt_stat>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;

  struct xfs_message_getnode *h;
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
    warn << h->header.sequence_num 
	 << ":" << "requesting file name " << doa.name << "\n";
#endif
    res = New refcounted<ex_lookup3res>;

    nfsc->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
	        wrap (this, &lookup_obj::found, timenow), auth_default);
  }

  lookup_obj (int fd1, struct xfs_message_getnode *h1, 
	      const nfs_fh3 &parent_fh1, const char *name1, 
	      ref<aclnt> c1, cb_t cb1) : cb(cb1), fd(fd1), c(c1), 
		h(h1), parent_fh(parent_fh1), name(name1)
  {
    lookup ();
  }
  
};

void lbfs_lookup (int fd, struct xfs_message_getnode *h, 
		  const nfs_fh3 &parent_fh, const char *name, 
		  ref<aclnt> c, lookup_obj::cb_t cb) 
{
  vNew lookup_obj (fd, h, parent_fh, name, c, cb);
}

struct getnode_obj {
  int fd;
  ref<aclnt> c;
  
  struct xfs_message_getnode *h;
  //ptr<ex_lookup3res> res;
  
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
	warn << h->header.sequence_num << ":getnode: error no attr present\n";
#endif
	xfs_reply_err(fd, h->header.sequence_num, ENOSYS); //Why ENOSYS??
	delete this;
	return;
      }

      struct xfs_message_installnode msg;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      cache_entry *e = update_cache (lres->resok->object, a);

      e->set_exp (rqt);
      nfsobj2xfsnode (h->cred, e, &msg.node);
      msg.header.opcode = XFS_MSG_INSTALLNODE;
      msg.parent_handle = h->parent_handle;
      strcpy (msg.name, h->name);
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h->header.sequence_num, err ? EIO : lres->status);

    delete this;
  }

  getnode_obj (int fd1, xfs_message_getnode *h1, ref<aclnt> c1) :
    fd(fd1), c(c1), h(h1) 
  {  
    cache_entry *e = xfsindex[h->parent_handle];
    if (!e) {
#if DEBUG > 0
      warn << h->header.sequence_num 
	   << ":" << "xfs_getnode: Can't find parent_handle\n";
#endif
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
    } else 
      lbfs_lookup (fd, h, e->nh, h->name, c, 
		   wrap (this, &getnode_obj::installnode));
  }
};

void 
lbfs_getnode (int fd1, xfs_message_getnode *h1, ref<aclnt> c1)
{  
  vNew getnode_obj (fd1, h1, c1);
}




