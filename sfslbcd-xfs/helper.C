#include "xfs.h"
#include "xfs-sfs.h"
#include "xfs-nfs.h"

AUTH *auth_root = authunix_create ("localhost", 0, 0, 0, NULL);
AUTH *auth_default = 
  authunix_create ("localhost", (uid_t) 14228, (gid_t) 100, 0, NULL);

struct getattr_obj {
  
  typedef callback<void, ptr<ex_getattr3res>, time_t, clnt_stat>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;

  struct xfs_message_getattr *h;
  nfs_fh3 fh;
  ptr<ex_getattr3res> res;
  
  void gotattr (time_t rqt, clnt_stat err) {
    if (!err) {
      (*cb) (res, rqt, err);
      //update attr cache
    } else
      (*cb) (NULL, 0, err);
    delete this;
  }

  void getattr () {
    res = New refcounted<ex_getattr3res>; 
    c->call (lbfs_NFSPROC3_GETATTR, &fh, res,
	      wrap (this, &getattr_obj::gotattr, timenow), 
	      auth_default);    
  }
  
  getattr_obj (int fd1, ref<aclnt> c1, xfs_message_getattr *h1,
	       const nfs_fh3 &fh1, cb_t cb1) : 
    cb(cb1), fd(fd1), c(c1), h(h1), fh(fh1) {

    getattr ();
  }
  
};

void 
lbfs_getattr(int fd, xfs_message_getattr *h, const nfs_fh3 &fh, 
	     ref<aclnt> c, getattr_obj::cb_t cb) {
  vNew getattr_obj (fd, c, h, fh, cb);
}

struct getroot_obj {
  int fd; 
  ref<aclnt> sc;
  ref<aclnt> nc;
  
  struct xfs_message_getroot *h;
  bool gotnfs_fsi;
  bool gotroot_attr;
  time_t rqtime;
  ptr<sfs_fsinfo> sfs_fsi;
  ptr<ex_fsinfo3res> nfs_fsi;
  ptr<ex_getattr3res> root_attr;

  void installroot () {
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installroot msg;
    msg.header.opcode = XFS_MSG_INSTALLROOT;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    nfsobj2xfsnode 
      (h->cred, sfs_fsi->nfs->v3->root, *root_attr->attributes, rqtime, &msg.node);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,	
     				      0, h0, h0_len, NULL, 0);
    delete this;
  }

  void gotnfs_fsinfo (clnt_stat err) {    
    assert (!err && nfs_fsi->status == NFS3_OK);
    nfs_fsinfo = *nfs_fsi->resok;
    gotnfs_fsi = true;
    if (gotroot_attr)
      installroot ();
  }

  void gotattr (ptr<ex_getattr3res> attr, time_t rqt, clnt_stat err) {
    assert (!err && attr->status == NFS3_OK);
    root_attr = attr;
    rqtime = rqt;
    gotroot_attr = true;
    if (gotnfs_fsi) 
      installroot ();
  }

  void getnfs_fsinfo (clnt_stat err) {

    assert (!err && sfs_fsi->prog == ex_NFS_PROGRAM && 
	    sfs_fsi->nfs->vers == ex_NFS_V3);

    x->compress();
    nfs_fsi = New refcounted<ex_fsinfo3res>;
    nc->call (lbfs_NFSPROC3_FSINFO, &sfs_fsi->nfs->v3->root, nfs_fsi,
	      wrap (this, &getroot_obj::gotnfs_fsinfo), auth_default);
    lbfs_getattr (fd, (xfs_message_getattr *) h, sfs_fsi->nfs->v3->root, 
		  nc, wrap (this, &getroot_obj::gotattr));
  }

  void getsfs_fsinfo () {

    sfs_fsi = New refcounted<sfs_fsinfo>;
    sfsc->call (SFSPROC_GETFSINFO, NULL, sfs_fsi,
		wrap (this, &getroot_obj::getnfs_fsinfo), 
		auth_default);
  }

  getroot_obj (int fd1, xfs_message_getroot *h1, 
	       ref<aclnt> sc1, ref<aclnt> nc1) : 
    fd(fd1), sc(sc1), nc(nc1), h(h1), gotnfs_fsi(false), gotroot_attr(false) {

    getsfs_fsinfo ();
  }
};

void 
lbfs_getroot (int fd1, xfs_message_getroot *h1, ref<aclnt> sc1, ref<aclnt> nc1) {
  vNew getroot_obj (fd1, h1, sc1, nc1);
}

void sfs_getfsinfo (ref<xfscall> xfsc) {

  sfs_fsinfo *fsi = static_cast<sfs_fsinfo *> (xfsc->resp[xfsc->inst]);
  
  assert (fsi->prog == ex_NFS_PROGRAM && fsi->nfs->vers == ex_NFS_V3);
  x->compress ();

  ex_fsinfo3res *nfs_fsi = New ex_fsinfo3res;
  xfsc->resp[++xfsc->inst] = nfs_fsi;
  nfsc->call (lbfs_NFSPROC3_FSINFO, &fsi->nfs->v3->root, nfs_fsi,
	      wrap (&nfs_dispatch, xfsc, timenow));
}

void nfs3_fsinfo (ref<xfscall> xfsc) {

  sfs_fsinfo *fsi = static_cast<sfs_fsinfo *> (xfsc->resp[xfsc->inst-1]);
  ex_fsinfo3res *nfs_fsi = static_cast<ex_fsinfo3res *> (xfsc->resp[xfsc->inst]); 
  assert (nfs_fsi->status == NFS3_OK);

  nfs_fsinfo = *nfs_fsi->resok;
  ex_getattr3res *attr_res = New ex_getattr3res;
  xfsc->resp[++xfsc->inst] = attr_res;

  nfsc->call (lbfs_NFSPROC3_GETATTR, &fsi->nfs->v3->root, attr_res,
	      wrap (&nfs_dispatch, xfsc, timenow));
}

void nfs3_getattr (ref<xfscall> xfsc) {

  ex_getattr3res *attr_res = static_cast<ex_getattr3res *> (xfsc->resp[xfsc->inst]);
  assert (attr_res->status == NFS3_OK);

  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;
  if (xfsc->opcode == XFS_MSG_GETROOT) {
    struct xfs_message_installroot msg;
    msg.header.opcode = XFS_MSG_INSTALLROOT;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);
  } else {
    struct xfs_message_installattr msg;
    msg.header.opcode = XFS_MSG_INSTALLATTR;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);
  }

#if 0
  nfsobj2xfsnode 
    (h->cred, fsi->nfs->v3->root, *attr_res->attributes, rqtime, &msg.node);
#endif

  xfs_send_message_wakeup_multiple (xfsc->fd, 
				    ((xfs_message_header *) 
				    (xfsc->argp))->sequence_num, 
				    0, h0, h0_len, NULL, 0);
}



