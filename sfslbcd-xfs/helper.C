#include "xfs.h"
#include "xfs-sfs.h"

AUTH *auth_root = authunix_create ("localhost", 0, 0, 0, NULL);
AUTH *auth_default = 
  authunix_create ("localhost", (uid_t) 14228, (gid_t) 100, 0, NULL);

struct getattr_obj {

};

struct getroot_obj {
  //  typedef callback<void, const sfs_fsinfo *, str>::ref cb_t;
  //  cb_t cb;
  ref<aclnt> sc;
  ref<aclnt> nc;
  

  bool gotattr;
  time_t rqtime;
  sfs_fsinfo *sfs_fsi;

  void gotroot(ex_fsinfo3res *nfs_fsi, clnt_stat err) {    
    assert (nfs_fsi->status == NFS3_OK);
    nfs_fsinfo = *nfs_fsi->resok;
  }

  void getnfs_fsinfo (sfs_fsinfo *s_fsi, time_t rqt, clnt_stat err) {
    rqtime = rqt;
    sfs_fsi = s_fsi;

    ref<ex_fsinfo3res> nfs_fsi = New refcounted<ex_fsinfo3res>;
    //    ref<ex_getattr3res> *attr = New refcounted<ex_getattr3res>;

    nc->call (lbfs_NFSPROC3_FSINFO, &sfs_fsi->nfs->v3->root, nfs_fsi,
		wrap (this, &getroot_obj::gotroot, nfs_fsi), auth_default);
#if 0
    nc->call (lbfs_NFSPROC3_GETATTR, &sfs_fsi->nfs->v3->root, attr_res,
		wrap (this, &getroot_obj::gotroot, NULL, attr_res), auth_default);
#endif
  }

  void getsfs_fsinfo () {

    ref<sfs_fsinfo> sfs_fsi = New refcounted<sfs_fsinfo>;
    sfsc->call (SFSPROC_GETFSINFO, NULL, sfs_fsi,
		wrap (this, &getroot_obj::getnfs_fsinfo, sfs_fsi, timenow));
  }

  getroot_obj(ref<aclnt> sc1, ref<aclnt> nc1) : sc(sc1), nc(nc1) {
    getsfs_fsinfo ();
  }
};

void 
getroot (ref<aclnt> sc1, ref<aclnt> nc1) {
  vNew getroot_obj (sc1, nc1);
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



