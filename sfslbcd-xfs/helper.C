#include "xfs.h"
#include "xfs-sfs.h"

void sfs_getfsinfo (xfscall *xfsc) {

  sfs_fsinfo *fsi = ((xfs_getroot_args*) xfsc->getvoidres ())->fsi;
  
  assert (fsi->prog == LBFS_PROGRAM && fsi->nfs->vers == LBFS_V3);
  x->compress ();
  xfsc->instance++;

  ((xfs_getroot_args*) xfsc->getvoidres ())->nfs_fsi = New ex_fsinfo3res;
  nfsc->call (lbfs_NFSPROC3_FSINFO, &fsi->nfs->v3->root, 
	      ((xfs_getroot_args*) xfsc->getvoidres ())->nfs_fsi,
	      wrap (&nfs_dispatch, xfsc));
}

void nfs3_fsinfo (xfscall *xfsc) {

  sfs_fsinfo *fsi = ((xfs_getroot_args*) xfsc->getvoidres ())->fsi;
  ex_fsinfo3res *nfs_fsi = ((xfs_getroot_args*) xfsc->getvoidres ())->nfs_fsi;
  assert (nfs_fsi->status == NFS3_OK);

  xfsc->instance++;
  nfs_fsinfo = *nfs_fsi->resok;
  ((xfs_getroot_args*) xfsc->getvoidres ())->attr_res = New ex_getattr3res;

  nfsc->call (lbfs_NFSPROC3_GETATTR, &fsi->nfs->v3->root, 
	      ((xfs_getroot_args*) xfsc->getvoidres ())->attr_res,
	      wrap (&nfs_dispatch, xfsc));

}

void nfs3_getattr (xfscall *xfsc) {

  ex_getattr3res *attr_res = ((xfs_getroot_args*) xfsc->getvoidres ())->attr_res;
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
				    ((struct xfs_message_header *)
				     xfsc->getvoidarg ())->sequence_num, 
				    0, h0, h0_len, NULL, 0);
}

