
#include "nfstrans.h"
#include "nfs3_nonnul.h"
#include "lbfs_prot.h"
  
inline bool
lbfs_constop (u_int32_t proc)
{
  switch (proc) {
  case lbfs_NFSPROC3_SETATTR:
  case lbfs_NFSPROC3_WRITE:
  case lbfs_NFSPROC3_CREATE:
  case lbfs_NFSPROC3_MKDIR:
  case lbfs_NFSPROC3_SYMLINK:
  case lbfs_NFSPROC3_MKNOD:
  case lbfs_NFSPROC3_REMOVE:
  case lbfs_NFSPROC3_RMDIR:
  case lbfs_NFSPROC3_RENAME:
  case lbfs_NFSPROC3_LINK:
  case lbfs_CONDWRITE:
  case lbfs_TMPWRITE:
  case lbfs_MKTMPFILE:
  case lbfs_COMMITTMP:
  case lbfs_GETFP:
    return false;
  default:
  case lbfs_NFSPROC3_COMMIT:
    return true;
  }
}

#define getxattr(proc, arg, res)			\
  case proc:						\
    rpc_traverse (*xvp, *static_cast<res *> (resp));	\
    break;

void
lbfs_getxattr (xattrvec *xvp, u_int32_t proc, void *argp, void *resp)
{
  xvp->clear ();
  xvp->push_back ().fh = static_cast<nfs_fh3 *> (argp);
  switch (proc) {
    LBFS_PROGRAM_3_APPLY_NOVOID (getxattr, nfs3void);
  default:
    panic ("lbfs_getxattr: bad proc %d\n", proc);
    break;
  }
  if (!(*xvp)[0].fattr && lbfs_constop (proc)
      && !*static_cast<nfsstat3 *> (resp))
    xvp->pop_front ();
}

