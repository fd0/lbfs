
#ifndef _LBFS_H_
#define _LBFS_H_

#include "arpc.h"
#include "nfs3_prot.h"
#include "lbfs_prot.h"

// translates LBFS proc number to NFS3 proc number for dealing with res
// structures: CONDWRITE uses WRITE res structures, COMMITTMP uses WRITE res
// structures, and MKTMPFILE uses CREATE res structures.

#define LBFS_PROC_RES_TRANS(p) \
  (p == lbfs_CONDWRITE ? NFSPROC3_WRITE : \
     (p == lbfs_MKTMPFILE ? NFSPROC3_CREATE : \
       (p == lbfs_COMMITTMP ? NFSPROC3_COMMIT : p)))

extern void lbfs_getxattr(xattrvec *, u_int32_t, void *, void *);

#endif _LBFS_H_

