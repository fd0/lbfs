
#ifndef _LBFS_H_
#define _LBFS_H_

#include "arpc.h"
#include "nfs3_prot.h"
#include "sfsrwsd.h"


// translates LBFS proc number to NFS3 proc number for dealing with res
// structures: CONDWRITE uses WRITE res structures, COMMITTMP uses WRITE res
// structures, and MKTMPFILE uses CREATE res structures.

#define LBFS_PROC_RES_TRANS(p) \
  (p == lbfs_NFSPROC3_CONDWRITE ? NFSPROC3_WRITE : \
     (p == lbfs_NFSPROC3_MKTMPFILE ? NFSPROC3_CREATE : \
       (p == lbfs_NFSPROC3_COMMITTMP ? NFSPROC3_COMMIT : p)))

extern void lbfs_nfs3exp_err (svccb *sbp, nfsstat3 status);

// read from nfs_fh3 object. pass into callback a data ptr, a count, and a
// reference to post_op_attr. callback should free data ptr when done, even if
// an error state is passed to the callback.

void readfh3 (ref<aclnt> c, const nfs_fh3 &fh,
              callback<void, unsigned char *, size_t, str>::ref,
              off_t pos, size_t count);

void mkdir3 (ref<aclnt> c, const nfs_fh3 &dir, const str &name, sattr3 attr,
             callback<void, const nfs_fh3 *, str>::ref);

void copy3 (ref<aclnt> c, const nfs_fh3 &src, const nfs_fh3 &dst, 
            callback<void, const unsigned char *, size_t>::ref,
            callback<void, const FATTR3 *, commit3res *, str>::ref);

#endif _LBFS_H_

