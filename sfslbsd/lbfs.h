
#ifndef _LBFS_H_
#define _LBFS_H_

#include "arpc.h"
#include "nfs3_prot.h"


// translates LBFS proc number to NFS3 proc number for dealing with res
// structures: CONDWRITE uses WRITE res structures, MKTMPFILE uses CREATE res
// structures.

#define LBFS_PROC_RES_TRANS(p) \
  (p == lbfs_NFSPROC3_CONDWRITE ? NFSPROC3_WRITE : \
     (p == lbfs_NFSPROC3_MKTMPFILE ? NFSPROC3_CREATE : p))

// read from nfs_fh3 object. pass into callback a data ptr, a count, and a
// reference to post_op_attr. callback should free data ptr when done, even if
// an error state is passed to the callback.

void readfh3 (ref<aclnt> c, const nfs_fh3 &fh,
              callback<void, unsigned char *, size_t, str>::ref,
              off_t pos, size_t count);

void mkdir3 (ref<aclnt> c, const nfs_fh3 &dir, const str &name, sattr3 attr,
             callback<void, const nfs_fh3 *, str>::ref);

#endif _LBFS_H_

