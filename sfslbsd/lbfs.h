
#ifndef _LBFS_H_
#define _LBFS_H_

#include "arpc.h"
#include "nfs3_prot.h"

// read from nfs_fh3 object. pass into callback a data ptr, a count, and a
// reference to post_op_attr. callback should free data ptr when done, even if
// an error state is passed to the callback.

void readfh3 (ref<aclnt> c, nfs_fh3 &fh,
              callback<void, unsigned char *, size_t, str>::ref,
              off_t pos, size_t count);

void readfh3 (ref<aclnt> c, nfs_fh3 &fh,
              callback<void, unsigned char *, size_t, str>::ref,
              off_t pos, size_t count, auto_auth &);

#endif _LBFS_H_

