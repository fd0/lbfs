
#ifndef _LBFS_H_
#define _LBFS_H_

#include "arpc.h"
#include "nfs3_prot.h"
#include "lbfs_prot.h"
#include "sfsrwsd.h"


// translates LBFS proc number to NFS3 proc number for dealing with res
// structures: CONDWRITE uses WRITE res structures, COMMITTMP uses WRITE res
// structures, and MKTMPFILE uses CREATE res structures.

#define LBFS_PROC_RES_TRANS(p) \
  (p == lbfs_CONDWRITE ? NFSPROC3_WRITE : \
     (p == lbfs_MKTMPFILE ? NFSPROC3_CREATE : \
       (p == lbfs_COMMITTMP ? NFSPROC3_COMMIT : p)))

extern void lbfs_nfs3exp_err (svccb *sbp, nfsstat3 status);
extern void lbfs_exp_enable(u_int32_t, void *);
extern void lbfs_exp_disable(u_int32_t, void *);
extern void lbfs_getxattr(xattrvec *, u_int32_t, void *, void *);

// issues READ requests to server. for each successful read, pass data
// pointer, number of bytes read, and offset to the rcb. when all read
// requests are finished, call cb and pass the total number of bytes read.

void nfs3_read (ref<aclnt> c, const nfs_fh3 &fh,
                callback<void, const unsigned char *, size_t, off_t>::ref rcb,
                callback<void, size_t, read3res *, str>::ref cb,
                off_t pos, size_t count);

// make nfs directory
void nfs3_mkdir (ref<aclnt> c, const nfs_fh3 &dir, const str &name, sattr3 attr,
                 callback<void, const nfs_fh3 *, str>::ref);

// copy data from one filehandle to another. for every successful read from
// the src file handle, call rcb and pass in the data pointer, number of bytes
// read, and offset. when copy is completed, call cb, pass in the file
// attribute of the dst filehandle, and the final commit res object.
void nfs3_copy (ref<aclnt> c, const nfs_fh3 &src, const nfs_fh3 &dst, 
                callback<void, const unsigned char *, size_t, off_t>::ref rcb,
                callback<void, const FATTR3 *, commit3res *, str>::ref cb);

void nfs3_write (ref<aclnt> c, const nfs_fh3 &fh, 
                 callback<void, write3res *, str>::ref cb,
		 unsigned char *data, off_t pos, uint32 count, stable_how s);

#endif _LBFS_H_

