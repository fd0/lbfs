/* $Id$ */

/*
 * This file contains the LBFS protocol: it is based on the NFS3+expire
 * protocol used in sfsrwsd. Any data structures not dealing with LBFS
 * are omitted.
 */

/* XXX TODO: change program numbers to be LBFS instead of ex_NFS3 */

#ifndef RFC_SYNTAX
# define RFC_SYNTAX 1
#endif /* RFC_SYNTAX */

#ifdef RPCC
# ifndef UNION_ONLY_DEFAULT
#  define UNION_ONLY_DEFAULT 1
# endif /* UNION_ONLY_DEFAULT */
#endif

%#include "nfs3exp_prot.h"

struct lbfs_condwrite3args {
  nfs_fh3 file; 
  uint64 offset; 
  uint32 count; 
  stable_how stable; 
  uint64 fingerprint; 
};

union lbfs_condwrite3res switch (nfsstat3 status) {
case NFS3_OK:
  write3resok resok;
default:
  wcc_data resfail;
};

program LBFS_PROGRAM {
	version LBFS_V3 {
		void
		lbfs_NFSPROC3_NULL (void) = 0;
		
		ex_getattr3res
		lbfs_NFSPROC3_GETATTR (nfs_fh3) = 1;
		
		ex_wccstat3
		lbfs_NFSPROC3_SETATTR (setattr3args) = 2;
		
		ex_lookup3res
		lbfs_NFSPROC3_LOOKUP (diropargs3) = 3;
		
		ex_access3res
		lbfs_NFSPROC3_ACCESS (access3args) = 4;
		
		ex_readlink3res
		lbfs_NFSPROC3_READLINK (nfs_fh3) = 5;
		
		ex_read3res
		lbfs_NFSPROC3_READ (read3args) = 6;
		
		ex_write3res
		lbfs_NFSPROC3_WRITE (write3args) = 7;
		
		ex_diropres3
		lbfs_NFSPROC3_CREATE (create3args) = 8;
		
		ex_diropres3
		lbfs_NFSPROC3_MKDIR (mkdir3args) = 9;
		
		ex_diropres3
		lbfs_NFSPROC3_SYMLINK (symlink3args) = 10;
		
		ex_diropres3
		lbfs_NFSPROC3_MKNOD (mknod3args) = 11;
		
		ex_wccstat3
		lbfs_NFSPROC3_REMOVE (diropargs3) = 12;
		
		ex_wccstat3
		lbfs_NFSPROC3_RMDIR (diropargs3) = 13;
		
		ex_rename3res
		lbfs_NFSPROC3_RENAME (rename3args) = 14;
		
		ex_link3res
		lbfs_NFSPROC3_LINK (link3args) = 15;
		
		ex_readdir3res
		lbfs_NFSPROC3_READDIR (readdir3args) = 16;
		
		ex_readdirplus3res
		lbfs_NFSPROC3_READDIRPLUS (readdirplus3args) = 17;
		
		ex_fsstat3res
		lbfs_NFSPROC3_FSSTAT (nfs_fh3) = 18;
		
		ex_fsinfo3res
		lbfs_NFSPROC3_FSINFO (nfs_fh3) = 19;
		
		ex_pathconf3res
		lbfs_NFSPROC3_PATHCONF (nfs_fh3) = 20;
		
		ex_commit3res
		lbfs_NFSPROC3_COMMIT (commit3args) = 21;
		
		lbfs_condwrite3res
		lbfs_NFSPROC3_CONDWRITE (lbfs_condwrite3args) = 22;
		
	} = 3;
} = 344444;

program LBFSCB_PROGRAM {
	version LBFSCB_V3 {
		void
		lbfs_NFSCBPROC3_NULL (void) = 0;

		void
		lbfs_NFSCBPROC3_INVALIDATE (ex_invalidate3args) = 1;
	} = 3;
}= 344445;
