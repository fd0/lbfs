
#ifndef _LBFS_H_
#define _LBFS_H_

void readfh3 (ref<aclnt> c, nfs_fh3 &fh, 
              callback<void, unsigned char *, int, str>::ref, 
              off_t pos, size_t count);

#endif _LBFS_H_

