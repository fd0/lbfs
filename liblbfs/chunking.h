
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "lbfsdb.h"

/* 
 * break file into chunks. if can't mmap file, returns
 * -1, otherwise, 0 is returned 
 */
int chunk_file(const char *path, vec<u_int64_t> *fvp, vec<lbfs_chunk *> *cvp);

#endif _CHUNKING_H_

