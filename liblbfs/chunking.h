
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "lbfsdb.h"

/* 
 * break file into chunks. if can't mmap file, returns
 * -1, otherwise, 0 is returned. creates chunk objects
 * in the vector, so they will have to be freed by
 * caller.
 */
int mapfile (const u_char **bufp, size_t *sizep, const char *path);
int chunk_file(const char *path, const nfs_fh3 *fhp, unsigned csize, 
               vec<lbfs_chunk *> *cvp);
int chunk_data(const char *path, const nfs_fh3 *fhp, unsigned csize, 
               const unsigned char *data, size_t size, 
	       vec<lbfs_chunk *> *cvp);

#endif _CHUNKING_H_

