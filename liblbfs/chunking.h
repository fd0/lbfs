
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "lbfsdb.h"

/* 
 * break file into chunks. if can't mmap file, returns
 * -1, otherwise, 0 is returned. creates chunk objects
 * in the vector, so they will have to be freed by
 * caller.
 */
int chunk_file(const char *path, vec<lbfs_chunk *> *cvp);
int chunk_data(const char *path, const unsigned char *data, 
               size_t size, vec<lbfs_chunk *> *cvp);

#endif _CHUNKING_H_

