
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "lbfsdb.h"

// map file into memory, pointed to by bufp
int mapfile (const u_char **bufp, size_t *sizep, const char *path);

// chunk a file
int chunk_file(const char *path, unsigned chunk_size, vec<lbfs_chunk *> *cvp);

// chunk a piece of memory
int chunk_data(unsigned chunk_size, const unsigned char *data, 
               size_t size, vec<lbfs_chunk *> *cvp);

// compute fingerprint of a piece of data
u_int64_t fingerprint(const unsigned char *data, size_t count);


#endif _CHUNKING_H_

