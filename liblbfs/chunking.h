
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "lbfsdb.h"

int mapfile (const u_char **bufp, size_t *sizep, const char *path);
int chunk_file(const char *path, unsigned chunk_size, vec<lbfs_chunk *> *cvp);
int chunk_data(const char *path, unsigned chunk_size, 
               const unsigned char *data, size_t size, vec<lbfs_chunk *> *cvp);

#endif _CHUNKING_H_

