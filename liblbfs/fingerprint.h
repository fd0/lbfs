
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "lbfsdb.h"
#include "rabinpoly.h"

// map file into memory, pointed to by bufp
int mapfile (const u_char **bufp, size_t *sizep, const char *path);
u_int64_t fingerprint(const unsigned char *data, size_t count);

class Chunker {
private:
  window _w;
  unsigned _chunk_size;
  size_t _last_pos;
  size_t _cur_pos;
  u_int64_t _fp;
  vec<lbfs_chunk *> *_cvp;

public:

  Chunker(unsigned s, vec<lbfs_chunk *> *cvp); 
  ~Chunker();

  void stop();
  void chunk_data (const unsigned char *data, size_t size);
  int chunk_file (const char *path);
};



#endif _CHUNKING_H_

