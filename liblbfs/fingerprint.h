
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "sfs_prot.h"
#include "lbfsdb.h"
#include "rabinpoly.h"

// map file into memory, pointed to by bufp
int mapfile (const u_char **bufp, size_t *sizep, const char *path);
u_int64_t fingerprint(const unsigned char *data, size_t count);

int chunk_data(unsigned chunk_size, vec<lbfs_chunk *> *cvp,
               const unsigned char *data, size_t count);
int chunk_file(unsigned chunk_size, vec<lbfs_chunk *> *cvp,
               const char *path);

#define MIN_CHUNK_SIZE 2048
#define MAX_CHUNK_SIZE 16384

class Chunker {
private:
  window _w;
  unsigned _chunk_size;
  size_t _last_pos;
  size_t _cur_pos;
  u_int64_t _fp;
  
  unsigned char *_hbuf; 
  unsigned int _hbuf_size;
  unsigned int _hbuf_cursor;
  bool _hash;

  void handle_hash(const unsigned char *data, size_t size);

public:
  vec<lbfs_chunk *> *cvp;
  vec<sfs_hash *> hv;

  Chunker(unsigned s, vec<lbfs_chunk *> *cvp, bool hash=false); 
  ~Chunker();

  void stop();
  void chunk (const unsigned char *data, size_t size);
};



#endif _CHUNKING_H_

