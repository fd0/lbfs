
#ifndef _CHUNKING_H_
#define _CHUNKING_H_

// we keep P(t), x, and K the same for whole file system, so two equivalent
// files would have the same breakmarks. for string A, fingerprint of A is
//
//   f(A) = A(t) mod P(t)
//
// we create breakmarks when
//
//   f(A) mod K = x
//
// if we use K = 8192, the average chunk size is 8k. we allow multiple K
// values so we can do multi-level chunking.


#include "sfs_prot.h"
#include "chunk.h"
#include "rabinpoly.h"

#define FINGERPRINT_PT     0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE    0x78
#define NUM_CHUNK_SIZES    4
#define CHUNK_SIZES(i) \
  (i == 0 ? 8192 : \
   (i == 1 ? 32768 : \
    (i == 2 ? 131072 : \
     (i == 3 ? 524288 : 0))))

#define MIN_CHUNK_SIZE 2048
#define MAX_CHUNK_SIZE 16384


// map file into memory, pointed to by bufp
int mapfile (const u_char **bufp, size_t *sizep, const char *path);
u_int64_t fingerprint(const unsigned char *data, size_t count);

int chunk_data(unsigned chunk_size, vec<lbfs_chunk *> *cvp,
               const unsigned char *data, size_t count);
int chunk_file(unsigned chunk_size, vec<lbfs_chunk *> *cvp,
               const char *path);

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
  
  vec<lbfs_chunk *> _cv;
  void handle_hash(const unsigned char *data, size_t size);

public:

  Chunker(unsigned s, bool hash=false); 
  ~Chunker();

  void stop();
  void chunk (const unsigned char *data, size_t size);

  const vec<lbfs_chunk*>& chunk_vector() { return _cv; }
  void get_chunk_vector(vec<lbfs_chunk*>*);
};

inline void
Chunker::get_chunk_vector(vec<lbfs_chunk*> *cvp)
{
  *cvp = _cv;
}

#endif _CHUNKING_H_

