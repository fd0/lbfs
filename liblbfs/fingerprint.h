
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

#define FINGERPRINT_PT  0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE 0x78
#define MIN_CHUNK_SIZE  2048
#define MAX_CHUNK_SIZE  65535

u_int64_t fingerprint(const unsigned char *data, size_t count);
int chunk_data(vec<lbfs_chunk *>& cvp, const unsigned char *data, size_t count);
int chunk_file(vec<lbfs_chunk *>& cvp, const char *path);

class Chunker {
private:
  window _w;
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
  Chunker(bool hash=false); 
  ~Chunker();

  void stop();
  void chunk (const unsigned char *data, size_t size);

  const vec<lbfs_chunk*>& chunk_vector() { return _cv; }
  void copy_chunk_vector(vec<lbfs_chunk*>&);
  
  static const unsigned chunk_size = 8192;
  static unsigned min_size_suppress;
  static unsigned max_size_suppress;
};

inline void
Chunker::copy_chunk_vector(vec<lbfs_chunk*>& cvp)
{
  cvp.setsize(_cv.size());
  for (unsigned i=0; i<_cv.size(); i++)
    cvp[i] = New lbfs_chunk(*(_cv[i]));
}

#endif _CHUNKING_H_

