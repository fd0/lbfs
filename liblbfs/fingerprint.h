
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

#include "vec.h"
#include "sha1.h"
#include "sfs_prot.h"
#include "rabinpoly.h"

#define FINGERPRINT_PT  0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE 0x78
#define MIN_CHUNK_SIZE  2048
#define MAX_CHUNK_SIZE  65535

class chunk_location {
private:
  off_t _pos;
  size_t _count;
  unsigned _fhsize;
  unsigned char _fh[NFS3_FHSIZE];

public:
  chunk_location() {
    _fhsize = 0;
  }
  
  chunk_location(off_t p, size_t c) {
    _fhsize = 0;
    _pos = p;
    _count = c;
  }

  chunk_location& operator= (const chunk_location &l) {
    _fhsize = l._fhsize;
    if (_fhsize > 0) memmove(_fh, l._fh, _fhsize);
    _pos = l._pos;
    _count = l._count;
    return *this;
  }
  
  void set_fh(const nfs_fh3 &f) {
    bzero(_fh, NFS3_FHSIZE);
    memmove(_fh, f.data.base(), f.data.size());
    _fhsize = f.data.size();
  }

  int get_fh(nfs_fh3 &f) const {
    if (_fhsize > 0) {
      char *data = New char[_fhsize];
      memmove(&data[0], _fh, _fhsize);
      f.data.set(data, _fhsize, freemode::DELETE);
      return 0;
    }
    else
      return -1;
  }

#if WITH_PATH
  void set_path(const char *p) {
    strcpy (_path, p);
  }
  
  char *get_path() { return _path; }
#endif

  off_t pos() const 		{ return _pos; }
  void set_pos(off_t p) 	{ _pos = p; }
  
  size_t count() const 		{ return _count; }
  void set_count(size_t c) 	{ _count = c; }

  size_t size() const { 
    return sizeof(off_t)+sizeof(size_t)+sizeof(unsigned)+_fhsize;
  }
};

class chunk {
private:
  chunk_location _loc;
  sfs_hash _hash;
  
  void compute_hash(unsigned char *data, unsigned count) {
    sha1_hash(_hash.base(), data, count); 
  }

public:
  chunk(off_t p, size_t s, sfs_hash h)
    : _loc(p, s), _hash(h)
  {
  }
  
  chunk(off_t p, size_t s, unsigned char *data)
    : _loc(p, s)
  {
    compute_hash(data, s);
  }
  
  chunk(chunk &c) 
  {
    _loc = c._loc;
    _hash = c._hash;
  }

  chunk& operator= (const chunk &c)
  {
    _loc = c._loc;
    _hash = c._hash;
    return *this;
  }

  sfs_hash hash() const { return _hash; }

  u_int64_t hashidx() const {
    u_int64_t n;
    memmove(&n, _hash.base(), sizeof(n));
    return n;
  }
  
  bool hash_eq(sfs_hash &h) const { 
    return memcmp(h.base(), _hash.base(), sha1::hashsize) == 0; 
  }

  chunk_location& location() { return _loc; }
};

u_int64_t fingerprint(const unsigned char *data, size_t count);
int chunk_data(vec<chunk *>& cvp, const unsigned char *data, size_t count);
int chunk_file(vec<chunk *>& cvp, const char *path);

class Chunker {
private:
  window _w;
  size_t _last_pos;
  size_t _cur_pos;
  
  unsigned char *_hbuf; 
  unsigned int _hbuf_size;
  unsigned int _hbuf_cursor;

  vec<chunk *> _cv;
  void handle_hash(const unsigned char *data, size_t size);

public:
  Chunker();
  ~Chunker();

  void stop();
  void chunk_data (const unsigned char *data, size_t size);

  const vec<chunk*>& chunk_vector() { return _cv; }
  void copy_chunk_vector(vec<chunk*>&);
  
  static const unsigned chunk_size = 2048;
  static unsigned min_size_suppress;
  static unsigned max_size_suppress;
};

inline void
Chunker::copy_chunk_vector(vec<chunk*>& cvp)
{
  cvp.setsize(_cv.size());
  for (unsigned i=0; i<_cv.size(); i++)
    cvp[i] = New chunk(*(_cv[i]));
}

#endif _CHUNKING_H_

