#ifndef _CHUNK_H_
#define _CHUNK_H_

#include "vec.h"
#include "sha1.h"
#include "async.h"
#include "sfs_prot.h"
#include "nfs3_prot.h"

class lbfs_chunk_loc {

private:
  unsigned char _fh[NFS3_FHSIZE];
  unsigned _fhsize;
  off_t _pos;
  size_t _count;
 
public:
  lbfs_chunk_loc() {
    _fhsize = 0;
  }
  
  lbfs_chunk_loc(off_t p, size_t c) {
    _fhsize = 0;
    _pos = p;
    _count = c;
  }

  lbfs_chunk_loc& operator= (const lbfs_chunk_loc &l) {
    _fhsize = l._fhsize;
    if (_fhsize > 0) memmove(_fh, l._fh, _fhsize);
    _pos = l._pos;
    _count = l._count;
    return *this;
  }
  
  void set_fh(const nfs_fh3 &f) {
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

  off_t pos() const 		{ return _pos; }
  void set_pos(off_t p) 	{ _pos = p; }
  
  size_t count() const 		{ return _count; }
  void set_count(size_t c) 	{ _count = c; }
};

struct lbfs_chunk {
  lbfs_chunk_loc loc;
  sfs_hash hash;
  u_int64_t fingerprint;
  lbfs_chunk(off_t p, size_t s, u_int64_t fp) : loc(p, s), fingerprint(fp) {}
};

#endif

