#ifndef _LBFS_DB_
#define _LBFS_DB_

#include "vec.h"
#include "async.h"
#include "db_cxx.h"

#define FMAP_DB "fmap.db"

// we keep P(t), x, and K the same for whole file system, so two equivalent
// files would have the same breakmarks. for string A, fingerprint of A is
//
//   f(A) = A(t) mod P(t) 
//
// we create breakmarks when 
//
//   f(A) mod K = x
//
// we use K = 8192 so that average chunk size is 8k. we allow multiple K
// values so we can do multi-level chunking.

#define FINGERPRINT_PT     0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE    0x78
#define NUM_CHUNK_SIZES    4
#define CHUNK_SIZES(i) \
  (i == 0 ? 8192 : \
   (i == 1 ? 32768 : \
    (i == 2 ? 131072 : \
     (i == 3 ? 524288 : 0))))

#define MY_PATH_MAX 1024

struct lbfs_chunk {
  char path[MY_PATH_MAX];
  off_t pos;
  ssize_t size;
  u_int64_t fingerprint;

  lbfs_chunk() {}
  lbfs_chunk(const char *p, off_t o, ssize_t s, u_int64_t f) {
    strncpy(path, p, (strlen(p)+1) > MY_PATH_MAX ? MY_PATH_MAX : (strlen(p)+1));
    pos = o;
    size = s;
    fingerprint = f;
  }
};

inline bool
operator ==(const lbfs_chunk &a, const lbfs_chunk &b)
{
  return 
    ((strncmp(a.path, b.path, MY_PATH_MAX) == 0) && 
     a.pos == b.pos && 
     a.size == b.size &&
     a.fingerprint == b.fingerprint);
}


class lbfs_db {
public:
  lbfs_db(const char *name);
  ~lbfs_db();

  // open db, returns db3 errnos
  int open(); 

  // search db, given fingerprint, writes chunk ptr, return db3 errnos
  int search(u_int64_t fingerprint, lbfs_chunk *cp);

  // returns db3 errnos
  int add_chunk(u_int64_t fingerprint, lbfs_chunk *c);
  int remove_chunk(u_int64_t fingerprint, lbfs_chunk *c);

  // XXX iterator function for a fingerprint

private: 
  Db _dbp;
  const char* _name;

};


#endif _LBFS_DB_

