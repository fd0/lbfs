#ifndef _LBFS_DB_
#define _LBFS_DB_

#include "vec.h"
#include "async.h"
#include "db_cxx.h"

#define FMAP_DB "fmap.db"

// we keep P(t), x, and K the same for whole file
// system, so two equivalent files would have the same
// breakmarks. for string A, fingerprint of A is
//
//   f(A) = A(t) mod P(t) 
//
// we create breakmarks when 
//
//   f(A) mod K = x
//
// we use K = 4096 so that average chunk size is 4k.

#define FINGERPRINT_PT     0xbfe6b8a5bf378d83LL
#define BREAKMARK_X        0x78
#define BREAKMARK_K        4096
#define BREAKMARK_CHUNK_SZ 64


struct lbfs_chunk {
  char path[PATH_MAX];
  off_t pos;
  size_t size;

  lbfs_chunk() {}
  lbfs_chunk(const char *p, off_t o, size_t s) {
    strncpy(path, p, PATH_MAX);
    pos = o;
    size = s;
  }
};

inline bool
operator ==(const lbfs_chunk &a, const lbfs_chunk &b)
{
  return 
    ((strncmp(a.path, b.path, PATH_MAX) == 0) && 
     a.pos == b.pos && 
     a.size == b.size);
}


class lbfs_db {
public:
  lbfs_db(const char *name);
  ~lbfs_db();

  // open db, returns db3 errnos
  int open(); 

  // search db, given fingerprint, writes chunk info, return db3 errnos
  int search(u_int64_t fingerprint, lbfs_chunk *c);

  // returns db3 errnos
  int add_chunk(u_int64_t fingerprint, lbfs_chunk *c);
  int remove_chunk(u_int64_t fingerprint, lbfs_chunk *c);

private: 
  Db _dbp;
  const char* _name;

};


#endif _LBFS_DB_

