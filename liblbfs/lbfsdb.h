#ifndef _LBFS_DB_
#define _LBFS_DB_

#include "vec.h"
#include "async.h"
#include "nfs3_prot.h"
#include "db_cxx.h"

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

#define FINGERPRINT_PT     0xbfe6b8a5bf378d83LL
#define BREAKMARK_VALUE    0x78
#define NUM_CHUNK_SIZES    4
#define CHUNK_SIZES(i) \
  (i == 0 ? 8192 : \
   (i == 1 ? 32768 : \
    (i == 2 ? 131072 : \
     (i == 3 ? 524288 : 0))))

#define FP_DB "fp.db"

struct lbfs_chunk_loc {
  nfs_fh3 fh;		// 8 bytes
  nfstime3 mtime;	// 8 bytes
  off_t pos;		// 8 bytes
  ssize_t size; 	// 4 bytes
  unsigned unused;	// 4 bytes	
  
  lbfs_chunk_loc &operator= (const lbfs_chunk_loc &l) {
    memmove(&fh, &l.fh, sizeof(fh));
    mtime = l.mtime;
    pos = l.pos;
    size = l.size;
    return *this;
  }
};



struct lbfs_chunk {
  u_int64_t fingerprint;
  struct lbfs_chunk_loc loc;
 
  lbfs_chunk() {}
  lbfs_chunk(off_t p, ssize_t s, u_int64_t fp) {
    loc.pos = p;
    loc.size = s;
    fingerprint = fp;
  }
};

class lbfs_db {
private:
  Db _fp_dbp;

public:

  class chunk_iterator {
    friend lbfs_db;
  private:
    Dbc* _cursor;
    chunk_iterator(Dbc *c);

  public:
    ~chunk_iterator();
    operator bool() const { return _cursor != 0; }

    // get current entry
    int get(lbfs_chunk_loc *c);
    // increment iterator, return entry
    int next(lbfs_chunk_loc *c);
  };

  lbfs_db();
  ~lbfs_db();

  // open db, returns db3 errnos
  int open(); 

  // creates an iterator and copies a ptr to it into the memory referenced by
  // iterp (callee responsible for freeing iterp). additionally, iterator is
  // moved under the data for the given key, if any.
  int get_chunk_iterator(u_int64_t fingerprint, chunk_iterator **iterp);

  // returns db3 errnos
  int add_chunk(u_int64_t fingerprint, lbfs_chunk_loc *c);

  // XXX - garbage collect in db
  int gc();

};

inline
lbfs_db::chunk_iterator::chunk_iterator(Dbc *c)
{
  _cursor = c;
}

inline
lbfs_db::chunk_iterator::~chunk_iterator()
{ 
  if (_cursor) 
    _cursor->close();
  _cursor = 0L;
}

inline int
lbfs_db::chunk_iterator::get(lbfs_chunk_loc *c)
{
  assert(_cursor);
  Dbt key;
  Dbt data;
  int ret = _cursor->get(&key, &data, DB_CURRENT);
  if (ret == 0 && data.get_data()) {
    lbfs_chunk_loc *loc = reinterpret_cast<lbfs_chunk_loc*>(data.get_data());
    *c = *loc;
  }
  return ret;
}

inline int
lbfs_db::chunk_iterator::next(lbfs_chunk_loc *c)
{
  assert(_cursor);
  Dbt key;
  Dbt data;
  int ret = _cursor->get(&key, &data, DB_NEXT_DUP);
  if (ret == 0 && data.get_data())
    *c = *(reinterpret_cast<lbfs_chunk_loc*>(data.get_data()));
  return ret;
}


#endif _LBFS_DB_

