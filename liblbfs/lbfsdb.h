#ifndef _LBFS_DB_
#define _LBFS_DB_

#ifdef HAVE_DB3_H
#include <db3.h>
#else /* !HAVE_DB3_H */
#include <db.h>
#endif /* !HAVE_DB3_H */

template<class K, class V> class db_base {
private:
  DB *_dbp;

public:
  class iterator {
    friend db_base;
  private:
    DBC* _cursor;
    iterator(DBC *c) {
      _cursor = c;
    }
    void done() {
      if (_cursor) 
        _cursor->c_close(_cursor);
      _cursor = 0L;
    }

  public:
    ~iterator() { done(); }

    operator bool() const { return _cursor != 0; }
    int del() { return _cursor->c_del(_cursor, 0); }

    // get current entry
    int get(V *c) {
      if (!_cursor) 
	return -1;
      DBT key;
      DBT data;
      memset(&key, 0, sizeof(key));
      memset(&data, 0, sizeof(data));
      int ret = _cursor->c_get(_cursor, &key, &data, DB_CURRENT);
      if (ret == 0)
        *c = *(reinterpret_cast<V*>(data.data));
      else 
	done();
      return ret;
    }
    
    // increment iterator, get that entry
    int next(V *c) {
      if (!_cursor) 
	return -1;
      DBT key;
      DBT data;
      memset(&key, 0, sizeof(key));
      memset(&data, 0, sizeof(data));
      int ret = _cursor->c_get(_cursor, &key, &data, DB_NEXT_DUP);
      if (ret == 0) {
        if (c) *c = *(reinterpret_cast<V*>(data.data));
      } else
	done();
      return ret;
    }
  };

  db_base();
  ~db_base();

  // open db, returns db3 errnos
  int open(const char *name, u_int32_t db3_flags = DB_CREATE); 

  // open and truncate existing db
  int open_and_truncate(const char *name);

  // creates an iterator and copies a ptr to it into the memory
  // referenced by iterp (callee responsible for freeing iterp). additionally,
  // iterator is moved under the data for the given key, if any.
  int get_iterator(K key, iterator **iterp);

  // add an entry to the database, returns db3 errnos
  int add_entry(K key, V *val, int size = sizeof(V));

  // sync data to stable storage
  int sync();
};

template<class K, class V>
inline int
db_base<K,V>::sync()
{
  return _dbp->sync(_dbp,0);
}

template<class K, class V>
inline 
db_base<K,V>::db_base()
  : _dbp(0L)
{
}

template<class K, class V>
inline 
db_base<K,V>::~db_base()
{
  if (_dbp) 
    _dbp->close(_dbp, 0);
}

template<class K, class V>
inline int 
db_base<K,V>::open(const char *name, u_int32_t db3_flags)
{
  int ret;
  if ((ret = db_create(&_dbp, NULL, 0)) != 0) { 
    fprintf(stderr, "db_create: %s\n", db_strerror(ret)); 
    exit (1); 
  } 
  _dbp->set_flags(_dbp, DB_DUP);

  if ((ret = _dbp->open
	(_dbp, name, NULL, DB_BTREE, db3_flags, 0664)) != 0) { 
    _dbp->err(_dbp, ret, "%s", name); 
    return ret;
  }
  return 0;
}

template<class K, class V>
inline int
db_base<K,V>::open_and_truncate(const char *name)
{
  return open(name, DB_CREATE | DB_TRUNCATE);
}

template<class K, class V>
inline int
db_base<K,V>::get_iterator(K k, db_base::iterator **iterp)
{
  DBC *cursor;
  if (_dbp->cursor(_dbp, NULL, &cursor, 0) == 0) { 
    DBT key;
    memset(&key, 0, sizeof(key));
    key.data = reinterpret_cast<void *>(&k);
    key.size = sizeof(k);
    DBT data;
    memset(&data, 0, sizeof(data));
    if (cursor->c_get(cursor, &key, &data, DB_SET) == 0) {
      *iterp = New iterator(cursor);
      return 0;
    }
  } 
  return -1;
}

template<class K, class V>
inline int
db_base<K,V>::add_entry(K k, V *v, int size)
{
  assert(v);
  DBT key;
  memset(&key, 0, sizeof(key));
  key.data = reinterpret_cast<void *>(&k);
  key.size = sizeof(k);
  DBT data;
  memset(&data, 0, sizeof(data));
  data.data = reinterpret_cast<void *>(v);
  data.size = size;
  return _dbp->put(_dbp, NULL, &key, &data, 0);
}

#include "chunk.h"

#define FP_DB "fp.db"
typedef db_base<u_int64_t, lbfs_chunk_loc> fp_db;

#endif _LBFS_DB_

