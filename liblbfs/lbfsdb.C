
#include <sys/types.h>
#include <stdio.h>

#include "lbfsdb.h"
#include "rabinpoly.h"

lbfs_db::lbfs_db()
  : _fp_dbp(0L)
{
}

lbfs_db::~lbfs_db()
{
  if (_fp_dbp) 
    _fp_dbp->close(_fp_dbp, 0);
}

int 
lbfs_db::open()
{
  int ret;
  if ((ret = db_create(&_fp_dbp, NULL, 0)) != 0) { 
    fprintf(stderr, "db_create: %s\n", db_strerror(ret)); 
    exit (1); 
  } 
  _fp_dbp->set_flags(_fp_dbp, DB_DUP | DB_DUPSORT);

  if ((ret = _fp_dbp->open
	(_fp_dbp, FP_DB, NULL, DB_BTREE, DB_CREATE, 0664)) != 0) { 
    _fp_dbp->err(_fp_dbp, ret, "%s", FP_DB); 
    return ret;
  }
  return 0;
}

int
lbfs_db::get_chunk_iterator(u_int64_t fingerprint, 
                            lbfs_db::chunk_iterator **iterp)
{
  DBC *cursor;
  if (_fp_dbp->cursor(_fp_dbp, NULL, &cursor, 0) == 0) { 
    DBT key;
    memset(&key, 0, sizeof(key));
    key.data = reinterpret_cast<void *>(&fingerprint);
    key.size = sizeof(fingerprint);
    DBT data;
    memset(&data, 0, sizeof(data));
    if (cursor->c_get(cursor, &key, &data, DB_SET) == 0) {
      *iterp = new chunk_iterator(cursor);
      return 0;
    }
  } 
  return -1;
}

int
lbfs_db::add_chunk(u_int64_t f, lbfs_chunk_loc *c)
{
  assert(c);
  DBT key;
  memset(&key, 0, sizeof(key));
  key.data = reinterpret_cast<void *>(&f);
  key.size = sizeof(f);
  DBT data;
  memset(&data, 0, sizeof(data));
  data.data = reinterpret_cast<void *>(c);
  data.size = sizeof(*c);
  return _fp_dbp->put(_fp_dbp, NULL, &key, &data, 0);
}

