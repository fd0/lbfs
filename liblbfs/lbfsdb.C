
#include <sys/types.h>
#include <stdio.h>

#include "lbfsdb.h"
#include "rabinpoly.h"

lbfs_db::lbfs_db()
  : _fp_dbp(0L,0)
{
}

lbfs_db::~lbfs_db()
{
  _fp_dbp.close(0);
}

int 
lbfs_db::open()
{
  int ret;
  _fp_dbp.set_flags(DB_DUP | DB_DUPSORT);

  if ((ret = _fp_dbp.open(FP_DB, NULL, DB_BTREE, DB_CREATE, 0664)) != 0) { 
    _fp_dbp.err(ret, "%s", FP_DB); 
    return ret;
  }
  
  return 0;
}

int
lbfs_db::get_chunk_iterator(u_int64_t fingerprint, 
                            lbfs_db::chunk_iterator **iterp)
{
  Dbc *cursor;
  if (_fp_dbp.cursor(NULL, &cursor, 0) == 0) { 
    Dbt key(&fingerprint, sizeof(fingerprint)); 
    Dbt data;
    if (cursor->get(&key, &data, DB_SET) == 0) {
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
  Dbt key(&f, sizeof(f));
  Dbt data(c, sizeof(*c));
  return _fp_dbp.put(NULL, &key, &data, 0);
}

