
#include <sys/types.h>
#include <stdio.h>

#include "lbfsdb.h"
#include "rabinpoly.h"

lbfs_db::lbfs_db(const char *name)
  : _dbp(0L,0)
{
  _name = 0;
}

lbfs_db::~lbfs_db()
{
  _dbp.close(0);
}

int 
lbfs_db::open()
{
  int ret;
  _dbp.set_flags(DB_DUP | DB_DUPSORT);
  if ((ret = _dbp.open(FMAP_DB, NULL, DB_BTREE, DB_CREATE, 0664)) != 0) { 
    _dbp.err(ret, "%s", FMAP_DB); 
    return ret;
  }
  return 0;
}

int
lbfs_db::search(u_int64_t fingerprint, lbfs_chunk *cp)
{
  Dbt key(&fingerprint, sizeof(fingerprint));
  Dbt data;

  int ret;
  if ((ret = _dbp.get(NULL, &key, &data, 0)) == 0)
    *cp = *reinterpret_cast<lbfs_chunk*>(data.get_data());
  return ret;
}

int
lbfs_db::add_chunk(u_int64_t f, lbfs_chunk *c)
{
  assert(c);
  Dbt key(&f, sizeof(f));
  Dbt data(c, sizeof(*c));
  return _dbp.put(NULL, &key, &data, 0);
}

int
lbfs_db::remove_chunk(u_int64_t f, lbfs_chunk *c)
{
  Dbc *cursor;
  int ret;

  if ((ret = _dbp.cursor(NULL, &cursor, 0)) != 0)
    return ret;

  assert(c);
  Dbt key(&f, sizeof(f));
  Dbt data;
    
  ret = cursor->get(&key, &data, DB_SET);

  while (ret == 0 && data.get_data() 
         && !(*(reinterpret_cast<lbfs_chunk*>(data.get_data()))==*c))
    ret = cursor->get(&key, &data, DB_NEXT_DUP);
  
  if (data.get_data() && 
      *(reinterpret_cast<lbfs_chunk*>(data.get_data())) == *c)
    ret = cursor->del(0);
  return ret;
}

