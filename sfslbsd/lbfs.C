
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfsdb.h"

int
lbfs_search_reusable_chunks(vec<lbfs_chunk *> &new_chunks,
                            vec<lbfs_chunk *> &reusable_chunks)
{
  if (new_chunks.size() == 0)
    return -1;

  lbfs_db db(FMAP_DB);
  db.open();

  reusable_chunks.setsize(new_chunks.size());
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    lbfs_chunk *c = new lbfs_chunk;
    if (!db.search(new_chunks[i]->fingerprint, c))
      reusable_chunks[i] = c;
    else {
      reusable_chunks[i] = 0L;
      delete c;
    }
  }
  return 0;
}

int
lbfs_load_reusable_chunks(int new_fd, 
                          vec<lbfs_chunk *> &new_chunks,
                          vec<lbfs_chunk *> &reusable_chunks)
{
  int failures = 0;
  for (unsigned i=0; i<new_chunks.size(); i++) {
    if (reusable_chunks[i]) {
      lbfs_chunk *c = reusable_chunks[i];
      assert(c->size == new_chunks[i]->size);
      int fd = open(c->path, O_RDONLY);
      if (fd < 0 || lseek(fd, c->pos, SEEK_SET) != c->pos) {
        failures++;
	delete c;
	reusable_chunks[i] = 0;
      }
      else {
	char *buf = new char[c->size];
        if (read(fd, buf, c->size) != c->size) {
	  failures++;
	  delete c;
	  reusable_chunks[i] = 0;
	}
	else {
	  lseek(new_fd, new_chunks[i]->pos, SEEK_SET);
          write(new_fd, buf, c->size);
	}
        delete buf;
      }
      close(fd);
    }
  }
  return failures;
}

int
lbfs_add_file(const char *path, vec<lbfs_chunk *> &new_chunks)
{
  vec<lbfs_chunk *> old_chunks;
  chunk_file(path, &old_chunks);
  
  lbfs_db db(FMAP_DB);
  db.open();

  for (unsigned i = 0; i < old_chunks.size(); i++) { 
    if (db.remove_chunk(old_chunks[i]->fingerprint, old_chunks[i]) != 0) 
      printf("db inconsistent: old chunk for %s does not exist!\n", path);
    delete old_chunks[i];
  }

  for (unsigned i = 0; i < new_chunks.size(); i++) { 
    lbfs_chunk c(path, new_chunks[i]->pos, 
	         new_chunks[i]->size, new_chunks[i]->fingerprint);
    int ret = db.add_chunk(new_chunks[i]->fingerprint, &c);
    assert(ret == 0);
  }

  return 0;
}


