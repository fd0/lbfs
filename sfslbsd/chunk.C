
// usage: chunk path
//
// chunk all files under path, create chunk statistics
//
// for example
//
// ./chunk /usr/lib

#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include "fingerprint.h"
#include "lbfsdb.h"

#define NBUCKETS 256
unsigned buckets[NBUCKETS];
unsigned total_chunks = 0;
fp_db db;

void done()
{
  extern unsigned min_size_chunks;
  extern unsigned max_size_chunks;
  printf("# %u total chunks\n", total_chunks);
  printf("# %u min size chunks\n", min_size_chunks);
  printf("# %u max size chunks\n", max_size_chunks);
#if 0
  for (int i=0; i<NBUCKETS; i++) {
    printf("%d %d\n", i, buckets[i]);
  }
#endif
}

void
chunk_file(const char *path)
{
  int fd = open(path, O_RDONLY);
  unsigned char buf[4096];
  int count;
  Chunker chunker(8192);
  while ((count = read(fd, buf, 4096))>0)
    chunker.chunk(buf, count);
  chunker.stop();
  int matches = 0;
  for (unsigned i=0; i<chunker.chunk_vector().size(); i++) {
    lbfs_chunk *c = chunker.chunk_vector()[i];
    fp_db::iterator *iter = 0;
    if (db.get_iterator(c->fingerprint, &iter) == 0) {
      iter->next(0);
      while(*iter) {
	matches++;
	iter->next(0);
      }
    }
    delete iter;
  }
  close(fd);
  total_chunks += chunker.chunk_vector().size();
  warn << path << " " << chunker.chunk_vector().size() << " chunks, "
       << matches << " matches\n";
}

void 
read_directory(const char *dpath)
{
  DIR *dirp;
  if ((dirp = opendir (dpath)) == 0) return;
  struct dirent *de = NULL;
  while ((de = readdir (dirp))) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    char path[PATH_MAX];
    sprintf(path, "%s/%s", dpath, de->d_name);
    struct stat sb;
    lstat(path, &sb);
    if (S_ISDIR(sb.st_mode))
      read_directory(path);
    else if (S_ISREG(sb.st_mode) && !S_ISLNK(sb.st_mode))
      chunk_file(path);
  }
  closedir(dirp);
}

int 
main(int argc, char *argv[]) 
{
  if (argc != 3) {
    printf("usage: %s path db\n", argv[0]);
    return -1;
  }
  db.open(argv[2]);
  for (int i=0; i<NBUCKETS; i++) buckets[i] = 0;
  read_directory(argv[1]);
}

