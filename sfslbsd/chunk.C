
// usage: chunk path
//
// chunk all files under path, create chunk statistics
//
// for example
//
// ./chunk /usr/lib

#include <sys/types.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include "fingerprint.h"

#define NBUCKETS (MAX_CHUNK_SIZE>>7)
unsigned buckets[NBUCKETS];

void done()
{
  extern unsigned min_size_chunks;
  extern unsigned max_size_chunks;
  printf("# %u min size chunks\n", min_size_chunks);
  printf("# %u max size chunks\n", max_size_chunks);
#if 0
  for (int i=0; i<NBUCKETS; i++) {
    printf("%d %d\n", i<<7, buckets[i]);
  }
#endif
}

void
chunk_file(const char *path)
{
  int fd = open(path, O_RDONLY);
  unsigned char buf[4096];
  unsigned count;
  Chunker chunker(8192);
  while ((count = read(fd, buf, 4096))>0)
    chunker.chunk(buf, count);
  chunker.stop();
  for (unsigned i=0; i<chunker.chunk_vector().size(); i++) {
    lbfs_chunk *c = chunker.chunk_vector()[i];
    // warn << path << " " <<  c->fingerprint << " @" << c->loc.pos() << "\n";
    buckets[c->loc.count()>>7]++;
  }
  close(fd);
  warn << path << " " << chunker.chunk_vector().size() << " chunks\n";
}

void 
read_directory(const char *dpath)
{
  DIR *dirp;
  if ((dirp = opendir (dpath)) == 0) return;
  struct dirent *de = NULL;
  while ((de = readdir (dirp))) {
    struct stat sb;
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    char path[PATH_MAX];
    sprintf(path, "%s/%s", dpath, de->d_name);
    if (S_ISDIR(sb.st_mode))
      read_directory(path);
    else
      chunk_file(path);
  }
  closedir(dirp);
  done();
}

int 
main(int argc, char *argv[]) 
{
  if (argc != 2) {
    printf("usage: %s path\n", argv[0]);
    return -1;
  }
  for (int i=0; i<NBUCKETS; i++) buckets[i] = 0;
  read_directory(argv[1]);
}

