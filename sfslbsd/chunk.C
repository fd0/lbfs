
// usage: chunk path search_db create_db
//
// chunk all files under path, create chunk statistics
//
// for example
//
// ./chunk /usr/lib fp.db tmp.db

#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include "fingerprint.h"
#include "lbfsdb.h"

#define NBUCKETS ((MAX_CHUNK_SIZE+1)>>7)
unsigned buckets[NBUCKETS];
unsigned totalchunks = 0;
unsigned totalfiles = 0;
uint64 totalsize = 0;
uint64 searchtime = 0;
uint64 inserttime = 0;
uint64 chunktime = 0;
uint64 rabintime = 0;
uint64 sha1time = 0;
fp_db sdb;
fp_db cdb;

struct timeval t0;
struct timeval t1;
inline unsigned timediff() {
  return (t1.tv_sec*1000000+t1.tv_usec)-(t0.tv_sec*1000000+t0.tv_usec);
}

void done()
{
  unsigned median = 0;
  unsigned m = totalchunks/2;
  for (int i=0; i<NBUCKETS; i++) {
    if (buckets[i] >= m) {
      median = i<<7;
      break;
    }
    m -= buckets[i];
  }

  printf("# %u files, %u chunks\n", totalfiles, totalchunks);
  if (totalchunks != 0) {
    printf("# chunk size average %qu, median %u\n", 
	   totalsize/totalchunks, median);
    printf("# average insert time: %qu usec\n", inserttime/totalchunks);
    printf("# average search time: %qu usec\n", searchtime/totalchunks);
    printf("# chunk time: %qu usec/chunk, %qu usec/Kbyte\n",
	   chunktime/totalchunks, chunktime/(totalsize/1024));
    printf("# sha1 time: %qu usec/chunk, %qu usec/Kbyte\n",
	   sha1time/totalchunks, sha1time/(totalsize/1024));
    printf("# rabin time: %qu usec/Kbyte\n", rabintime/(totalsize/1024));
    printf("# %u min size supprssed\n", Chunker::min_size_suppress);
    printf("# %u max size supprssed\n", Chunker::max_size_suppress);
#if 0
    for (int i=0; i<NBUCKETS; i++)
      printf("%d %d\n", i<<7, buckets[i]);
#endif
  }
}

void
chunk_file(const char *path)
{
  int fd = open(path, O_RDONLY);
  unsigned char buf[8192];
  int count;
  Chunker chunker;
  while ((count = read(fd, buf, 8192))>0) {
    gettimeofday(&t0,0L);
    chunker.chunk_data(buf, count);
    gettimeofday(&t1,0L);
    chunktime += timediff();
    
    {
      u_int64_t poly = FINGERPRINT_PT;
      window w (poly);
      w.reset();
      u_int64_t fp = 0;
      gettimeofday(&t0,0L);
      for (int i = 0; i < count; i++)
        fp = w.append8 (fp, buf[i]);
      gettimeofday(&t1,0L);
      rabintime += timediff();
    }
    
    gettimeofday(&t0,0L);
    unsigned char h[20];
    sha1_hash(h, buf, count);
    gettimeofday(&t1,0L);
    sha1time += timediff();
  }
  chunker.stop();
  for (unsigned i=0; i<chunker.chunk_vector().size(); i++) {
    chunk *c = chunker.chunk_vector()[i];
    totalsize += c->location().count();
    buckets[(c->location().count())>>7]++;
    fp_db::iterator *iter = 0;
    gettimeofday(&t0,0L);
    sdb.get_iterator(c->hashidx(), &iter);
    gettimeofday(&t1,0L);
    searchtime += timediff();
    if (iter) {
      int cnt = 0;
      chunk_location ctmp;
      while(!iter->get(&ctmp)) {
	iter->next(&ctmp);
	cnt++;
      }
      warn << path << ": " << c->hashidx() << ": " << cnt << "\n";
      delete iter;
    }
    gettimeofday(&t0,0L);
    cdb.add_entry(c->hashidx(), &c->location(), c->location().size());
    gettimeofday(&t1,0L);
    inserttime += timediff();
  }
  close(fd);
  totalchunks += chunker.chunk_vector().size();
  totalfiles++;
#if 0
  warn << path << " " << chunker.chunk_vector().size() << " chunks\n";
#endif
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
  if (argc != 4) {
    printf("usage: %s path search_db create_db\n", argv[0]);
    return -1;
  }
  sdb.open(argv[2]);
  cdb.open_and_truncate(argv[3]);
  for (int i=0; i<NBUCKETS; i++) buckets[i] = 0;
  read_directory(argv[1]);
  done();
}

