
// usage: mkdb dbname host nfs_mount_point
//
// creates a LBFS database for files under "nfs_mount_point". chunk files
// using posix fd interface, but connect to nfs server on "host" to retrieve
// file handles. if database already exists, add to the database instead.
//
// for example
//
// ./mkdb fp.db localhost /disk/pw0/benjie/play

#include <sys/types.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>

#include "fingerprint.h"
#include "lbfsdb.h"
#include "sfsrwsd.h"

ptr<aclnt> _mountc;
ptr<aclnt> _c;

static const char *_dbfn;
static const char *_host;
static const char *_mntp;

static fp_db _fp_db;
static nfs_fh3 _rootfh;

static int _totalfns = 0;
static int _requests = 0;

static int read_directory(const char *dpath, DIR *dirp = 0L);
#define NBUCKETS (MAX_CHUNK_SIZE>>7)
unsigned buckets[NBUCKETS];
unsigned total_chunks = 0;

void done()
{
  if (_requests == 0) {
    extern unsigned min_size_chunks;
    extern unsigned max_size_chunks;
    printf("# %d files\n", _totalfns);
    printf("# %u total chunks\n", total_chunks);
    printf("# %u min size chunks\n", min_size_chunks);
    printf("# %u max size chunks\n", max_size_chunks);
    for (int i=0; i<NBUCKETS; i++) {
      printf("%d %d\n", i<<7, buckets[i]);
    }
    exit(0);
  }
}

void
gotattr(const char *dpath, const char *fname, DIR *dirp,
        const nfs_fh3 *fhp, const FATTR3 *attr, str err)
{
  if (!err) {
    char fspath[PATH_MAX];
    sprintf(fspath, "%s/%s/%s", _mntp, dpath, fname);

    int fd = open(fspath, O_RDONLY);
    unsigned char buf[4096];
    unsigned count;
    Chunker chunker(8192);
    while ((count = read(fd, buf, 4096))>0)
      chunker.chunk(buf, count);
    chunker.stop();
    for (unsigned i=0; i<chunker.chunk_vector().size(); i++) {
      lbfs_chunk *c = chunker.chunk_vector()[i];
      c->loc.set_fh(*fhp);
      _fp_db.add_entry(c->fingerprint, &(c->loc));
      buckets[c->loc.count()>>7]++;
    }
    total_chunks += chunker.chunk_vector().size();
    close(fd);
    _fp_db.sync();
    warn << fspath << " " << chunker.chunk_vector().size() << " chunks\n";
  }
  if (_requests > 0) 
    _requests--;

  delete fname;
  read_directory(dpath, dirp);
  delete dpath;
}

int 
read_directory(const char *dpath, DIR *dirp = 0L)
{
  char fspath[PATH_MAX];
  if (dirp == 0L) {
    snprintf(fspath, PATH_MAX, "%s/%s", _mntp, dpath);
    if ((dirp = opendir (fspath)) == 0)
      return -1;
  }

  struct dirent *de = NULL;
  _requests++;
  while ((de = readdir (dirp))) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    snprintf(fspath, PATH_MAX, "%s/%s/%s", _mntp, dpath, de->d_name);
    char newpath[PATH_MAX];
    snprintf(newpath, PATH_MAX, "%s/%s", dpath, de->d_name);

    struct stat sb;
    stat(fspath, &sb);
    if (S_ISDIR(sb.st_mode))
      read_directory(newpath);
    else {
      _requests++;
      _totalfns++;
      char *dpath2 = New char[PATH_MAX];
      strncpy(dpath2, dpath, PATH_MAX);
      char *fname = New char[PATH_MAX];
      strncpy(fname, de->d_name, PATH_MAX);
      lookupfh3(_c, _rootfh, newpath, wrap(gotattr, dpath2, fname, dirp));
      _requests--;
      return 0;
    }
  }
  _requests--;
  closedir(dirp);
  done();
  return 0;
}

void
gotrootfh(const nfs_fh3 *fhp, str err)
{
  if (!err) {
    _rootfh = *fhp;
    _fp_db.open(_dbfn);
    read_directory("");
  }
  else
    warn << "get root fh: " << err << "\n";
  done();
}

void
getmountc(ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << _host << ": MOUNT3: " << stat << "\n";
    exit(-1);
  }
  _mountc = nc;
  getfh3(_mountc, _mntp, wrap(gotrootfh));
}


void
getnfsc(ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << _host << ": NFS3: " << stat << "\n";
    exit(-1);
  }
  _c = nc;
  aclntudp_create (_host, 0, mount_program_3, wrap(getmountc));
}


int 
main(int argc, char *argv[]) 
{
  if (argc != 4) {
    printf("usage: %s dbname host mount_point\n", argv[0]);
    return -1;
  }

  _dbfn = argv[1];
  _host = argv[2];
  _mntp = argv[3];

  for (int i=0; i<NBUCKETS; i++)
    buckets[i] = 0;

  aclntudp_create (_host, 0, nfs_program_3, wrap(getnfsc));
  amain();
}

