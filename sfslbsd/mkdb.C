
// usage: mkdb host nfs_mount_point
//
// creates a LBFS database for files under "nfs_mount_point". chunk files
// using posix fd interface, but connect to nfs server on "host" to retrieve
// file handles. if database already exists, add to the database instead.
//
// for example
//
// ./mkdb localhost /disk/pw0/benjie/play
//

#include <sys/types.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>

#include "fingerprint.h"
#include "lbfsdb.h"
#include "sfsrwsd.h"

ptr<aclnt> _mountc;
ptr<aclnt> _c;

static const char *_host;
static const char *_mntp;

static fp_db _fp_db;
static nfs_fh3 _rootfh;

static int _totalfns = 0;
static int _requests = 0;

static int read_directory(const char *dpath, DIR *dirp = 0L);

void done()
{
  if (_requests == 0) {
    printf("%d files\n", _totalfns);
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

    const u_char *fp;
    size_t fl; 
    if (mapfile (&fp, &fl, fspath) == 0) {
      for (unsigned j = 0; j < NUM_CHUNK_SIZES; j++) {
        vec<lbfs_chunk *> cv;
        chunk_data(CHUNK_SIZES(j), &cv, fp, fl);
	for(unsigned i=0; i<cv.size(); i++) { 
	  cv[i]->loc.set_fh(*fhp);
	  _fp_db.add_entry(cv[i]->fingerprint, &(cv[i]->loc));
	  delete cv[i];
	}
        if (cv.size()==1) break;
      }
      munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
      _fp_db.sync();
    }
  }
  else 
    warn << "nfs: " << dpath << "/" << fname << ": " << err << "\n";
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
  while ((de = readdir (dirp))) {
    struct stat sb;
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
      
    snprintf(fspath, PATH_MAX, "%s/%s/%s", _mntp, dpath, de->d_name);
    stat(fspath, &sb);
      
    char newpath[PATH_MAX];
    snprintf(newpath, PATH_MAX, "%s/%s", dpath, de->d_name);
    
    if (S_ISDIR(sb.st_mode))
      read_directory(newpath);
    
    else if (S_ISREG(sb.st_mode)) {
      _requests++;
      _totalfns++;
      
      char *dpath2 = New char[PATH_MAX];
      strncpy(dpath2, dpath, PATH_MAX);
      char *fname = New char[PATH_MAX];
      strncpy(fname, de->d_name, PATH_MAX);

      lookupfh3(_c, _rootfh, newpath, wrap(gotattr, dpath2, fname, dirp));
      return 0;
    }
  }
  closedir(dirp);
  done();
  return 0;
}

void
gotrootfh(const nfs_fh3 *fhp, str err)
{
  if (!err) {
    _rootfh = *fhp;
    _fp_db.open(FP_DB); 
    read_directory("");
  }
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
  if (argc != 3) {
    printf("usage: %s host mount_point\n", argv[0]);
    return -1;
  }

  _host = argv[1];
  _mntp = argv[2];

  aclntudp_create (_host, 0, nfs_program_3, wrap(getnfsc));
  amain();
}

