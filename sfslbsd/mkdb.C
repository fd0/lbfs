
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

#define MAKE_FH_DB 0

ptr<aclnt> _mountc;
ptr<aclnt> _c;

static const char *_host;
static const char *_mntp;

static fp_db _fp_db;
static fh_db _fh_db;
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

#if MAKE_FH_DB
    fh_rep fh0(*fhp);
    fh_db::iterator *iter0 = 0;
    _fh_db.get_iterator(fh0, &iter0);
    lbfs_chunk c;
    if (!iter0) 
      warn << armor32(fhp->data.base(), fhp->data.size()) << "\n";
    while (iter0 && !iter0->get(&c)) {
      nfs_fh3 fh;
      c.loc.get_fh(fh);
#if 0
      warn << armor32(fh.data.base(), fh.data.size()) << " @"
	   << c.loc.pos() << " " << c.loc.count() << " "
	   << c.fingerprint << "\n";
#endif
      iter0->del();
      iter0->next(&c);
    }
    delete iter0;
#endif

    const u_char *fp;
    size_t fl; 
    if (mapfile (&fp, &fl, fspath) == 0) {
#if 0
      for (unsigned j = 0; j < NUM_CHUNK_SIZES; j++) {
#endif
      for (unsigned j = 0; j < 1; j++) {
        vec<lbfs_chunk *> cv;
        chunk_data(CHUNK_SIZES(j), &cv, fp, fl);
	for(unsigned i=0; i<cv.size(); i++) { 
	  cv[i]->loc.set_fh(*fhp);
	  _fp_db.add_entry(cv[i]->fingerprint, &(cv[i]->loc));
#if MAKE_FH_DB
	  fh_rep fh(*fhp);
	  cv[i]->mtime = attr->mtime;
	  _fh_db.add_entry(fh, cv[i]);
#endif
	  delete cv[i];
	}
        if (cv.size()==1) break;
      }
      munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
#if MAKE_FH_DB
      _fh_db.sync();
#endif
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
#if MAKE_FH_DB
    _fh_db.open(FH_DB); 
#endif
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

