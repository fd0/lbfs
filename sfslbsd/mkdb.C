
#include <sys/types.h>
#include <stdio.h>
#include <db.h>
#include <dirent.h>
#include <unistd.h>

#include "fingerprint.h"
#include "lbfsdb.h"
#include "sfsrwsd.h"

ptr<aclnt> _mountc;
ptr<aclnt> _c;

static const char *_host;
static const char *_root;
static int _totalfns = 0;
static int _requests = 0;
static lbfs_db _db;
static nfs_fh3 _rootfh;

void done()
{
  if (_requests > 0) 
    _requests--;
  if (_requests == 0) {
    printf("%d files\n", _totalfns);
    exit(0);
  }
}

void
gotattr(char *path, const nfs_fh3 *fhp, const FATTR3 *attr, str err)
{
  if (!err) {
    const u_char *fp;
    size_t fl; 
    if (mapfile (&fp, &fl, path) == 0) {
      for (unsigned j = 0; j < NUM_CHUNK_SIZES; j++) {
        vec<lbfs_chunk *> cv;
        if (chunk_data(CHUNK_SIZES(j), fp, fl, &cv) == 0) { 
	  for(unsigned i=0; i<cv.size(); i++) { 
	    cv[i]->loc.set_fh(*fhp);
	    cv[i]->loc.set_mtime(attr->mtime);
#if 0
            printf("add %s fp 0x%016qx size %d\n", 
	           path, cv[i]->fingerprint, cv[i]->loc.count());
#endif
	    _db.add_chunk(cv[i]->fingerprint, &(cv[i]->loc)); 
	    delete cv[i];
	  }
        }
        if (cv.size()==1) break;
      }
      munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
    }
  }
  delete path;
  done();
}

int 
add_directory(const char *path)
{
  char tmp[PATH_MAX];
  snprintf(tmp, PATH_MAX, "%s/%s", _root, path);

  DIR *dirp;
  if ((dirp = opendir (tmp)) == 0)
    return -1;

  struct dirent *de = NULL;
  while ((de = readdir (dirp))) { 
    struct stat sb;
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;

    snprintf(tmp, PATH_MAX, "%s/%s", path, de->d_name);
    char *fullpath = new char [PATH_MAX];
    snprintf(fullpath, PATH_MAX, "%s/%s", _root, tmp);
    
    stat(fullpath, &sb);
    if (S_ISDIR(sb.st_mode)) 
      add_directory(tmp);
    else if (S_ISREG(sb.st_mode)) {
      _requests++;
      _totalfns++;
      lookupfh3(_c, _rootfh, tmp, wrap(gotattr, fullpath));
    }
  }

  closedir(dirp);
  return 0;
}

void
gotrootfh(const nfs_fh3 *fhp, str err)
{
  if (!err) {
    _rootfh = *fhp;
    _db.open(); 
    add_directory("");
  }
  else
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
  getfh3(_mountc, _root, wrap(gotrootfh));
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
    printf("usage: %s host nfs_root_path\n", argv[0]);
    return -1;
  }
  _host = argv[1];
  _root = argv[2];
  aclntudp_create (_host, 0, nfs_program_3, wrap(getnfsc));
  amain();
}

