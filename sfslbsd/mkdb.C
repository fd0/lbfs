
#include <sys/types.h>
#include <stdio.h>
#include <db.h>
#include <dirent.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfsdb.h"
#include "sfsrwsd.h"

ptr<aclnt> _mountc;
ptr<aclnt> _c;

static const char *_host;
static const char *_root;
static int _totalfns = 0;
static int _requests = 0;
static lbfs_db _db;

void
gotfh(const char *path, const nfs_fh3 *fhp, str err)
{
  if (!err) {
#if 0
    for(int i=0; i<64; i++)
      printf ("%d.", (int)((char*)fhp)[i]);
    printf("\n");
#endif  
    const u_char *fp;
    size_t fl; 
    mapfile (&fp, &fl, path); 
    for (unsigned j = 0; j < NUM_CHUNK_SIZES; j++) {
      vec<lbfs_chunk *> cv;
      if (chunk_data(path, CHUNK_SIZES(j), fp, fl, &cv) == 0) { 
	for(unsigned i=0; i<cv.size(); i++) { 
	  cv[i]->loc.fh = *fhp;
          printf("add %s 0x%016qx %d\n", 
	         path, cv[i]->fingerprint, cv[i]->loc.size);
	  _db.add_chunk(cv[i]->fingerprint, &(cv[i]->loc)); 
	  delete cv[i]; 
	}
      }
      if (cv.size()==1) break;
    }
    munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
  }
  delete path;
  _requests--;
  if (_requests == 0) {
    printf("%d files\n", _totalfns);
    exit(0);
  }
}

int 
add_directory(const char *path)
{
  DIR *dirp;
  if ((dirp = opendir (path)) == 0)
    return -1;

  struct dirent *de = NULL;
  while ((de = readdir (dirp))) { 
    struct stat sb;
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
      continue;
    char *fullpath = new char[PATH_MAX];
    snprintf(fullpath, PATH_MAX, "%s/%s", path, de->d_name);
    stat(fullpath, &sb);

    if (S_ISDIR(sb.st_mode)) 
      add_directory(fullpath);
    else if (S_ISREG(sb.st_mode)) {
      _requests++;
      _totalfns++;
      getfh3(_mountc, fullpath, wrap(gotfh, fullpath));
    }
  }

  closedir(dirp);
  return 0;
}


void
getmountc(ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << _host << ": MOUNT3: " << stat << "\n";
    return;
  }
  _mountc = nc;
 
  _db.open();
  add_directory(_root);
}


void
getnfsc(ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << _host << ": NFS3: " << stat << "\n";
    return;
  }
  _c = nc;
  aclntudp_create (_host, 0, mount_program_3, wrap(&getmountc));
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
  aclntudp_create (_host, 0, nfs_program_3, wrap(&getnfsc));
  amain();
}

