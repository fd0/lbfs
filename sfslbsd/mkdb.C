
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
static lbfs_db *_db = 0;
static int _requests = 0;

void
gotfh(const char *path, const nfs_fh3 *fhp, str err)
{
  if (!err) {
    for(int i=0; i<64; i++)
      printf ("%d.", (int)((char*)fhp)[i]);
    printf("\n");
      
    const u_char *fp;
    size_t fl; 
    mapfile (&fp, &fl, path); 
    for (unsigned j = 0; j < NUM_CHUNK_SIZES; j++) {
      vec<lbfs_chunk *> cv;
      if (chunk_data(path, fhp, CHUNK_SIZES(j), fp, fl, &cv) == 0) { 
	printf("%s: %d, %d chunks\n", path, CHUNK_SIZES(j), cv.size()); 
	for(unsigned i=0; i<cv.size(); i++) { 
	  _db->add_chunk(cv[i]->fingerprint, &(cv[i]->where)); 
	  delete cv[i]; 
	}
      }
    }
    munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);

  } else {
    warn << path << ": " << err << "!!!\n";
  }
  delete path;
  _requests--;
  if (_requests == 0) {
    delete _db;
    exit(0);
  }
}

int 
add_directory(const char *path)
{
  DIR *dirp;
  if ((dirp = opendir (path)) == 0) {
    printf("%s is not a directory\n", path);
    return -1;
  }

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
 
  _db = new lbfs_db();
  _db->open();
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

