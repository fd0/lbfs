
#include <sys/types.h>
#include <stdio.h>
#include <db.h>
#include <dirent.h>
#include <unistd.h>

#include "chunking.h"
#include "lbfsdb.h"

//
// recursively add a directory to database
//
int 
add_directory(const char *path, lbfs_db &db)
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
    char fullpath[PATH_MAX];
    snprintf(fullpath, PATH_MAX, "%s/%s", path, de->d_name);
    stat(fullpath, &sb);
    if (S_ISDIR(sb.st_mode))
      add_directory(fullpath, db);
    else if (S_ISREG(sb.st_mode)) {
      const u_char *fp;
      size_t fl;
      mapfile (&fp, &fl, fullpath);
      for (unsigned j = 0; j < NUM_CHUNK_SIZES; j++) {
        vec<lbfs_chunk *> cv;
        if (chunk_data(fullpath, CHUNK_SIZES(j), fp, fl, &cv) == 0) {
	  // printf("%s: %d, %d chunks\n", fullpath, CHUNK_SIZES(j), cv.size());
          for(unsigned i=0; i<cv.size(); i++) {
            db.add_chunk(cv[i]->fingerprint, cv[i]);
            delete cv[i];
          }
        }
      }
      munmap(static_cast<void*>(const_cast<u_char*>(fp)), fl);
    }
  }

  closedir(dirp);
  return 0;
}

int 
main(int argc, char *argv[]) 
{
  if (argc != 2) {
    printf("usage: %s directory\n", argv[0]);
    return -1;
  }

  lbfs_db db(FMAP_DB);
  db.open();

  return 
    add_directory(argv[1], db);
}

