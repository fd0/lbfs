
#include <sys/types.h>
#include <stdio.h>
#include <db.h>
#include <dirent.h>
#include <unistd.h>

#include "rabinpoly.h"
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
      vec<u_int64_t> fv;
      vec<lbfs_chunk *> cv;
      if (chunk_file(fullpath, &fv, &cv) == 0) {
        // printf("adding %d for %s\n", fv.size(), fullpath);
        for(unsigned i=0; i<fv.size(); i++) {
          db.add_chunk(fv[i], cv[i]);
          delete cv[i];
        }
      }
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

