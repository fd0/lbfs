/*
 *
 * Copyright (C) 2000 Athicha Muthitacharoen (athicha@mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#ifndef _FHMAP_H_
#define _FHMAP_H_

#include <xfs/xfs_message.h>
#include "nfs3exp_prot.h"
#include "xfs-nfs.h"

#define MAX_FH 4000
//#define MAX_FH MAXHANDLE //xfs constant for max file handles opened at any time
//#define MAXPATHLEN (1024+4) //This is what arla used...

extern bool xfs_fheq(xfs_handle, xfs_handle);
extern bool nfs_fheq(nfs_fh3, nfs_fh3);

#if 0
int assign_dirname(char *, int);
int assign_filename(char *, int);
int assign_cachefile(char *, int);
#endif

typedef struct fh_pair{
  xfs_handle xh;
  nfs_fh3 nh;
  ex_fattr3 nfs_attr;
  char *cache_name;
  bool opened;
} fh_pair;

class fh_map {
  fh_pair entry[MAX_FH];
  int max_fh, cur_fh;

 public:

  fh_map() {
    max_fh = -1;
    cur_fh = -1;
    for (int i=0; i<MAX_FH; i++) {
      entry[i].xh.a = 0;
      entry[i].xh.b = 0;
      entry[i].xh.c = 0;
      entry[i].xh.d = 0;
      entry[i].opened = false;
    }
    //move this part to cache soon
    if (int fd = open("cache", O_RDONLY, 0666) < 0) {
      if (errno == ENOENT) {
	warn << "Creating dir: cache\n";
	if (mkdir("cache", 0777) < 0) {
	  warn << strerror(errno) << "(" << errno << ") mkdir cache\n";
	  //return -1;
	}
      } else {
	warn << strerror(errno) << "(" << errno << ") mkdir cache\n";
	//return -1;
      }
    } else close(fd);
  }

  int getcur() {return cur_fh;}
  int getmax() {return max_fh;}

  int setcur(xfs_handle xfh) {
    int fh = find(xfh);
    if (fh > -1) {
      cur_fh = fh;
      return 0;
    } else return -1;
  }

  void setopened(bool b) { entry[cur_fh].opened = b; }
  void setcache_name(const char* c) { strcpy(entry[cur_fh].cache_name, c); }
  
  xfs_handle getxh(int i) { return entry[i].xh; }
  nfs_fh3 getnh(int i) { return entry[i].nh; }
  ex_fattr3 getattr(int i) {return entry[i].nfs_attr; }
  char *getcache_name() { return entry[cur_fh].cache_name; }
  bool opened() { return entry[cur_fh].opened; }

  int find(xfs_handle x) {
    for (int i=0; i<=max_fh; i++)
      if (xfs_fheq(entry[i].xh, x))
	return i;
    return -1;
  }

  int find(nfs_fh3 n) {
    for (int i=0; i<=max_fh; i++) 
      if (nfs_fheq(entry[i].nh, n)) 
	return i;
    return -1;
  }

  void remove(nfs_fh3 n) {
    int i = find(n);
    if (i > -1) {
      entry[i].xh.a = 0;
      entry[i].xh.b = 0;
      entry[i].xh.c = 0;
      entry[i].xh.d = 0;
      //entry[i].nh = ...
      entry[i].opened = false;
    }
  }

  xfs_handle gethandle(nfs_fh3 nfh, ex_fattr3 attr) {
    // if reaching end of max_fh, need to signal invalid node to xfs
    cur_fh = find(nfh);
    if (cur_fh > -1)
      return entry[cur_fh].xh;
    else {
      xfs_handle xfh;
      xfh.a = 0; xfh.b = 0; xfh.c = 0;
      xfh.d = entry[max_fh].xh.d + 1;
      max_fh = (++max_fh % MAX_FH);
      entry[max_fh].xh = xfh;
      entry[max_fh].nh = nfh;
      entry[max_fh].nfs_attr = attr;
      cur_fh = max_fh;
      return xfh;
    }
  }
  
};

#endif /* _FHMAP_H_ */
