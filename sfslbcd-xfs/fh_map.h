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

typedef struct {
  char* path_name;
  xfs_handle xh;
  nfs_fh3 nh;
  ex_fattr3 nfs_attr;
  nfstime3 ltime;
  union {
    char *cache_name; //[MAXPATHLEN];
    char *path_name;  //[MAXPATHLEN];
  } cloc;
  bool opened;
} fh_pair;

class fh_map { //File handle map to cache_name
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
      //entry[i].cache_name = new char[MAXPATHLEN];
      entry[i].opened = false;
    }

    if (int fd = open("cache", O_RDONLY, 0666) < 0) {
      if (errno == ENOENT) {
	warn << "Creating dir: cache\n";
	if (mkdir("cache", 0777) < 0) {
	  warn << strerror(errno) << "(" << errno << ") mkdir cache\n";
	}
      } else {
	warn << strerror(errno) << "(" << errno << ") mkdir cache\n";
      }
    } else close(fd);
  }

  int getcur() {return cur_fh;}
  int getmax() {return max_fh;}

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

  int find(char *pname) {
    for (int i=0; i<=max_fh; i++) 
      if (!strcmp(entry[i].path_name, pname)) 
	return i;
    return -1;
  }

  int setcur(xfs_handle xfh) {
    int fh = find(xfh);
    if (fh > -1) {
      cur_fh = fh;
      return 0;
    } else return -1;
  }

  int setcur(nfs_fh3 nfh) {
    int fh = find(nfh);
    if (fh > -1) {
      cur_fh = fh;
      return 0;
    } else return -1;
  }

  
  void setopened(bool b) { entry[cur_fh].opened = b; }
  bool opened() { return entry[cur_fh].opened; }
  
  void setpath_name(char *pname) { strcpy(entry[cur_fh].path_name, pname); }
  char *getpath_name() { return entry[cur_fh].path_name; }

  xfs_handle getxh(int i) { return entry[i].xh; }
  nfs_fh3 getnh(int i) { return entry[i].nh; }
  void set_nfsattr(ex_fattr3 nattr) { entry[cur_fh].nfs_attr = nattr; }
  ex_fattr3 get_nfsattr() {return entry[cur_fh].nfs_attr; }

  nfstime3 max(nfstime3 mtime, nfstime3 ctime) {
    if (mtime.seconds > ctime.seconds)
      return mtime;
    else 
      if (mtime.seconds < ctime.seconds) 
	return ctime;
      else 
	if (mtime.nseconds > ctime.nseconds)
	  return mtime;
	else return ctime;
  }

  void set_ltime(nfstime3 a, nfstime3 b) { entry[cur_fh].ltime = max(a,b); }
  nfstime3 get_ltime() { return entry[cur_fh].ltime; }

  char *getcache_loc(int index) {
    if (index < 0 || index > max_fh)
      return NULL;
    if (entry[index].nfs_attr.type == NF3REG || entry[index].nfs_attr.type == NF3DIR)
      return entry[index].cloc.cache_name; 
    else 
      if (entry[index].nfs_attr.type == NF3LNK)
	return getcache_loc(find(entry[index].cloc.path_name));
      else return NULL;
  }

  char *getcache_loc() { 
    if (entry[cur_fh].nfs_attr.type == NF3REG || entry[cur_fh].nfs_attr.type == NF3DIR)
      return entry[cur_fh].cloc.cache_name; 
    else 
      if (entry[cur_fh].nfs_attr.type == NF3LNK)
	return getcache_loc(find(entry[cur_fh].cloc.path_name));
      else return NULL;
  }

  int assign_dirname(char *dname, int index) {
    return snprintf(dname, MAXPATHLEN, "cache/%02X", index / 0x100);
  }
  
  int assign_filename(char *fname, int index) {
    return snprintf(fname, MAXPATHLEN, "cache/%02X/%02X", 
		    index / 0x100, index % 0x100);
  }

  void setcache_name(char *fname, int index) { 
    
    int fd;

    assign_filename(fname, index);
    fd = open(fname, O_CREAT | O_RDWR | O_TRUNC, 0666); 
    if (fd < 0) { 
      if (errno == ENOENT) {
	char *dname = new char[MAXPATHLEN];
	assign_dirname(dname, index);
	warn << "Creating dir: " << dname << "\n";
	if (mkdir(dname, 0777) < 0) {
	  warn << strerror(errno) << "(" << errno << ") mkdir " << dname << "\n";
	  return;
	}
	fd = open(fname, O_CREAT | O_RDWR | O_TRUNC, 0666); 
	if (fd < 0) {
	  warn << strerror(errno) << "(" << errno << ") on file =" << fname << "\n";
	  return;
	}
      } else {
	warn << strerror(errno) << "(" << errno << ") on file =" << fname << "\n";
	return;
      }
    }
    
    close(fd);
  }

  void setcache_loc(char *clname) {
    if (entry[cur_fh].nfs_attr.type == NF3REG || entry[cur_fh].nfs_attr.type == NF3DIR)
      setcache_name(entry[cur_fh].cloc.cache_name, cur_fh);
    else 
      if (entry[cur_fh].nfs_attr.type == NF3LNK)
	strcpy(entry[cur_fh].cloc.path_name, clname);
  }

  xfs_handle gethandle(nfs_fh3 nfh, ex_fattr3 attr) { //, char *pname, char *clname) {
    // if reaching end of max_fh, need to signal invalid node to xfs
    // This function also serves as a way to add new entries to the table.
    cur_fh = find(nfh);
    if (cur_fh > -1) {
      entry[cur_fh].nfs_attr = attr;
      return entry[cur_fh].xh;
    }
    else {
      xfs_handle xfh;
      xfh.a = 0; xfh.b = 0; xfh.c = 0;
      xfh.d = entry[max_fh].xh.d + 1;
      max_fh = (++max_fh % MAX_FH);
      cur_fh = max_fh;
      entry[max_fh].xh = xfh;
      entry[max_fh].nh = nfh;
      entry[max_fh].nfs_attr = attr;
      entry[max_fh].ltime = max(entry[max_fh].nfs_attr.mtime, entry[max_fh].nfs_attr.ctime);
      //setpath_name(pname);
      //setcache_loc(clname);
      return xfh;
    }
  }

  void remove(nfs_fh3 n) {
    int i = find(n);
    if (i > -1) {
      entry[i].xh.a = 0;
      entry[i].xh.b = 0;
      entry[i].xh.c = 0;
      entry[i].xh.d = 0;
      entry[i].opened = false;
    }
  }
  
  void remove(xfs_handle x) {
    int i = find(x);
    if (i > -1) {
      entry[i].xh.a = 0;
      entry[i].xh.b = 0;
      entry[i].xh.c = 0;
      entry[i].xh.d = 0;
      entry[i].opened = false;
    }
  }

};

extern fh_map fht;

#endif /* _FHMAP_H_ */
