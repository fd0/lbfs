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
#include "ihash.h"
#include "nfstrans.h"

#define DEBUG 0

#define MAX_FH 65535 //4000
//#define MAX_FH MAXHANDLE //xfs constant for max file handles opened at any time
//#define MAXPATHLEN (1024+4) //This is what arla used...

bool xfs_fheq(xfs_handle, xfs_handle);
bool nfs_fheq(nfs_fh3, nfs_fh3);
str setcache_name(uint32 index);
nfstime3 max(nfstime3 mtime, nfstime3 ctime);

template<>
struct hashfn<xfs_handle> {
  hash_t operator() (const xfs_handle &xh) const
    { return xh.a; }
};

template<>
struct equals<xfs_handle> {
  bool operator() (const xfs_handle &a, const xfs_handle &b) const {
    return xfs_handle_eq (&a, &b);
  }
};


typedef struct cache_entry{
  static u_int64_t nextxh;

  xfs_handle xh;
  nfs_fh3 nh;
  ex_fattr3 nfs_attr;
  nfstime3 ltime;
  str cache_name;
  bool incache;
  uint32 writers;
  ihash_entry<cache_entry> nlink;
  ihash_entry<cache_entry> xlink;

  cache_entry (nfs_fh3 &n, ex_fattr3 &na);
  ~cache_entry ();
} cache_entry;

extern ihash<nfs_fh3, cache_entry, &cache_entry::nh,
  &cache_entry::nlink> nfsindex;
extern ihash<xfs_handle, cache_entry, &cache_entry::xh,
  &cache_entry::xlink> xfsindex;

inline
cache_entry::cache_entry (nfs_fh3 &n, ex_fattr3 &na)
  : nh (n), nfs_attr(na), incache(false), writers(0)
{
  bzero (&xh, sizeof (xh));
  xh.a = ++nextxh;
  xh.b = nextxh >> 32;
  cache_name = setcache_name(xh.a); //check if NULL??
  ltime = max(nfs_attr.mtime, nfs_attr.ctime);
  nfsindex.insert (this);
  xfsindex.insert (this);
}


inline
cache_entry::~cache_entry ()
{
  nfsindex.remove (this);
  xfsindex.remove (this);
}

#if 0
class fh_map {


  cache_entry entry[MAX_FH];
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
      entry[i].cache_name = new char[MAXPATHLEN];
      entry[i].opened = false;
    }
    //move this part to cache soon
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
      if (/*entry[i].opened && */xfs_fheq(entry[i].xh, x))
	return i;
    return -1;
  }

  int find(nfs_fh3 n) {
    for (int i=0; i<=max_fh; i++) 
      if (/*entry[i].opened && */nfs_fheq(entry[i].nh, n)) 
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
  //void setcache_name(const char* c) { strcpy(entry[cur_fh].cache_name, c); }
  
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

  char *getcache_name() { return entry[cur_fh].cache_name; }
  bool opened() { return entry[cur_fh].opened; }

  void remove(nfs_fh3 n) {
    int i = find(n);
    if (i > -1) {
      entry[i].xh.a = 0;
      entry[i].xh.b = 0;
      entry[i].xh.c = 0;
      entry[i].xh.d = 0;
      //delete entry[i].nh;
      entry[i].nh = nfs_fh3();
      delete entry[i].cache_name;
      //delete cache file
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
      //delete entry[i].nh;
      entry[i].nh = nfs_fh3();
      delete entry[i].cache_name;
#if 0      
      //delete cache file
      if (unlink(entry[i].cache_name) < 0)
	warn << strerror(errno) << "\n";
#endif
      entry[i].opened = false;
    }
  }

  xfs_handle gethandle(nfs_fh3 nfh, ex_fattr3 attr) {
    // if reaching end of max_fh, need to signal invalid node to xfs
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
      entry[max_fh].xh = xfh;
      entry[max_fh].nh = nfh;
      entry[max_fh].nfs_attr = attr;
      entry[max_fh].ltime = max(entry[max_fh].nfs_attr.mtime, entry[max_fh].nfs_attr.ctime);
      setcache_name(entry[max_fh].cache_name, max_fh);
      cur_fh = max_fh;
      return xfh;
    }
  }
  
};
#endif

#endif /* _FHMAP_H_ */
