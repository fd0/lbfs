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
#include "xfs-nfs.h"

typedef struct fh_pair{
  xfs_handle xh;
  nfs_fh3 nh;
  ex_fattr3 nfs_attr;
} fh_pair;

class fh_map {
  fh_pair entry[MAXHANDLE]; //xfs constant for max file handles opened at any time
  int max_fh, cur_fh;

 public:

  fh_map() {
    max_fh = -1;
    cur_fh = -1;
    for (int i=0; i<MAXHANDLE; i++) {
      entry[i].xh.a = 0;
      entry[i].xh.b = 0;
      entry[i].xh.c = 0;
      entry[i].xh.d = 0;
    }
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
  
  xfs_handle getxh(int i) { return entry[i].xh; }
  nfs_fh3 getnh(int i) { return entry[i].nh; }
  ex_fattr3 getattr(int i) {return entry[i].nfs_attr; }

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

  xfs_handle gethandle(nfs_fh3 nfh, ex_fattr3 attr) {
    cur_fh = find(nfh);
    if (cur_fh > -1)
      return entry[cur_fh].xh;
    else {
      xfs_handle xfh;
      xfh.a = 0; xfh.b = 0; xfh.c = 0;
      xfh.d = entry[max_fh].xh.d + 1;
      max_fh = (++max_fh % MAXHANDLE);
      entry[max_fh].xh = xfh;
      entry[max_fh].nh = nfh;
      entry[max_fh].nfs_attr = attr;
      cur_fh = max_fh;
      return xfh;
    }
  }

};

#endif /* _FHMAP_H_ */
