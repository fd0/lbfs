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

#include "xfs.h"
#include "xfs-sfs.h"
#include "fh_map.h"
#include "crypt.h"

ex_fsinfo3resok nfs_fsinfo;
u_int64_t cache_entry::nextxh;
ihash<nfs_fh3, cache_entry, &cache_entry::nh,
  &cache_entry::nlink> nfsindex;
ihash<xfs_handle, cache_entry, &cache_entry::xh,
  &cache_entry::xlink> xfsindex;

void sfs_getfsinfo (ref<xfscall>);
void nfs3_fsinfo (ref<xfscall>);
void nfs3_getattr (ref<xfscall>);

void 
nfs_dispatch (ref<xfscall> xfsc, time_t rqtime, clnt_stat err) {

  assert(!err);

  xfsc->rqtime = rqtime;
  switch (xfsc->opcode) {
  case XFS_MSG_GETROOT: 
    switch (xfsc->inst) {
    case 0:
      sfs_getfsinfo (xfsc);
      break;
    case 1:
      nfs3_fsinfo (xfsc);
      break;
    case 2:
      nfs3_getattr (xfsc);
      break;
    }
  case XFS_MSG_GETNODE:
    break;
  default:
#if DEBUG < 0
    warn << "Uncovered case .. opcode = " << xfsc->opcode << "\n";
#endif
    break;
  }

}

int
xfs_wakeup (ref<xfscall> xfsc) {

#if DEBUG > 0
  warn << "Received wakeup from XFS\n";
#endif
  return 0;
}

int 
xfs_getroot (ref<xfscall> xfsc) {

#if DEBUG > 0
  warn << "Received getroot from XFS\n";
#endif
  
  getroot (sfsc, nfsc);

#if 0
  sfs_fsinfo *fsi = New sfs_fsinfo;
  xfsc->resp[++xfsc->inst] = fsi;
  sfsc->call (SFSPROC_GETFSINFO, NULL, fsi,
	      wrap (nfs_dispatch, xfsc, timenow));
#endif 

  return 0;
}

int 
xfs_getnode (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_getattr (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_getdata (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_inactivenode (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_open (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_putdata (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_putattr (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_create (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_mkdir (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_link (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_symlink (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_remove (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_rmdir (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_rename (ref<xfscall> xfsc) {

  return 0;
}

int 
xfs_pioctl (ref<xfscall> xfsc) {

  return 0;
}

void 
cbdispatch(svccb *sbp) {

#if DEBUG > 0
  warn << "cbdispatch triggered\n";
#endif
  if (!sbp)
    return;

  switch (sbp->proc ()) {
  case ex_NFSCBPROC3_NULL:
    sbp->reply (NULL);
    break;
  case ex_NFSCBPROC3_INVALIDATE:
    {
      ex_invalidate3args *xa = sbp->template getarg < ex_invalidate3args > ();
      ex_fattr3 *a = NULL;
      if (xa->attributes.present && xa->attributes.attributes->expire) {
	a = xa->attributes.attributes.addr ();
	a->expire += timenow;
	cache_entry *e = nfsindex[xa->handle];
	if (!e) {
	  warn << "cbdispatch: Can't find handle\n";
	  return;
	}
	e->nfs_attr = *a;
      }
      //delete a; should we delete this?
      sbp->reply (NULL);
      break;
    }
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
