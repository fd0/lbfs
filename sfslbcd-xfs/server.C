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

void sfs_getfsinfo (xfscall *);
void nfs3_fsinfo (xfscall *);
void nfs3_getattr (xfscall *);

void nfs_dispatch (xfscall *xfsc, clnt_stat err) {

  assert(!err);

  switch (xfsc->opcode) {
  case XFS_MSG_GETROOT: 
    switch (xfsc->instance) {
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
    break;

  default:
#if DEBUG < 0
    warn << "Uncovered case .. opcode = " << xfsc->opcode << "\n";
#endif
    break;
  }

}

int
xfs_wakeup (xfscall *xfsc) {

#if DEBUG > 0
  warn << "Received wakeup from XFS\n";
#endif
  return 0;
}

int 
xfs_getroot (xfscall *xfsc) {

#if DEBUG > 0
  warn << "Received getroot from XFS\n";
#endif

  ((xfs_getroot_args*) xfsc->getvoidres ())->fsi = New sfs_fsinfo;
  sfsc->call (SFSPROC_GETFSINFO, NULL, 
	      ((xfs_getroot_args*) xfsc->getvoidres ())->fsi,
	      wrap (nfs_dispatch, xfsc));
  return 0;

}

int 
xfs_getnode (xfscall *xfsc) {

  return 0;
}

int 
xfs_getattr (xfscall *xfsc) {

  return 0;
}

int xfs_getdata (xfscall *xfsc) {

  return 0;
}

int xfs_inactivenode (xfscall *xfsc) {

  return 0;
}

int xfs_open (xfscall *xfsc) {

  return 0;
}

int xfs_putdata (xfscall *xfsc) {

  return 0;
}

int xfs_putattr (xfscall *xfsc) {

  return 0;
}

int xfs_create (xfscall *xfsc) {

  return 0;
}

int xfs_mkdir (xfscall *xfsc) {

  return 0;
}

int xfs_link (xfscall *xfsc) {

  return 0;
}

int xfs_symlink (xfscall *xfsc) {

  return 0;
}

int xfs_remove (xfscall *xfsc) {

  return 0;
}

int xfs_rmdir (xfscall *xfsc) {

  return 0;
}

int xfs_rename (xfscall *xfsc) {

  return 0;
}

int xfs_pioctl (xfscall *xfsc) {

  return 0;
}

void cbdispatch(svccb *sbp) {

}
