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

#ifndef _XFS_NFS_H
#define _XFS_NFS_H

#include <xfs/xfs_message.h>

bool xfs_fheq(xfs_handle x1, xfs_handle x2) {
  if (xfs_handle_eq(&x1, &x2)) 
    return true;
  else return false;
}

bool nfs_fheq(nfs_fh3 n1, nfs_fh3 n2) {
  if (memcmp(n1.data.base(), n2.data.base(), 
	     n1.data.size()) == 0)
    return true;
  else return false;
}

#endif /* _XFS_NFS_H */
