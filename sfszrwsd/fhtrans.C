/* $Id$ */

/*
 *
 * Copyright (C) 1998 David Mazieres (dm@uun.org)
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

/*
 * NFS <-> SFS filehandle translation
 *
 * An NFS file handle has around 8 32-bit words which always include:
 *
 *   - A filesystem ID (which contains the device number)
 *   - The inode number of the file
 *   - The generation number of the file
 *
 * In addition, implementations often include some of the following:
 *
 *   - A second filesystem ID word (for a 64-bit fsid)
 *   - The length of the filehandle data
 *   - The inode number of the export point (to prevent lookup ("..") there)
 *   - The generation number of the export point
 *   - Another copy of the second filesystem ID word (for the export point?)
 *   - one or more unused 0 words
 *
 * The key thing to note here is that all but two of the words (namely
 * the inode number and generation number of the file) are constant
 * for the entire entire filesystem if we just export the root.  We
 * therefore pull the inode and generation number out of the file
 * handle, but store the rest in a lookup table to shorten file
 * handles.
 *
 * The final thing to note is that because of NFS's abysmal security,
 * one inode, generation number pair of any directory may be all it
 * takes to access all files on a filesystem one does not have mount
 * permissions on.  For that reason, the SFS filehandles are encrypted
 * using blowfish in CBC mode before being shipped to the remote
 * system.  This should also prevent malicious clients from guessing
 * the filehandles of files they have read permission on in
 * directories they do not have execute permission on.
 */

#include "sfsrwsd.h"
#include "ihash.h"
#include "blowfish.h"

#ifdef __linux__
# define FHINO_POS1 0
# define FHINO_POS2 1
#else /* default */
# define FHINO_POS1 3
# define FHINO_POS2 4
#endif /* default */

#ifndef FHINO_POS3
# define FHINO_POS3 FHINO_POS1
# define FHINO_POS4 FHINO_POS2
#elif !defined (FHINO_POS4)
# define FHINO_POS4 FHINO_POS1
#endif /* FHINO_POS3 && !FHINO_POS4 */

static inline u_int32_t
extract32 (const char *base, size_t bytes, size_t pos)
{
  if (pos + 4 <= bytes)
    return *reinterpret_cast<const u_int32_t *> (base);

  base += 4 * pos;
  switch (bytes - 4 * pos) {
  case 3:
    return base[0] << 24 | base[1] << 16 | base[2] << 8;
  case 2:
    return base[0] << 24 | base[1] << 16;
  case 1:
    return base[0] << 24;
  default:
    return 0;
  }
}

struct fhccmp {
  fhccmp () {}
  hash_t operator() (const nfs_fh3 &fh) const {
    u_int res = 0;
    for (int n = fh.data.size () + 3 / 4; n-- > 0;)
      if (n != FHINO_POS1 && n != FHINO_POS2
	  && n != FHINO_POS3 && n != FHINO_POS4)
	res ^= extract32 (fh.data.base (), fh.data.size (), n);
    return res;
  }
  bool operator() (const nfs_fh3 &a, const nfs_fh3 &b) const {
    size_t size = a.data.size ();
    if (b.data.size () != size)
      return false;
    for (int n = size + 3 / 4; n-- > 0;)
      if (n != FHINO_POS1 && n != FHINO_POS2
	  && n != FHINO_POS3 && n != FHINO_POS4
	  && (extract32 (a.data.base (), a.data.size (), n)
	      != extract32 (b.data.base (), b.data.size (), n)))
	return false;
    return true;
  }
};

struct fhclass {
  nfs_fh3 fhproto;
  ihash_entry<fhclass> link;
  size_t pos () const;
};

static vec<fhclass> fhcvec;
static ihash<nfs_fh3, fhclass, &fhclass::fhproto,
  &fhclass::link, fhccmp> fhctab;

inline size_t
fhclass::pos () const
{
  return this - fhcvec.base ();
}

int
fh3_compress (fh3trans &fht, nfs_fh3 *fhp)
{
  nfs_fh3 res;
  fhclass *fhc;
  u_int32_t *dp, *rp;

  switch (fht.mode) {
  case fh3trans::ENCODE:
    if (fhp->data.size () < 4 * (FHINO_POS3 + 1)
	|| fhp->data.size () > NFS3_FHSIZE - 4) {
      warn << "fh3_compress: cannot compress " << fhp->data.size ()
	   << "-byte file handle\n";
      return NFS3ERR_BADHANDLE;
    }

    fhc = fhctab[*fhp];
    if (!fhc) {
      fhc = &fhcvec.push_back ();
      fhc->fhproto.data.setsize (fhp->data.size ());
      memcpy (fhc->fhproto.data.base (), fhp->data.base (), fhp->data.size ());
      fhctab.insert (fhc);
    }
    res.data.setsize (20);
    dp = reinterpret_cast<u_int32_t *> (res.data.base ());
    dp[0] = extract32 (fhp->data.base (), fhp->data.size (), FHINO_POS1);
    dp[1] = extract32 (fhp->data.base (), fhp->data.size (), FHINO_POS2);
    dp[2] = extract32 (fhp->data.base (), fhp->data.size (), FHINO_POS3);
    dp[3] = extract32 (fhp->data.base (), fhp->data.size (), FHINO_POS4);
    dp[4] = fhc->pos ();
    *fhp = res;
    break;
  case fh3trans::DECODE:
    if (fhp->data.size () != 16) {
      warn << "fh3_compress: cannot uncompress " << fhp->data.size ()
	   << "-byte file handle\n";
      return NFS3ERR_BADHANDLE;
    }
    dp = reinterpret_cast<u_int32_t *> (fhp->data.base ());
    if (dp[4] >= fhcvec.size ()) {
      warn << "fh3_compress: bad file handle\n";
      return NFS3ERR_BADHANDLE;
    }
    res = fhcvec[dp[4]].fhproto;
    rp = reinterpret_cast<u_int32_t *> (res.data.base ());
    rp[FHINO_POS1] = dp[0];
    rp[FHINO_POS2] = dp[1];
    rp[FHINO_POS3] = dp[2];
    rp[FHINO_POS4] = dp[3];
    if (rp[FHINO_POS1] != dp[0] || rp[FHINO_POS2] != dp[1]
	|| rp[FHINO_POS3] != dp[2]) {
      warn << "fh3_compress: bad file handle\n";
      return NFS3ERR_BADHANDLE;
    }
    *fhp = res;
    break;
  }
  return 0;
}
