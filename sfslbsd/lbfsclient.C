/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@mit.edu)
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

#include "lbfsusrv.h"
#include "rxx.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/time.h>

#ifndef MAINTAINER
enum { dumptrace = 0 };
#else /* MAINTAINER */
const bool dumptrace (getenv ("SFSRO_TRACE"));
#endif /* MAINTAINER */

#define NFS_MAXDATA 8192
static char iobuf[NFS_MAXDATA];

struct {
	nfsstat3		error;
	int			errnum;
} nfs_errtbl[]= {
	{ NFS3ERR_PERM,		EPERM		},
	{ NFS3ERR_NOENT,	ENOENT		},
	{ NFS3ERR_IO,		EIO		},
	{ NFS3ERR_NXIO,		ENXIO		},
	{ NFS3ERR_ACCES,	EACCES		},
	{ NFS3ERR_EXIST,	EEXIST		},
	{ NFS3ERR_NODEV,	ENODEV		},
	{ NFS3ERR_NOTDIR,	ENOTDIR		},
	{ NFS3ERR_ISDIR,	EISDIR		},
	{ NFS3ERR_INVAL,	EINVAL		},
	{ NFS3ERR_FBIG,		EFBIG		},
	{ NFS3ERR_NOSPC,	ENOSPC		},
	{ NFS3ERR_ROFS,		EROFS		},
	{ NFS3ERR_NAMETOOLONG,	ENAMETOOLONG	},
	{ NFS3ERR_NOTEMPTY,	ENOTEMPTY	},
	{ NFS3ERR_STALE,	ESTALE		},
	{ NFS3ERR_SERVERFAULT,  EMFILE          }, // NFS3ERR_JUKEBOX XXXX
	{ NFS3ERR_SERVERFAULT,  ENFILE          }, // NFS3ERR_JUKEBOX XXXX
	{ NFS3_OK,		EIO		}
};

static nfsstat3
nfs_errno (void)
{
  for (int i = 0; nfs_errtbl[i].error != NFS3_OK; i++) {
    if (nfs_errtbl[i].errnum == errno) {
      return (nfs_errtbl[i].error);
    }
  }
  warnx << NFS3ERR_IO << " (don't know) \n";
  return (NFS3ERR_IO);
}

static void
trans_attr (ex_fattr3 *fa, struct stat *sb)
{
  ftype3 t;
  if (S_ISREG (sb->st_mode))
    t = NF3REG;
  else if (S_ISDIR (sb->st_mode))
    t = NF3DIR;
  else if (S_ISLNK (sb->st_mode))
    t = NF3LNK;
  else {
    t = ftype3 (0);
    warn << "Non-supported file type " << sb->st_mode << "\n";
  }

  fa->type = t;
  fa->mode = sb->st_mode & 0xFFF;
  fa->nlink = sb->st_nlink;
  fa->uid = sb->st_uid;
  fa->gid = sb->st_gid;
  fa->size = sb->st_size;
  fa->used = sb->st_blocks * 512;
  fa->rdev.major = 0;
  fa->rdev.minor = 0;
  fa->fsid = 0;
  fa->fileid = sb->st_ino;
  //  fa->fileid = (fa->fileid << 32) | sb->st_dev;
  fa->mtime.seconds = sb->st_mtime;
  fa->mtime.nseconds = 0;
  fa->ctime.seconds = sb->st_ctime;
  fa->ctime.nseconds = 0;
  fa->atime.seconds = sb->st_atime;
  fa->atime.nseconds = 0;
  fa->expire = 0;		// NFS semantics for for now.
}


fh_entry::fh_entry (str p, nfs_fh3 f, ex_fattr3 *a, filesrv *fs)
  : path (p), fh (f), fa (*a), fsrv (fs)
{ 
  fd = 0; 
  lastused = time (NULL); 
}

fh_entry::~fh_entry (void)
{ 
  assert (fd <= 0);
}

void
fh_entry::print (void)
{
  strbuf sb;
  rpc_print (sb, fh, 5, NULL, " ");

  warnx << path << " fileid " << fa.fileid << " fd: " << fd << " used " << lastused << " " << sb << "\n"; 
}

void
fh_entry::update_attr (int fd)
{
  struct stat sb;

  if (fstat (fd, &sb) != 0)
      warnx << "fstat failed; weird\n";
  trans_attr (&fa, &sb);
}


void
fh_entry::update_attr (str p)
{
  struct stat sb;

  if (stat (p, &sb) != 0)
      warnx << "fstat failed; weird\n";
  trans_attr (&fa, &sb);
}

int
fh_entry::closefd (void)
{
  if (fd > 0) {
    close (fd);
    fd = 0;
    return 1;
  }
  return 0;
}

filesrv::filesrv (void) 
{
  fhe_n = 0;
  fd_n = 0;
  fhetmo = delaycb (fhe_timer, wrap (this, &filesrv::fhetimeout));
}

void
filesrv::purgefhe (void)
{
  if (fhe_n < fhe_max) return; 

  time_t curtime = time (NULL);
  for (fh_entry *fhe = timeoutlist.first; fhe != 0; 
       fhe = timeoutlist.next (fhe)) {
    if (curtime > fhe->lastused + fhe_expire) {
      warnx << "purgefhe: delete old handle\n";
      remove (fhe);
    }
  }
}

void
filesrv::fhetimeout (void)
{
  purgefhe ();
  purgefd (0);
  fhetmo = delaycb (fhe_timer, wrap (this, &filesrv::fhetimeout));
}

void
filesrv::mk_fh (nfs_fh3 *fh, ex_fattr3 *fa)
{
  // compute a file handle XXX improve
  fh->data.setsize (10);
  bzero (fh->data.base (), 10);
  memcpy (fh->data.base (), &fa->fileid, sizeof (fa->fileid));
}

int
filesrv::closefd (fh_entry *fhe)
{
  assert (fd_n >= 0);
  if (fhe->closefd()) {
    fd_n--;
    return 1;
  } else
    return 0;
}

void
filesrv::purgefd (int force)
{
  time_t curtime = time (NULL);
  for (fh_entry *fhe = timeoutlist.first; fhe != 0; 
         fhe = timeoutlist.next(fhe)) {
    if (force || (curtime > fhe->lastused + fd_expire)) {
      (void ) closefd(fhe);

    }
  }
}

void 
filesrv::printfhe (void)
{
  fh_entry *fhe;

  warnx << "entries:\n";

  //  entries.traverse(&fh_entry::print);

  int i = 0;
  for (fhe = entries.first (); fhe != 0; 
         fhe = entries.next(fhe)) {
    i++;
    fhe->print();
  }

  warnx << "entries #entries: " << i << " timeoutlist:\n";

  i = 0;
  for (fhe = timeoutlist.first; fhe != 0; 
         fhe = timeoutlist.next(fhe)) {
    i++;
    // fhe->print();
  }
  warnx << "timeoutlist #entries: " << i << "\n";
}

int
filesrv::checkfd (void)
{
  if (fd_n < fd_max) 
    return 1;

  purgefd(1);

  if (fd_n < fd_max) {
    return 1;
  } else {
    warnx << "checkfd: ENFILE\n";
    errno = ENFILE;
    return 0;
  }
}

int
filesrv::getfd (fh_entry *fhe, int flags)
{
  // check permissions XXX
  assert (fhe);

  if (!checkfd ()) return -1;

  int fd = open (fhe->path, flags, 0);
  if (fd > 0) {
    fd_n++;
    fhe->setfd(fd);
  }
  return fd;
}

int
filesrv::getfd (str p, int flags, mode_t mode)
{
  if (!checkfd ()) return -1;

  // check permissions XXX
  int fd = open (p, flags, mode);
  if (fd > 0) fd_n++;
  return fd;
}


fh_entry *
filesrv::lookup (nfs_fh3 *fh) 
{ 
  fh_entry *fhe = entries[*fh];
  if (fhe) {
    timeoutlist.remove (fhe);
    fhe->lastused = time (NULL);
    timeoutlist.insert_tail (fhe);
  }
  return fhe;
}

int
filesrv::checkfhe (void)
{
  if (fhe_n < fhe_max)
    return 1;

  warnx << "checkfhe: out of fh entries\n";

  purgefhe();

  if (fhe_n < fhe_max) {
    return 1;
  } else {
    warnx << "checkfhe: EMFILE\n";
    errno = EMFILE;
    return 0;
  }
}

int
filesrv::lookup_attr (str p, ex_fattr3 *fa)
{
  struct stat sb;

  if (lstat (p, &sb) != 0) 
    return 0;
  trans_attr (fa, &sb);
  return 1;
}

fh_entry *
filesrv::lookup_add (str p)
{  
  ex_fattr3 fa;
  nfs_fh3 fh;

  if (!lookup_attr (p, &fa))
      return NULL;
  mk_fh (&fh, &fa);
  fh_entry *fhe = lookup (&fh);
  if (!fhe) {
    if (checkfhe ()) {
      fhe = New fh_entry (p, fh, &fa, this);
      entries.insert (fhe);
      timeoutlist.insert_tail (fhe);
      fhe_n++;
    } else {
      errno = EMFILE;
      return NULL;
    }
  }
  return fhe;
}

void
filesrv::remove (fh_entry *fhe)
{
  timeoutlist.remove (fhe);
  entries.remove (fhe);
  fhe_n--;
  closefd (fhe);
  delete fhe;
}

client::client (ref<axprt_crypt> x)
  : sfsserv (x)
{
  rwsrv = asrv::alloc (x, ex_nfs_program_3,
			wrap (this, &client::nfs3dispatch));
  //  nfscbc = aclnt::alloc (x, ex_nfscb_program_3);

  authid_valid = false;
}

bool
client::dirlookup (str dir, filename3 *name)
{
  DIR *dirp;
  struct dirent *de = NULL;

  if ((dirp = opendir (dir)) == 0) {
    warn << dir << " is not a directory\n";
    return 0;
  }

  while ((de = readdir (dirp))) {
      str n (de->d_name);
      if (n == *name) {
	closedir (dirp);
	return 1;
      }
  }
  closedir (dirp);
  errno = ENOENT;
  return 0;
}

void
client::nfs3_getattr (svccb *sbp)
{
  nfs_fh3 *fh = sbp->template getarg<nfs_fh3> ();
  fh_entry *fhe = fsrv->lookup (fh);

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  ex_getattr3res nfsres (NFS3_OK);
  *nfsres.attributes = fhe->fa;
  sbp->reply (&nfsres);
}

void
client::nfs3_fsinfo (svccb *sbp)
{
  ex_fsinfo3res res (NFS3_OK);
  res.resok->rtmax = 8192;
  res.resok->rtpref = 8192;
  res.resok->rtmult = 512;
  res.resok->wtmax = 8192;
  res.resok->wtpref = 8192;
  res.resok->wtmult = 8192;
  res.resok->dtpref = 8192;
  res.resok->maxfilesize = INT64 (0x7fffffffffffffff);
  res.resok->time_delta.seconds = 0;
  res.resok->time_delta.nseconds = 1;
  res.resok->properties = (FSF3_LINK | FSF3_SYMLINK | FSF3_HOMOGENEOUS);
  sbp->reply (&res);
}

void
client::nfs3_fsstat (svccb *sbp)
{
  ex_fsstat3res res (NFS3_OK);
  rpc_clear (res);
  sbp->reply (&res);
}

uint32
client::access_check(ex_fattr3 *fa, uint32 access_req)
{
  // XXX implement this.
  return access_req;
}

void
client::nfs3_access (svccb *sbp)
{
  access3args *aa = sbp->template getarg<access3args> ();
  uint32 access_req = aa->access;
  ex_access3res nfsres (NFS3_OK);
  fh_entry *fhe = fsrv->lookup (&aa->object);

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  nfsres.resok->access = access_check (&fhe->fa, access_req);
  nfsres.resok->obj_attributes.set_present (true);
  *nfsres.resok->obj_attributes.attributes = fhe->fa;
  sbp->reply (&nfsres);
}

void
client::nfs3_lookup (svccb *sbp)
{
  diropargs3 *dirop = sbp->template getarg<diropargs3> ();
  fh_entry *dhe = fsrv->lookup (&dirop->dir);

  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (!fsrv->checkfd ()) {		// dirlookup needs a fd
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (dirlookup (dhe->path, &(dirop->name))) {
    str p = dhe->path << "/" << dirop->name;
    fh_entry *fhe = fsrv->lookup_add (p);
    if (!fhe) {
      nfs3exp_err (sbp, nfs_errno());
      return;
    }
    ex_lookup3res nfsres (NFS3_OK); 
    nfsres.resok->object = fhe->fh;
    nfsres.resok->obj_attributes.set_present (true);
    *nfsres.resok->obj_attributes.attributes = fhe->fa;
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno());
  }
}

void
client::nfs3_readdir (svccb *sbp)
{
  readdir3args *readdirop = sbp->template getarg<readdir3args> ();
  fh_entry *dhe = fsrv->lookup (&readdirop->dir);
  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (!fsrv->checkfd ()) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  DIR *dirp;
  if ((dirp = opendir (dhe->path)) == 0) {
    warn << dhe->path << " is not a directory\n";
    nfs3exp_err (sbp, nfs_errno ());
    return;
  }

  struct dirent *de;
  uint64 i = 1;

  if(readdirop->cookie != 0){
    // The cookie is the index of the last file previously returned.
    // We use 1-origin cookies.
    // So skip over cookie files.
    for(i = 1; i <= readdirop->cookie; i++)
      readdir(dirp);
  }

  ex_readdir3res nfsres (NFS3_OK);
  nfsres.resok->reply.eof = true;
  rpc_ptr < entry3 > *direntp = &nfsres.resok->reply.entries;

  // Set up to approximate stopping after readdirop->count bytes.
  unsigned size_guess = 200; // XXX guess at size of fixed part of reply.

  while ((de = readdir (dirp))) {
      str n (de->d_name);
      str p = dhe->path << "/" << n;
      fh_entry *fhe = fsrv->lookup_add (p);
      if (fhe) {
        size_guess += n.len() + 32; // XXX
        if(size_guess > readdirop->count){
          nfsres.resok->reply.eof = false;
          break;
        }
        (*direntp).alloc ();
        (*direntp)->name = n;
        (*direntp)->fileid = fhe->fa.fileid;
        (*direntp)->cookie = i++;
        direntp = &(*direntp)->nextentry;
      } else {
        (void) closedir (dirp);
        nfs3exp_err (sbp, nfs_errno());
        return;
      }
  }
  (void) closedir (dirp);
  sbp->reply (&nfsres);
}

void
client::nfs3_read (svccb *sbp)
{
  read3args *ra = sbp->template getarg<read3args> ();
  fh_entry *fhe = fsrv->lookup (&ra->file);
  ex_read3res nfsres (NFS3_OK);
  int len;

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  int fd = fsrv->getfd (fhe, O_RDONLY);
  if (fd < 0) {
    nfs3exp_err (sbp, nfs_errno ());
    return;
  }

  len = -1;
  if (lseek (fd, (long) ra->offset, L_SET) >= 0) {
    if ((len = ra->count) > NFS_MAXDATA)
      len = NFS_MAXDATA;
    len = read(fd, iobuf, len);
  }
  if (len < 0) {
    nfs3exp_err (sbp, nfs_errno ());
  } else {
    nfsres.resok->count = len;
    nfsres.resok->data.setsize (len);
    memcpy (nfsres.resok->data.base (), iobuf, len);

    if (ra->offset + ra->count >= fhe->fa.size)
      nfsres.resok->eof = true;
    else
      nfsres.resok->eof = false;

    sbp->reply (&nfsres);
  }
}

void
client::nfs3_create (svccb *sbp)
{
  create3args *ca = sbp->template getarg<create3args> ();
  fh_entry *dhe = fsrv->lookup (&ca->where.dir);
  fh_entry *fhe;
  uint32 m = 0;
  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno ());
    return;
  }
  switch (ca->how.mode) {
  case UNCHECKED:
  case GUARDED:
    if (ca->how.obj_attributes->mode.set)
      m = *ca->how.obj_attributes->mode.val;
    break;
  case EXCLUSIVE:
    break;
  }
  str n = dhe->path << "/" << ca->where.name;
  int fd = fsrv->getfd(n, O_CREAT | O_TRUNC | O_RDWR, m);
  if (fd >= 0) {
    fhe = fsrv->lookup_add (n);
    dhe->update_attr (dhe->path);
    fhe->setfd (fd);
    ex_diropres3 nfsres (NFS3_OK);
    nfsres.resok->obj.set_present (true);
    *nfsres.resok->obj.handle = fhe->fh;
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_condwrite (svccb *sbp)
{
  lbfs_condwrite3args *wa = sbp->template getarg<lbfs_condwrite3args> ();
  fh_entry *fhe = fsrv->lookup (&wa->file);

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  lbfs_condwrite3res nfsres (NFS3_OK);
  sbp->reply (&nfsres);
}

void
client::nfs3_write (svccb *sbp)
{
  write3args *wa = sbp->template getarg<write3args> ();
  fh_entry *fhe = fsrv->lookup (&wa->file);
  int len;

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  int fd = fsrv->getfd (fhe, O_WRONLY);
  if (fd < 0) {
    nfs3exp_err (sbp, nfs_errno ());
    return;
  }

  len = -1;
  if (lseek (fd, (long) wa->offset, L_SET) >= 0) {
    len = write(fd, wa->data.base (), wa->count);
  }
  if (len < 0) {
    nfs3exp_err (sbp, nfs_errno ());
  } else {
    if (wa->stable == FILE_SYNC) {
      if (fsync (fd) != 0) 
	warnx << "fsync failed; weird\n";
    }
    fhe->update_attr (fd);
    ex_write3res nfsres (NFS3_OK);
    nfsres.resok->count = len;
    nfsres.resok->committed = wa->stable;
    sbp->reply (&nfsres);
  }
}

void
client::nfs3_commit (svccb *sbp)
{
  commit3args *ca = sbp->template getarg<commit3args> ();
  fh_entry *fhe = fsrv->lookup (&ca->file);

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  int fd = fsrv->getfd (fhe, O_WRONLY);
  if (fd < 0) {
    nfs3exp_err (sbp, nfs_errno ());
    return;
  }
  if (fsync (fd) == 0) {
    ex_commit3res nfsres (NFS3_OK);
    sbp->reply (&nfsres);
  } else {
    warnx << "fsync failed; weird\n";
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_remove (svccb *sbp)
{
  diropargs3 *dirop = sbp->template getarg<diropargs3> ();
  fh_entry *dhe = fsrv->lookup (&dirop->dir);
  str n = dhe->path << "/" << dirop->name;

  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  fh_entry *fhe = fsrv->lookup_add (n);

  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  if (!fhe) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  if (unlink (n) == 0) {
    fsrv->remove (fhe);
    ex_wccstat3 nfsres (NFS3_OK);
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_rmdir (svccb *sbp)
{
  diropargs3 *dirop = sbp->template getarg<diropargs3> ();
  fh_entry *dhe = fsrv->lookup (&dirop->dir);

  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  str n = dhe->path << "/" << dirop->name;
  fh_entry *fhe = fsrv->lookup_add (n);
  if (!fhe) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }
  if (rmdir (n) == 0) {
    dhe->update_attr (dhe->path); // update link count of parent
    fsrv->remove (fhe);
    ex_wccstat3 nfsres (NFS3_OK);
    sbp->reply (&nfsres);
  } else {
    warnx << "rmdir: " << n << " " << errno << "\n";
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_rename (svccb *sbp)
{
  rename3args *rn = sbp->template getarg<rename3args> ();
  fh_entry *fhefrom = fsrv->lookup (&rn->from.dir);
  fh_entry *fheto = fsrv->lookup (&rn->to.dir);
  str from = fhefrom->path << "/" << rn->from.name;
  str to = fheto->path << "/" << rn->to.name;

  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  fh_entry *fh = fsrv->lookup_add (from);

  if (!fhefrom || !fheto) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  if (!fh) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }
  if (rename (from, to) == 0) {
    fhefrom->update_attr (fhefrom->path);
    fheto->update_attr (fheto->path);
    fh->path = to;
    fh->update_attr (fh->path);
    ex_rename3res nfsres (NFS3_OK);
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_link (svccb *sbp)
{
  link3args *lk = sbp->template getarg<link3args> ();
  fh_entry *fhe = fsrv->lookup (&lk->file);
  fh_entry *dhe = fsrv->lookup (&lk->link.dir);

  if (!fhe || !dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  str n = dhe->path << "/" << lk->link.name;
  if (link (fhe->path, n) == 0) {
    fsrv->lookup_add (n);	// XXX we have same fh; this is a problem
    fhe->update_attr (n);
    ex_link3res nfsres (NFS3_OK);
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}


void
client::nfs3_mkdir (svccb *sbp)
{
  mkdir3args *mk = sbp->template getarg<mkdir3args> ();
  fh_entry *dhe = fsrv->lookup (&mk->where.dir);
  fh_entry *fhe;
  uint32 m = 0;
  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  if (mk->attributes.mode.set)
    m = *mk->attributes.mode.val;

  str n = dhe->path << "/" << mk->where.name;
  if (mkdir (n, m) == 0) {
    ex_diropres3 nfsres (NFS3_OK);
    dhe->update_attr (dhe->path); // update link count of parent
    fhe = fsrv->lookup_add (n);
    nfsres.resok->obj.set_present (true);
    *nfsres.resok->obj.handle = fhe->fh;
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_symlink (svccb *sbp)
{
  symlink3args *sl = sbp->template getarg<symlink3args> ();
  fh_entry *dhe = fsrv->lookup (&sl->where.dir);
  fh_entry *fhe;

  if (!dhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (!fsrv->checkfhe ()) {
    nfs3exp_err (sbp, nfs_errno());
    return;
  }

  str p = dhe->path << "/" << sl->where.name; // the name of the file to create
  if (symlink (sl->symlink.symlink_data, p) == 0) {
    ex_diropres3 nfsres (NFS3_OK);
    fhe = fsrv->lookup_add (p);
    nfsres.resok->obj.set_present (true);
    *nfsres.resok->obj.handle = fhe->fh;
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client::nfs3_readlink (svccb *sbp)
{
  nfs_fh3 *fh = sbp->template getarg<nfs_fh3> ();
  fh_entry *fhe = fsrv->lookup (fh);
  char dest [PATH_MAX + 1];
  str p;

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }
  bzero (dest, PATH_MAX + 1);
  if (readlink (fhe->path, dest, PATH_MAX + 1) != -1) {
    str p (dest);
    ex_readlink3res nfsres (NFS3_OK);
    nfsres.resok->data = p;
    sbp->reply (&nfsres);
  } else {
    nfs3exp_err (sbp, nfs_errno ());
  }
}

void
client:: nfs3_setattr (svccb *sbp)
{
  setattr3args *sa = sbp->template getarg<setattr3args> ();
  fh_entry *fhe = fsrv->lookup (&sa->object);

  if (!fhe) {
    nfs3exp_err (sbp, NFS3ERR_STALE);
    return;
  }

  if (sa->new_attributes.mode.set) {
    uint32 m = *sa->new_attributes.mode.val;
    if (chmod (fhe->path, m) == 0) {
      fhe->update_attr (fhe->path);
      ex_wccstat3 nfsres (NFS3_OK);
      sbp->reply (&nfsres);
    } else {
      nfs3exp_err (sbp, nfs_errno ());
    }
  } else if (sa->new_attributes.size.set) {
    uint64 s = *sa->new_attributes.size.val;
    if (truncate (fhe->path, s) == 0) {
      fhe->update_attr (fhe->path);
      ex_wccstat3 nfsres (NFS3_OK);
      sbp->reply (&nfsres);
    } else {
      nfs3exp_err (sbp, nfs_errno ());
    }
  } else if (sa->new_attributes.atime.set || sa->new_attributes.mtime.set) {
    nfstime3 t;
    struct timeval tim[2];

    switch (sa->new_attributes.atime.set) {
    case SET_TO_CLIENT_TIME:
      t = *sa->new_attributes.atime.time;
      tim[0].tv_sec = t.seconds;
      tim[0].tv_usec = t.nseconds;
      break;
    case SET_TO_SERVER_TIME:
      gettimeofday(&tim[0], NULL);
      break;
    default:
      t = fhe->fa.atime;
      tim[0].tv_sec = t.seconds;
      tim[0].tv_usec = t.nseconds;
    }

    switch (sa->new_attributes.mtime.set) {
    case SET_TO_CLIENT_TIME:
      t = *sa->new_attributes.mtime.time;
      tim[1].tv_sec = t.seconds;
      tim[1].tv_usec = t.nseconds;
      break;
    case SET_TO_SERVER_TIME:
      gettimeofday(&tim[1], NULL);
      break;
    default:
      t = fhe->fa.mtime;
      tim[1].tv_sec = t.seconds;
      tim[1].tv_usec = t.nseconds;
    }
    
    if (utimes (fhe->path, tim) == 0) {
      fhe->update_attr (fhe->path);
      ex_wccstat3 nfsres (NFS3_OK);
      sbp->reply (&nfsres);
    } else {
      nfs3exp_err (sbp, nfs_errno ());
    }
  } else {
    nfs3exp_err (sbp, NFS3ERR_NOTSUPP);
  }
}

void
client::nfs3dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }

#if 0
  U_int32_t authno = sbp->getaui ();
  if (authno >= authtab.size () || !authtab[authno]) {
    sbp->reject (AUTH_REJECTEDCRED);
    return;
  }
#endif

  if (!rwsrv) {
    nfs3exp_err (sbp, NFS3ERR_BADHANDLE);
    return;
  }

  switch (sbp->proc()) {
  case lbfs_NFSPROC3_NULL:
    sbp->reply (NULL);
    break;
  case lbfs_NFSPROC3_GETATTR:
    nfs3_getattr (sbp);
    break;
  case lbfs_NFSPROC3_FSINFO:
    nfs3_fsinfo (sbp);
    break;
  case lbfs_NFSPROC3_FSSTAT:
    nfs3_fsstat (sbp);
    break;
  case lbfs_NFSPROC3_ACCESS:
    nfs3_access (sbp);
    break;
  case lbfs_NFSPROC3_LOOKUP:
    nfs3_lookup (sbp);
    break;
  case lbfs_NFSPROC3_READDIR:
    nfs3_readdir (sbp);
    break;
  case lbfs_NFSPROC3_READ:
    nfs3_read (sbp);
    break;
  case lbfs_NFSPROC3_CREATE:
    nfs3_create (sbp);
    break;
  case lbfs_NFSPROC3_CONDWRITE:
    nfs3_condwrite (sbp);
    break;
  case lbfs_NFSPROC3_WRITE:
    nfs3_write (sbp);
    break;
  case lbfs_NFSPROC3_COMMIT:
    nfs3_commit (sbp);
    break;
  case lbfs_NFSPROC3_REMOVE:
    nfs3_remove (sbp);
    break;
  case lbfs_NFSPROC3_RMDIR:
    nfs3_rmdir (sbp);
    break;
  case lbfs_NFSPROC3_RENAME:
    nfs3_rename (sbp);
    break;
  case lbfs_NFSPROC3_LINK:
    nfs3_link (sbp);
    break;
  case lbfs_NFSPROC3_MKDIR:
    nfs3_mkdir (sbp);
    break;
  case lbfs_NFSPROC3_SYMLINK:
    nfs3_symlink (sbp);
    break;
  case lbfs_NFSPROC3_READLINK:
    nfs3_readlink (sbp);
    break;
  case lbfs_NFSPROC3_SETATTR:
    nfs3_setattr (sbp);
    break;
  default:
    nfs3exp_err (sbp, NFS3ERR_NOTSUPP);
  }
}

void
client::sfs_getfsinfo (svccb *sbp)
{
  sfs_fsinfo fsinfo;

  fsinfo.set_prog (ex_NFS_PROGRAM);
  fsinfo.nfs->set_vers (ex_NFS_V3);
  fsinfo.nfs->v3->root = fsrv->root->fh;

  if (rwsrv) {
    sbp->replyref (fsinfo);
  } else 
    sbp->reject (PROC_UNAVAIL);
}

ptr<rabin_priv>
client::doconnect (const sfs_connectarg *ci, sfs_servinfo *si)
{
  fsrv = defsrv;
  *si = fsrv->servinfo;
  return fsrv->sk;
}

