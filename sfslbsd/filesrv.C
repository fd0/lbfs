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

#include "nfstrans.h"
#include "nfs3_nonnul.h"
#include "lbfs.h"
#include "sfsrwsd.h"

extern int lbsd_trace;

static struct timeval t0;
static struct timeval t1;
static inline unsigned timediff() {
  return (t1.tv_sec*1000000+t1.tv_usec)-(t0.tv_sec*1000000+t0.tv_usec);
}

class erraccum : public virtual refcount {
  typedef callback<void, bool>::ref cb_t;
  const cb_t cb;
  bool ok;
public:
  erraccum (cb_t c) : cb (c), ok (true) {}
  ~erraccum () { (*cb) (ok); }
  void seterr () { ok = false; }
};

#define FSID_SHIFT 20

static inline u_int64_t
fsidino2ino (u_int64_t fsid, u_int64_t fileid)
{
  return fileid ^ (fsid << FSID_SHIFT);
}

static u_int64_t
fattr2ino (const fattr3 &a)
{
  return fsidino2ino (a.fsid, a.fileid);
}

inline bool
resok (erraccum *ea, str path, str err)
{
  if (!err)
    return true;
  warn << path << ": " << err << "\n";
  ea->seterr ();
  return false;
}

int
filesrv::path2fsidx (str path, size_t nfs)
{
  int best = 0;
  for (size_t i = 1; i < nfs; i++) {
    const str &iname = fstab[i].path_mntpt;
    if (iname.len () > fstab[best].path_mntpt.len ()
	&& iname.len () < path.len ()
	&& path[iname.len ()] == '/'
	&& !memcmp (iname, path, iname.len ()))
      best = i;
  }
  return best;
}

#define lbfs_mkerr(proc, arg, res)                \
  case proc:                                      \
    sbp->replyref (res (status), nocache);        \
      break;

#define lbfs_trans(proc, type)                                  \
  case proc:                                                    \
    if (rpc_traverse (fht, *static_cast<type *> (objp)))        \
      return true;                                              \
    if (!fht.err)                                               \
      fht.err = NFS3ERR_INVAL;                                  \
    return false;
#define lbfs_transarg(proc, arg, res) lbfs_trans (proc, arg)
#define lbfs_transres(proc, arg, res) lbfs_trans (proc, res)

#define LBFS_PROGRAM_3_APPLY_NONULL(macro) \
    LBFS_PROGRAM_3_APPLY_NOVOID (macro, nfs3void)

static bool
lbfs_nfs3_transarg (fh3trans &fht, void *objp, u_int32_t proc)
{
  switch (proc) {
    LBFS_PROGRAM_3_APPLY_NONULL (lbfs_transarg);
  default:
    panic ("lbfs_nfs3_transarg: bad proc %d\n", proc);
  }
}

static bool
lbfs_nfs3exp_transres (fh3trans &fht, void *objp, u_int32_t proc)
{
  switch (proc) {
    LBFS_PROGRAM_3_APPLY_NONULL (lbfs_transres);
  default:
    panic ("lbfs_nfs3exp_transres: bad proc %d\n", proc);
  }
}

void
lbfs_nfs3exp_err (svccb *sbp, nfsstat3 status)
{
  assert (status);
  /* After JUKEBOX errors, FreeBSD resends requests with the same xid. */
  bool nocache = status == NFS3ERR_JUKEBOX;

  switch (sbp->proc ()) {
    LBFS_PROGRAM_3_APPLY_NOVOID (lbfs_mkerr, nfs3void);
  default:
    panic ("nfs3exp_err: invalid proc %d\n", sbp->proc ());
  }
}

/*
 *  Convert to/from the ex_ version of data structures
 */

#define stompit(proc, arg, res)			\
  case proc:					\
    stompcast (*static_cast<res *> (resp));	\
    break;

void
lbfs_exp_enable (u_int32_t proc, void *resp)
{
  switch (proc) {
    LBFS_PROGRAM_3_APPLY_NOVOID (stompit, nfs3void);
  default:
    panic ("lbfs_exp_enable: bad proc %d\n", proc);
    break;
  }
}

void
lbfs_exp_disable (u_int32_t proc, void *resp)
{
  switch (proc) {
    LBFS_PROGRAM_3_APPLY_NOVOID (stompit, nfs3void);
  default:
    panic ("lbfs_exp_enable: bad proc %d\n", proc);
    break;
  }
}


filesrv::filesrv ()
  : leasetime (60), db_gc_on(false), st (synctab_alloc ())
{
}

void
filesrv::init (cb_t c)
{
  assert (!cb);
  cb = c;
  aclntudp_create (host, 0, nfs_program_3, wrap (this, &filesrv::getnfsc));
  fpdb.open (FP_DB);
}

void
filesrv::getnfsc (ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << host << ": NFS3: " << stat << "\n";
    (*cb) (false);
    return;
  }
  c = nc;
  aclntudp_create (host, 0, mount_program_3, wrap (this, &filesrv::getmountc));
}

void
filesrv::getmountc (ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << host << ": MOUNT3: " << stat << "\n";
    (*cb) (false);
    return;
  }
  mountc = nc;

  for (size_t i = 0; i < fstab.size (); i++)
    fstab[i].parent = &fstab[path2fsidx (fstab[i].path_mntpt, i)];
  ref<erraccum> ea (New refcounted<erraccum> (wrap (this,
						    &filesrv::gotroots)));
  for (size_t i = 0; i < fstab.size (); i++)
    if (fstab[i].path_root)
      getfh3 (mountc, fstab[i].path_root,
	      wrap (this, &filesrv::gotroot, ea, i));
}

void
filesrv::gotroot (ref<erraccum> ea, int i, const nfs_fh3 *fhp, str err)
{
  if (resok (ea, strbuf ("mount: ") << fstab[i].path_root, err))
    fstab[i].fh_root = *fhp;
}

void
filesrv::gotroots (bool ok)
{
  if (!ok) {
    (*cb) (false);
    return;
  }

  ref<erraccum> ea (New refcounted<erraccum> (wrap (this, &filesrv::gotmps)));
  sfs_trash.setsize(fstab.size());

  for (size_t i = 0; i < fstab.size (); i++) {
    lookupfh3 (c, fstab[i].fh_root, "",
	       wrap (this, &filesrv::gotrootattr, ea, i));
    lookupfh3 (c, fstab[i].parent->fh_root,
	       substr (fstab[i].path_mntpt,
		       fstab[i].parent->path_mntpt.len ()),
	       wrap (this, &filesrv::gotmp, ea, i));
    
    sattr3 trash_attr;
    trash_attr.mode.set_set(true);
    *(trash_attr.mode.val) = 0777;
    trash_attr.uid.set_set(true);
    *(trash_attr.uid.val) = 0;
    trash_attr.gid.set_set(true);
    *(trash_attr.gid.val) = 0;
   
    nfs3_mkdir (c, fstab[i].fh_root, ".sfs.trash", trash_attr,
	        wrap (this, &filesrv::gottrashdir, ea, i, 0, true));
  }
}

void
filesrv::gottrashdir (ref<erraccum> ea, int i, int j, bool root,
                      const nfs_fh3 *fhp, str err)
{
  if (resok(ea, strbuf ("create trash dir for ") << fstab[i].path_mntpt, err)) {
    if (root) {
      sfs_trash[i].topdir = *fhp;
      warn << "trash dir for " << fstab[i].path_mntpt << ": " << 
              armor32(fhp->data.base(), fhp->data.size()) << "\n";
      sattr3 trash_attr;
      trash_attr.mode.set_set(true);
      *(trash_attr.mode.val) = 0777;
      trash_attr.uid.set_set(true);
      *(trash_attr.uid.val) = 0;
      trash_attr.gid.set_set(true);
      *(trash_attr.gid.val) = 0;
      char subdir[32];
      sprintf(subdir, "%d", 0);
      nfs3_mkdir (c, sfs_trash[i].topdir, subdir, trash_attr,
	          wrap(this, &filesrv::gottrashdir, ea, i, 0, false));
    }
    else {
      sfs_trash[i].subdirs[j] = *fhp;
      if (j < SFS_TRASH_DIR_BUCKETS-1) {
        sattr3 trash_attr;
        trash_attr.mode.set_set(true);
        *(trash_attr.mode.val) = 0777;
        trash_attr.uid.set_set(true);
        *(trash_attr.uid.val) = 0;
        trash_attr.gid.set_set(true);
        *(trash_attr.gid.val) = 0;
        char subdir[32];
        sprintf(subdir, "%d", j+1);
        nfs3_mkdir (c, sfs_trash[i].topdir, subdir, trash_attr,
	            wrap(this, &filesrv::gottrashdir, ea, i, j+1, false));
      } else {
        sfs_trash[i].window[0] = 0;
        for (unsigned j = 0; j < SFS_TRASH_WIN_SIZE; j++)
          make_trashent(i, j);
      }
    }
  }
}

unsigned
filesrv::get_trashent(unsigned fsno)
{
  unsigned i = sfs_trash[fsno].window[0];
  unsigned r = sfs_trash[fsno].window[i+1];
  return r;
}

void
filesrv::update_trashent(unsigned fsno)
{
  unsigned i = sfs_trash[fsno].window[0];
  if (sfs_trash[fsno].window[0] == SFS_TRASH_WIN_SIZE-1)
    sfs_trash[fsno].window[0] = 0;
  else
    sfs_trash[fsno].window[0]++;
  make_trashent(fsno, i);
}

void
filesrv::make_trashent(unsigned fsno, unsigned trash_idx)
{
  sfs_trash[fsno].window[trash_idx+1] = rnd.getword() % SFS_TRASH_DIR_SIZE;
  unsigned r = sfs_trash[fsno].window[trash_idx+1];
  str rstr = armor32((void*)&r, sizeof(r));
  char tmpfile[7+rstr.len()+1];
  sprintf(tmpfile, "oscar.%s", rstr.cstr());

  diropargs3 arg;
  arg.dir = sfs_trash[fsno].subdirs[r % SFS_TRASH_DIR_BUCKETS];
  arg.name = tmpfile;
  lookup3res *res = New lookup3res;
  c->call (NFSPROC3_LOOKUP, &arg, res,
	   wrap(this, &filesrv::make_trashent_lookup_cb, r, fsno, res),
	   auth_default);
}

void
filesrv::make_trashent_lookup_cb(unsigned r, unsigned fsno, 
                                 lookup3res *res, clnt_stat err)
{
  str rstr = armor32((void*)&r, sizeof(r));
  char tmpfile[7+rstr.len()+1];
  sprintf(tmpfile, "oscar.%s", rstr.cstr());
  if (!err && !res->status) {
    if (res->resok->obj_attributes.present) {
      /* schedule removal of this fh from database */
      removed_fhs.push_back(res->resok->object);
      if (lbsd_trace > 1)
        warn << "GC: schedule old fh for " << tmpfile << " for gc\n";
      if (!db_gc_on) {
        delaycb(5, wrap(this, &filesrv::db_gc));
	db_gc_on = true;
      }
    }
    diropargs3 arg;
    arg.name = tmpfile;
    arg.dir = sfs_trash[fsno].subdirs[r % SFS_TRASH_DIR_BUCKETS];
    wccstat3 *wres = New wccstat3;
    c->call(NFSPROC3_REMOVE, &arg, wres,
	    wrap(this, &filesrv::make_trashent_remove_cb, wres), auth_default);
  }
  delete res;
}

void
filesrv::make_trashent_remove_cb(wccstat3 *res, clnt_stat err)
{
  delete res;
}

void
filesrv::db_gc()
{
  size_t n = removed_fhs.size();
  int k = 0;
  if (lbsd_trace > 1) {
    warn << "GC: trying to gc " << n << " fhs\n";
    gettimeofday(&t0, 0L);
  }
  fp_db::iterator *iter = 0;
  if (fpdb.get_iterator(&iter) == 0 && iter) {
    chunk_location c;
    int j = iter->get(&c);
    if (!j) {
      do {
	k++;
	nfs_fh3 fh;
	c.get_fh(fh);
        for(size_t i=0; i<removed_fhs.size(); i++) {
	  if (fh == removed_fhs[i]) {
	    n--;
            iter->del();
	    break;
	  }
        }
      } while(!iter->next(&c) && n > 0);
    } 
  }
  if (iter)
    delete iter;
  if (lbsd_trace > 1) {
    gettimeofday(&t1, 0L);
    unsigned d = timediff()/1000;
    warn << "GC: " << k << " chunks in " << d << " msec, "
         << removed_fhs.size()-n << " removed\n";
  }
  removed_fhs.setsize(0);
  db_gc_on = false;
}

void
filesrv::gotrootattr (ref<erraccum> ea, int i,
		      const nfs_fh3 *fhp, const FATTR3 *attr, str err)
{
  if (resok (ea, strbuf ("getattr: ") << fstab[i].path_root, err)) {
    fstab[i].fsid = attr->fsid;
    fstab[i].fileid_root = attr->fileid;
  }
}

void
filesrv::gotmp (ref<erraccum> ea, int i,
		const nfs_fh3 *fhp, const FATTR3 *attr, str err)
{
  if (resok (ea, strbuf ("lookup: ") << fstab[i].path_mntpt, err)) {
    fstab[i].fh_mntpt = *fhp;
    fstab[i].fileid_mntpt = attr->fileid;
  }
}

void
filesrv::gotmps (bool ok)
{
  if (!ok) {
    (*cb) (false);
    return;
  }

  ref<erraccum> ea (New refcounted<erraccum> (wrap (this, &filesrv::gotdds)));
  for (size_t i = 0; i < fstab.size (); i++)
    for (int mp = 0; mp < 2; mp++) {
      ref<readdir3res> res (New refcounted<readdir3res>);
      rpc_clear (*res);
      res->resok->reply.entries.alloc ();
      rpc_clear (*res->resok->reply.entries);
      gotrdres (ea, res, i, mp, RPC_SUCCESS);
    }
}

void
filesrv::gotrdres (ref<erraccum> ea, ref<readdir3res> res,
		   int i, bool mp, clnt_stat stat)
{
  str path (strbuf ("readdir: ")
	    << (mp ? fstab[i].path_mntpt : fstab[i].path_root));
  if (!resok (ea, path, stat2str (res->status, stat)))
    return;

  entry3 *ep = res->resok->reply.entries;
  if (ep)
    for (;;) {
      if (ep->name == "..") {
	if (mp)
	  fstab[i].fileid_mntpt_dd = ep->fileid;
	else
	  fstab[i].fileid_root_dd = ep->fileid;
	return;
      }
      entry3 *nep = ep->nextentry;
      if (!nep)
	break;
      ep = nep;
    }

  if (res->resok->reply.eof) {
    warn << path << ": could not find \"..\"\n";
    ea->seterr ();
    return;
  }
  else if (!ep) {
    warn << path << ": zero entry non-EOF reply from kernel\n";
    ea->seterr ();
    return;
  }

  readdir3args arg;
  arg.dir = mp ? fstab[i].fh_mntpt : fstab[i].fh_root;
  arg.cookie = ep->cookie;
  arg.cookieverf = res->resok->cookieverf;
  arg.count = 8192;
  c->call (NFSPROC3_READDIR, &arg, res,
	   wrap (this, &filesrv::gotrdres, ea, res, i, mp),
	   auth_default);
}

#if 0
static void
dumpsubst (const u_int64_t &l, u_int64_t *r)
{
  warnx ("%14qx -> %qx\n", l, *r);
}
#endif

void
filesrv::gotdds (bool ok)
{
  if (!ok) {
    (*cb) (false);
    return;
  }

  str p;
  sha1ctx sc;
  p = str2wstr (sk->p.getraw ());
  sc.update (p, p.len ());
  sc.update (hostid.base (), hostid.size ());
  p = str2wstr (sk->q.getraw ());
  sc.update (p, p.len ());
  char key[sha1::hashsize];
  sc.final (key);
  fhkey.setkey (key, sizeof (key));
  bzero (key, sizeof (key));

  for (filesys *fsp = fstab.base (); fsp < fstab.lim (); fsp++) {
    mp3tab.insert (fsp);
    root3tab.insert (fsp);
    fsp->inotab = New filesys::inotab_t;
    if (fsp == fsp->parent)
      /* Make sure readdir returns unique fileid for /.., since an
       * accessible copy of /.. may reside somewhere else in the file
       * system and otherwise break pwd. */
      fsp->inotab->insert (fsp->fileid_root_dd, 1);
    else {
      fsp->parent->inotab->insert (fsp->fileid_mntpt,
				   fsidino2ino (fsp->fsid, fsp->fileid_root));
      fsp->inotab->insert (fsp->fileid_root_dd,
			   fsidino2ino (fsp->parent->fsid,
					fsp->fileid_mntpt_dd));
    }
  }

#if 0
  for (filesys *fsp = fstab.base (); fsp < fstab.lim (); fsp++) {
    warnx << fsp->path_mntpt << ":\n";
    warnx ("fsid 0x%qx, / 0x%qx, /.. 0x%qx, mp 0x%qx, mp/.. 0x%qx\n",
	   fsp->fsid, fsp->fileid_root, fsp->fileid_root_dd,
	   fsp->fileid_mntpt, fsp->fileid_mntpt_dd);
    fsp->inotab->traverse (wrap (dumpsubst));
  }
#endif

  fh3trans fht (fh3trans::ENCODE, fhkey, 0);

  fsinfo.set_prog (LBFS_PROGRAM);
  fsinfo.nfs->set_vers (LBFS_V3);
  fsinfo.nfs->v3->root = fstab[0].fh_root;
  if (!rpc_traverse (fht, fsinfo.nfs->v3->root))
    fatal ("filesrv::finish: nfs3_transres encode failed (err %d)\n", fht.err);

  fsinfo.nfs->v3->subfs.setsize (fstab.size () - 1);
  for (size_t i = 1; i < fstab.size (); i++) {
    fsinfo.nfs->v3->subfs[i-1].path = fstab[i].path_mntpt;
    fsinfo.nfs->v3->subfs[i-1].fh = fstab[i].fh_root;
    fht.srvno = i;
    if (!rpc_traverse (fht, fsinfo.nfs->v3->subfs[i-1].fh))
      fatal ("filesrv::finish: nfs3_transres encode failed (err %d)\n",
	     fht.err);
  }

  mountc = NULL;
  (*cb) (true);
  cb = NULL;
}

bool
filesrv::getauthclnt ()
{
  if (authxprt && !authxprt->ateof ())
    return true;
  int fd = suidgetfd ("authserv");
  if (fd < 0) {
    authxprt = NULL;
    authclnt = NULL;
    return false;
  }
  authclnt = aclnt::alloc (authxprt = axprt_stream::alloc (fd),
			   sfsauth_program_1);
  return true;
}

int
filesrv::fhsubst (bool *substp, nfs_fh3 *fhp, u_int32_t *fhnop)
{
  filesys *fsp;
  for (fsp = mp3tab[*fhp]; fsp; fsp = mp3tab.nextkeq (fsp))
    if (fsp != fstab.base () && getfsno (fsp->parent) == *fhnop) {
      *fhp = fsp->fh_root;
      *fhnop = fsp - fstab.base ();
      *substp = true;
      break;
    }
  return 0;
}

static inline bool
anon_checkperm (svccb *sbp, u_int options, bool isroot)
{
  switch (sbp->proc ()) {
  case NFSPROC3_GETATTR:
  case NFSPROC3_ACCESS:
  case NFSPROC3_FSINFO:
  case NFSPROC3_PATHCONF:
    if (isroot || (options & filesys::ANON_READ) == filesys::ANON_READ)
      return true;
    break;

  case NFSPROC3_FSSTAT:
    {
      if (isroot || (options & filesys::ANON_READ) == filesys::ANON_READ)
	return true;
      ex_fsstat3res res (NFS3_OK);
      res.resok->tbytes = 0;
      res.resok->fbytes = 0;
      res.resok->abytes = 0;
      res.resok->tfiles = 0;
      res.resok->ffiles = 0;
      res.resok->afiles = 0;
      res.resok->invarsec = 0;
      sbp->reply (&res);
      return false;
    }

  case NFSPROC3_LOOKUP:
  case NFSPROC3_READLINK:
  case NFSPROC3_READ:
  case NFSPROC3_READDIR:
  case NFSPROC3_READDIRPLUS:
    if (options & filesys::ANON_READ)
      return true;
    break;

  default:
    if ((options & filesys::ANON_READWRITE) == filesys::ANON_READWRITE)
      return true;
    break;
  }

  lbfs_nfs3exp_err (sbp, NFS3ERR_PERM);
  return false;
}

bool
filesrv::fixarg (svccb *sbp, reqstate *rqsp)
{
  fh3trans fht (fh3trans::DECODE, fhkey);
  if (!lbfs_nfs3_transarg (fht, sbp->template getarg<void> (), sbp->proc ())) {
    lbfs_nfs3exp_err (sbp, nfsstat3 (fht.err));
    return false;
  }
  if (fht.srvno >= fstab.size ()) {
    lbfs_nfs3exp_err (sbp, NFS3ERR_BADHANDLE);
    return false;
  }
  rqsp->fsno = fht.srvno;
  filesys *fsp = &fstab[rqsp->fsno];
  rqsp->rootfh = false;

  /* We let anonymous users GETATTR any root file handle, not just the
   * root of all exported files.  This is to help client
   * implementations that want to avoid (st_ino, st_dev) conflicts by
   * creating multiple mount points for each server.  Is this bad? */
  if (!sbp->getaui ()
      && !anon_checkperm (sbp, fsp->options,
			  *sbp->template getarg<nfs_fh3> () == fsp->fh_root))
    return false;

  switch (sbp->proc ()) {
  case NFSPROC3_LOOKUP:
    {
      diropargs3 *doa = sbp->template getarg<diropargs3> ();
      if (doa->name == ".." && doa->dir == fsp->fh_root) {
	if (!getfsno (fsp)) {
	  lbfs_nfs3exp_err (sbp, NFS3ERR_ACCES);
	  return false;
	}
	doa->dir = fsp->fh_mntpt;
	rqsp->fsno = getfsno (fsp->parent);
	rqsp->rootfh = true;
      }
      break;
    }
  case NFSPROC3_READDIR:
  case NFSPROC3_READDIRPLUS:
    {
      nfs_fh3 *rpa = sbp->template getarg<nfs_fh3> ();
      if (*rpa == fsp->fh_root)
	rqsp->rootfh = true;
      break;
    }
  }
  return true;
}

void
filesrv::fixrdres (void *_res, filesys *fsp, bool rootfh)
{
  ex_readdir3res *res = static_cast<ex_readdir3res *> (_res);
  if (res->status)
    return;
  for (entry3 *e = res->resok->reply.entries; e; e = e->nextentry) {
    u_int64_t *idp = (*fsp->inotab)[e->fileid];
    if (idp && e->name != "." && (rootfh || e->name != ".."))
      e->fileid = *idp;
    else
      e->fileid = fsidino2ino (fsp->fsid, e->fileid);
  }
}

void
filesrv::fixrdplusres (void *_res, filesys *fsp, bool rootfh)
{
  ex_readdirplus3res *res = static_cast<ex_readdirplus3res *> (_res);
  if (res->status)
    return;
  for (ex_entryplus3 *e = res->resok->reply.entries; e; e = e->nextentry) {
    u_int64_t *idp = (*fsp->inotab)[e->fileid];
    if (idp && e->name != "." && (rootfh || e->name != ".."))
      e->fileid = *idp;
    else
      e->fileid = fsidino2ino (fsp->fsid, e->fileid);
    if (!e->name_handle.present)
      e->name_attributes.set_present (false);
    else if (rootfh && e->name == "..") {
      e->name_attributes.set_present (false);
      e->name_handle.set_present (false);
    }
    else {
      filesys *nfsp;
      for (nfsp = mp3tab[*e->name_handle.handle];
	   nfsp && nfsp->parent != fsp; nfsp = mp3tab.nextkeq (nfsp))
	;
      if (nfsp)
	e->name_attributes.set_present (false);
    }
  }
}

static int
xor_ino (fattr3 *fp, u_int32_t srvno)
{
  fp->fileid = fattr2ino (*fp);
  return 0;
}
static fh3trans::fattr_hook_t fhook = wrap (xor_ino);

bool
filesrv::fixres (svccb *sbp, void *res, reqstate *rqsp)
{
  filesys *fsp = &fstab[rqsp->fsno];

  if (sbp->proc () == NFSPROC3_READDIR)
    fixrdres (res, fsp, rqsp->rootfh);
  else if (sbp->proc () == NFSPROC3_READDIRPLUS)
    fixrdplusres (res, fsp, rqsp->rootfh);

  bool subst = false;
  fh3trans fht (fh3trans::ENCODE, fhkey, rqsp->fsno,
		wrap (this, &filesrv::fhsubst, &subst));

  fht.fattr_hook = fhook;
  if (!lbfs_nfs3exp_transres (fht, res, sbp->proc ())) {
    warn ("nfs3reply: nfs3_transres encode failed (err %d)\n", fht.err);
    lbfs_nfs3exp_err (sbp, nfsstat3 (fht.err));
    return false;
  }

  switch (sbp->proc ()) {
  case NFSPROC3_LOOKUP:
    {
      ex_lookup3res *dor = static_cast<ex_lookup3res *> (res);
      if (rqsp->rootfh) {
	if (dor->status)
	  dor->resfail->set_present (false);
	else
	  dor->resok->dir_attributes.set_present (false);
      }
      if (subst && !dor->status)
	dor->resok->obj_attributes.set_present (false);
      break;
    }
  case NFSPROC3_ACCESS:
    if (!sbp->getaui () && fsp->options != filesys::ANON_READWRITE) {
      ex_access3res *ar = static_cast<ex_access3res *> (res);
      if ((fsp->options & filesys::ANON_READ) != filesys::ANON_READ) {
	if (ar->status)
	  ar->resfail->set_present (false);
	else {
	  ar->resok->access = 0;
	  ar->resok->obj_attributes.set_present (false);
	}
      }
      else if (!ar->status)
	ar->resok->access &= ACCESS3_READ|ACCESS3_LOOKUP|ACCESS3_EXECUTE;
    }
    break;
  }
  return true;
}
