#include "sfslbcd.h"
#include "sfsclient.h"
#include "xfs-sfs.h"
#include "xfs-nfs.h"
#include "cache.h"
#include "../sfslbsd/sfsrwsd.h"
#include <sys/stat.h>
#include "dmalloc.h"

int lbfs (getenv("LBFS") ? atoi (getenv ("LBFS")) : 2);
int lbcd_trace (getenv("LBCD_TRACE") ? atoi (getenv ("LBCD_TRACE")) : 0);
unsigned FILE_DES = 0;
int OLD = 0;

extern bool lbfsdb_is_dirty;

uint64 total_bytes_sent = 0;
uint64 total_write_filesize = 0;
uint64 total_bytes_recv = 0;
uint64 total_read_filesize = 0;

struct attr_obj {
  attr_cb_t cb;
  int fd;
  ref<aclnt> c;

  ref<xfs_message_header> hh;
  xfs_message_putattr *h;
  sfs_aid sa;
  const nfs_fh3 fh;
  uint32 seqnum; 
  xfs_cred cred;
  setattr3args saa;
  ptr<ex_getattr3res> gres;
  ptr<ex_wccstat3> wres;
  
  void installattr (time_t rqt, clnt_stat err)
  {
    if (!err) {
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_installattr msg;

      msg.header.opcode = XFS_MSG_INSTALLATTR;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);
      
      cache_entry *ce;
      if (h->header.opcode == XFS_MSG_PUTATTR) {
	if (wres->wcc->after.present) {
	  ce = nfsindex[fh];
	  if ((*wres->wcc->before.attributes).size == ce->nfs_attr.size &&
	      (*wres->wcc->before.attributes).mtime == ce->nfs_attr.mtime &&
	      (*wres->wcc->before.attributes).ctime == ce->nfs_attr.ctime) {
	    truncate(ce->cache_name, (*wres->wcc->after.attributes).size);
            ce->ltime = max((*wres->wcc->after.attributes).mtime, 
		            (*wres->wcc->after.attributes).ctime);
	  }
	  ce = update_cache (fh, *wres->wcc->after.attributes);
	}
	else 
	  if (wres->wcc->before.present) {
	    ce = nfsindex[fh];
	    ce->nfs_attr.size = (*wres->wcc->before.attributes).size;
	    ce->nfs_attr.mtime = (*wres->wcc->before.attributes).mtime;
	    ce->nfs_attr.ctime = (*wres->wcc->before.attributes).ctime;
	  } else {
	    warn << "attr_obj: No attr present!!\n"; 
	    abort ();
	  }
      } else {
	ce = update_cache (fh, *gres->attributes);
      }

      bool update_dir_expire = false;

      if (ce->nfs_attr.type == NF3DIR) {
	nfstime3 maxtime = max(ce->nfs_attr.mtime, ce->nfs_attr.ctime);
	if (!greater(maxtime, ce->ltime))
	  update_dir_expire = true;
      }
      ce->set_exp (rqt, update_dir_expire);
      nfsobj2xfsnode (cred, ce, &msg.node);
      
      xfs_send_message_wakeup_multiple (fd, seqnum,
					0, h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, seqnum, err);
  }
  
  void gotattr (time_t rqt, clnt_stat err) 
  {
    if (!cb)
      installattr (rqt, err);
    else
      if (!err)
	(*cb) (gres, rqt, err);
      else
	(*cb) (NULL, 0, err);
    delete this;
  }

  void setattr () 
  {
    cred = h->cred;
    seqnum = h->header.sequence_num;

    saa.object = fh;
    xfsattr2nfsattr (h->header.opcode, h->attr, &saa.new_attributes);
    saa.guard.set_check (false);

    wres = New refcounted <ex_wccstat3>;
    c->call (lbfs_NFSPROC3_SETATTR, &saa, wres,
	     wrap (this, &attr_obj::installattr, timenow), lbfs_authof (sa));
  }

  void getattr () 
  {
    struct xfs_message_getattr *h1 = (xfs_message_getattr *) h;
    cred = h1->cred;
    seqnum = h1->header.sequence_num;
#if 1
    gres = New refcounted<ex_getattr3res>; 
    c->call (lbfs_NFSPROC3_GETATTR, &fh, gres,
	     wrap (this, &attr_obj::gotattr, timenow), 
	     lbfs_authof (sa));
#else
   void *res = lbfs_program_3.tbl[lbfs_NFSPROC3_GETATTR].alloc_res ();
   
#endif   
  }
  
  attr_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1,
	    const nfs_fh3 &fh1, ref<aclnt> c1, attr_cb_t cb1) : 
    cb(cb1), fd(fd1), c(c1), hh(h1), sa(sa1), fh(fh1)
  {
    h = msgcast<xfs_message_putattr> (hh);
    switch (h->header.opcode) {
    case XFS_MSG_PUTATTR:
      setattr ();
      break;
    case XFS_MSG_GETATTR:
    default:
      getattr ();
      break;
    }
  }
  
};

void 
lbfs_attr (int fd, ref<xfs_message_header> h, sfs_aid sa,
           const nfs_fh3 &fh, ref<aclnt> c, attr_cb_t cb) 
{
  vNew attr_obj (fd, h, sa, fh, c, cb);
}

struct getroot_obj {
  int fd; 
  ref<aclnt> sc;
  ref<aclnt> nc;
  
  ref<xfs_message_header> hh;
  xfs_message_getroot *h;
  sfs_aid sa;
  bool gotnfs_fsi;
  bool gotroot_attr;
  time_t rqtime;
  ptr<sfs_fsinfo> sfs_fsi;
  ptr<ex_fsinfo3res> nfs_fsi;
  ptr<ex_getattr3res> root_attr;

  void installroot () 
  {
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    struct xfs_message_installroot msg;
    msg.header.opcode = XFS_MSG_INSTALLROOT;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    cache_entry *e = update_cache (sfs_fsi->nfs->v3->root, 
				   *root_attr->attributes, "/");
    e->set_exp (rqtime);
    nfsobj2xfsnode (h->cred, e, &msg.node);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,	
     				      0, h0, h0_len, NULL, 0);
    delete this;
  }

  void gotnfs_fsinfo (clnt_stat err) 
  {    
    assert (!err && nfs_fsi->status == NFS3_OK);
    nfs_fsinfo = *nfs_fsi->resok;
    gotnfs_fsi = true;
    if (gotroot_attr)
      installroot ();
  }

  void gotattr (ptr<ex_getattr3res> attr, time_t rqt, clnt_stat err) 
  {
    assert (!err && attr->status == NFS3_OK);
    root_attr = attr;
    rqtime = rqt;
    gotroot_attr = true;
    if (gotnfs_fsi) 
      installroot ();
  }

  void getnfs_fsinfo (clnt_stat err) 
  {
    assert (!err && sfs_fsi->prog == ex_NFS_PROGRAM && 
	    sfs_fsi->nfs->vers == ex_NFS_V3);
    if (lbfs > 0)
      x->compress();

    nfs_fsi = New refcounted<ex_fsinfo3res>;
    nc->call (lbfs_NFSPROC3_FSINFO, &sfs_fsi->nfs->v3->root, nfs_fsi,
	      wrap (this, &getroot_obj::gotnfs_fsinfo), lbfs_authof (sa));
    lbfs_attr (fd, hh, sa, sfs_fsi->nfs->v3->root, 
	       nc, wrap (this, &getroot_obj::gotattr));
  }

  void getsfs_fsinfo () 
  {
    sfs_fsi = New refcounted<sfs_fsinfo>;
    sfsc->call (SFSPROC_GETFSINFO, NULL, sfs_fsi,
		wrap (this, &getroot_obj::getnfs_fsinfo), lbfs_authof (sa));
  }

  getroot_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1,  
	       ref<aclnt> sc1, ref<aclnt> nc1) : 
    fd(fd1), sc(sc1), nc(nc1), hh(h1), sa(sa1), gotnfs_fsi(false), 
    gotroot_attr(false) 
  {
    h = msgcast<xfs_message_getroot> (hh);
    getsfs_fsinfo ();
  }
};

void 
lbfs_getroot (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, 
	      ref<aclnt> sc1, ref<aclnt> nc1) 
{
  vNew getroot_obj (fd1, h1, sa1, sc1, nc1);
}

struct lookup_obj {
  
  typedef callback<void, ptr<ex_lookup3res>, time_t, clnt_stat>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;

  ref<xfs_message_header> hh;
  xfs_message_getnode *h;
  sfs_aid sa;
  const nfs_fh3 parent_fh;
  const char *name;
  ptr<ex_lookup3res> res;

  void found (time_t rqt, clnt_stat err) 
  {
    if (!err) 
      (*cb) (res, rqt, err);
    else 
      (*cb) (NULL, 0, err);
    delete this;
  }

  void lookup () 
  {
    diropargs3 doa;
    doa.dir = parent_fh;
    doa.name = name;
    if (lbcd_trace > 1)
      warn << h->header.sequence_num 
	   << ":" << "requesting file name " << doa.name << "\n";
    res = New refcounted<ex_lookup3res>;

    c->call (lbfs_NFSPROC3_LOOKUP, &doa, res,
	        wrap (this, &lookup_obj::found, timenow), lbfs_authof (sa));
  }

  lookup_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, 
	      const nfs_fh3 &parent_fh1, const char *name1, 
	      ref<aclnt> c1, cb_t cb1) : cb(cb1), fd(fd1), c(c1), 
		hh(h1), sa(sa1), parent_fh(parent_fh1), name(name1)
  {
    h = msgcast<xfs_message_getnode> (hh);
    lookup ();
  }
  
};

void lbfs_lookup (int fd, ref<xfs_message_header> h, sfs_aid sa,
		  const nfs_fh3 &parent_fh, const char *name, 
		  ref<aclnt> c, lookup_obj::cb_t cb) 
{
  vNew lookup_obj (fd, h, sa, parent_fh, name, c, cb);
}

struct getnode_obj {
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_getnode *h;
  sfs_aid sa;
  
  void installnode (ptr<ex_lookup3res> lres, time_t rqt, clnt_stat err) 
  {
    if (!err && lres->status == NFS3_OK) {
      ex_fattr3 a;

      if (lres->resok->obj_attributes.present)
	a = *lres->resok->obj_attributes.attributes;
      else if (lres->resok->dir_attributes.present)
	a = *lres->resok->dir_attributes.attributes;
      else {
	if (lbcd_trace > 1)
	  warn << h->header.sequence_num << ":getnode: error no attr present\n";
	xfs_reply_err(fd, h->header.sequence_num, ENOSYS); //Why ENOSYS??
	delete this;
	return;
      }

      struct xfs_message_installnode msg;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      cache_entry *e = update_cache (lres->resok->object, a, h->name);

      e->set_exp (rqt);
      nfsobj2xfsnode (h->cred, e, &msg.node);
      msg.header.opcode = XFS_MSG_INSTALLNODE;
      msg.parent_handle = h->parent_handle;
      strcpy (msg.name, h->name);
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h->header.sequence_num, err ? EIO : lres->status);
    
    delete this;
  }

  getnode_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, ref<aclnt> c1)
    : fd(fd1), c(c1), hh(h1), sa(sa1)
  {  
    h = msgcast<xfs_message_getnode> (hh);
    cache_entry *e = xfsindex[h->parent_handle];
    if (!e) {
      if (lbcd_trace > 1)
	warn << h->header.sequence_num << ":" 
	     << "xfs_getnode: Can't find parent_handle\n";
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      delete this;
    } else 
      lbfs_lookup (fd, hh, sa, e->nh, h->name, c, 
		   wrap (this, &getnode_obj::installnode));
  }
};

void 
lbfs_getnode (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c)
{  
  vNew getnode_obj (fd, h, sa, c);
}

struct readlink_obj {

  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_open *h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_readlink3res> rlres;
  
  void install_link (time_t rqt, clnt_stat err) 
  {
    if (!err && rlres->status == NFS3_OK) {
      
      struct xfs_message_installdata msg;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;

      e->nfs_attr = *rlres->resok->symlink_attributes.attributes;
      e->set_exp (rqt);
      e->ltime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
      nfsobj2xfsnode (h->cred, e, &msg.node);
      msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;

      int lfd = assign_cachefile (fd, h->header.sequence_num, e, 
				  msg.cache_name, &msg.cache_handle);

      write (lfd, rlres->resok->data.cstr (), rlres->resok->data.len ());
      close (lfd);
      e->incache = true;

      msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);
    } else 
      xfs_reply_err (fd, h->header.sequence_num, err ? EIO : rlres->status);      

    delete this;    
  }

  ~readlink_obj ()
  {
    (*cb) ();
  }

  readlink_obj (int fd1, ref<xfs_message_header> h1, cache_entry *e1,
                sfs_aid sa1, ref<aclnt> c1, readlink_obj::cb_t cb1)
    : cb(cb1), fd(fd1), c(c1), hh(h1), sa(sa1), e(e1)
  {
    h = msgcast<xfs_message_open> (hh);
    rlres = New refcounted <ex_readlink3res>;
    c->call (lbfs_NFSPROC3_READLINK, &e->nh, rlres,
	     wrap (this, &readlink_obj::install_link, timenow), 
	     lbfs_authof (sa));
  }
};

void 
lbfs_readlink (int fd, ref<xfs_message_header> h, cache_entry *e, sfs_aid sa, 
	       ref<aclnt> c, readlink_obj::cb_t cb) 
{
  vNew readlink_obj (fd, h, e, sa, c, cb);
}

void 
lbfs_readexist (int fd, ref<xfs_message_header> hh, cache_entry *e) 
{
  xfs_message_getdata *h = msgcast<xfs_message_getdata> (hh);
  struct xfs_message_installdata msg;

  nfsobj2xfsnode (h->cred, e, &msg.node);
  msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;
  int cfd = assign_cachefile (fd, h->header.sequence_num, e, 
			      msg.cache_name, &msg.cache_handle);
  close (cfd);

  struct xfs_message_header *h0 = NULL;
  size_t h0_len = 0;

  e->incache = true;
  msg.header.opcode = XFS_MSG_INSTALLDATA;
  h0 = (struct xfs_message_header *) &msg;
  h0_len = sizeof (msg);

  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				    h0, h0_len, NULL, 0);  
}

struct readdir_obj {

  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_open *h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_readdir3res> rdres;
  struct xfs_message_installdata msg;
  
  void write_dirfile (write_dirent_args args, xfs_message_installdata msg,
		      clnt_stat err)
  {
    if (nfsdir2xfsfile (rdres, &args) < 0) {
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      return;
    }
    if (args.last)
      flushbuf (&args);
    //free (args.buf);
    if (!rdres->resok->reply.eof) {
      readdir3args rda;
      rda.dir = e->nh; 
      entry3 *ent = rdres->resok->reply.entries;
      while (ent->nextentry != NULL)
	ent = ent->nextentry;
      rda.cookie = ent->cookie;
      rda.cookieverf = rdres->resok->cookieverf;
      rda.count = nfs_fsinfo.dtpref;

      rdres = New refcounted <ex_readdir3res>;
      c->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
	       wrap (this, &readdir_obj::write_dirfile, args, msg),
	       lbfs_authof(sa));
    } else {
      close (args.fd);
      e->incache = true;

      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      msg.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg;
      h0_len = sizeof (msg);
      
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, NULL, 0);
      delete this;
    }      
  }

  void installdir (time_t rqt, clnt_stat err) 
  {
    if (!err && rdres->status == NFS3_OK) {

      struct write_dirent_args args;

      e->nfs_attr = *rdres->resok->dir_attributes.attributes;
      e->set_exp (rqt, true);
      e->ltime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
      nfsobj2xfsnode (h->cred, e, &msg.node);
      msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R;
      
      args.fd = assign_cachefile (fd, h->header.sequence_num, e, 
				  msg.cache_name, &msg.cache_handle, 
				  O_CREAT | O_WRONLY | O_TRUNC);
      write_dirfile (args, msg, clnt_stat (0));
    } else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : rdres->status);      
      delete this;
    }
  }
  
  void readdir () {
    readdir3args rda;
    rda.dir = e->nh;
    rda.cookie = 0;
    rda.cookieverf = cookieverf3 ();
    rda.count = nfs_fsinfo.dtpref;

    rdres = New refcounted <ex_readdir3res>;
    c->call (lbfs_NFSPROC3_READDIR, &rda, rdres,
	     wrap (this, &readdir_obj::installdir, timenow), 
	     lbfs_authof (sa));
  }

  void get_updated_copy (ptr<ex_getattr3res> res, time_t rqt, clnt_stat err) {
    if (res) {
      if (!err && res->status == NFS3_OK) {
	e->nfs_attr = *(res->attributes);
	e->set_exp (rqt, true);
      } else {
	xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
	delete this; return;
      }
    } 
    nfstime3 maxtime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
    if (greater (maxtime, e->ltime)) 
      readdir ();
    else {
      lbfs_readexist (fd, hh, e);
      delete this;
    }
  }

  ~readdir_obj ()
  {
    (*cb) ();
  }

  readdir_obj (int fd1, ref<xfs_message_header> h1, cache_entry *e1, 
               sfs_aid sa1, ref<aclnt> c1, readdir_obj::cb_t cb1) :
    cb(cb1), fd(fd1), c(c1), hh(h1), sa(sa1), e(e1)    
  {
    h = msgcast<xfs_message_open> (hh);
    uint32 owriters = e->writers;
    if ((h->tokens & XFS_OPEN_MASK) & (XFS_OPEN_NW|XFS_OPEN_EW)) {
      e->writers = 1;
    }
    if (!e->incache) {
      readdir ();
    } else {
      if (owriters > 0) {
	lbfs_readexist (fd, hh, e);
	delete this; return;
      } else {
	if (lbcd_trace > 1)
	  warn << "directory in cache, exp " << e->nfs_attr.expire << " " 
	       << (uint32) timenow << " ltime " << e->ltime.seconds << "\n";
	if (e->nfs_attr.expire < (uint32) timenow) {
	  lbfs_attr (fd, hh, sa, e->nh, c, 
		     wrap (this, &readdir_obj::get_updated_copy));
	} else get_updated_copy (NULL, 0, clnt_stat (0));
      }
    }   
  }
};

void 
lbfs_readdir (int fd, ref<xfs_message_header> h, cache_entry *e,
              sfs_aid sa, ref<aclnt> c, readdir_obj::cb_t cb)
{
  vNew readdir_obj (fd, h, e, sa, c, cb);
}

struct getfp_obj {
  typedef callback<void>::ref cb_t;
  cb_t cb;  
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_open *h;
  sfs_aid sa;
  cache_entry *e;
  lbfs_getfp3args gfa;
  int out_fd;
  str out_fname;
  uint bytes_written;
  uint total_bytes;
  bool eof;
  int retries;
  uint64 bytes_recv;
  uint getfps_sent, reads_sent;

  void installdata () 
  {
    struct xfs_message_installdata msg;
    struct xfs_message_header *h0 = NULL;
    size_t h0_len = 0;
    
    e->cache_name = out_fname;
    e->incache = true;

    int cfd = assign_cachefile (fd, h->header.sequence_num, e, 
				msg.cache_name, &msg.cache_handle);
    close (cfd);

    nfsobj2xfsnode (h->cred, e, &msg.node);
    msg.node.tokens |= XFS_OPEN_NR | XFS_DATA_R | XFS_OPEN_NW | XFS_DATA_W;
    msg.header.opcode = XFS_MSG_INSTALLDATA;
    h0 = (struct xfs_message_header *) &msg;
    h0_len = sizeof (msg);

    xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
				      h0, h0_len, NULL, 0);

    if (lbcd_trace) {
      warn << "***********************************\n";
      warn << "File name            = " << e->name << "\n";
      warn << "File data received   = " << bytes_recv << " bytes\n";
      warn << "File size            = " << e->nfs_attr.size << " bytes\n";
      warn << "GETFP RPCs sent      = " << getfps_sent << "\n";
      warn << "READ  RPCs sent      = " << reads_sent << "\n";
      total_bytes_recv += bytes_recv;
      total_read_filesize += e->nfs_attr.size;
      warn << "Total bytes received = " << total_bytes_recv << " bytes\n";
      warn << "Total read file size = " << total_read_filesize << " bytes\n";
    }
  }

  void write_file (uint64 cur_offst, uint32 size, ref<ex_read3res> rres)
  {
    if (lbcd_trace > 1)
      warn << "filename = " << out_fname << " offset = " << cur_offst << "\n";

    int err;
    if ((err = lseek (out_fd, cur_offst, SEEK_SET)) < 0) {
      if (lbcd_trace > 1)
	warn << "getfp_obj::write_file1: "
	     << out_fname << " " << strerror (errno) << "\n";
      delete this;
      return;
    } 

    if ((err = write (out_fd, rres->resok->data.base (), 
		      rres->resok->count)) < 0) {
      if (lbcd_trace > 1)
	warn << "getfp_obj::write_file2: " 
	     << out_fname << " " << strerror (errno) << "\n";
      delete this;
      return;
    } else
      if (err != (int) rres->resok->count) {
	if (lbcd_trace > 1)
	  warn << "write error or short write!!\n";
	delete this;
	return;
      }

    bytes_written += rres->resok->count;
    if (rres->resok->count < size)
      nfs3_read (cur_offst+rres->resok->count, size-rres->resok->count);
  }
  
  void gotdata
    (uint64 cur_offst, uint32 size, ref<ex_read3res> rres, clnt_stat err) 
  {
    if (!err && rres->status == NFS3_OK) {
      assert (rres->resok->file_attributes.present);
      bytes_recv += rres->resok->count;
      write_file (cur_offst, size, rres);
      if (bytes_written == total_bytes && eof) {
	installdata ();
	vec <chunk *> cvp;
	if (chunk_file(cvp, (char const *) out_fname) < 0) {
	  if (lbcd_trace > 1)
	    warn << "getfp::gotdata: " 
	         << strerror (errno) << "(" << errno << ")\n";
	  delete this;
	  return;
	}
	for (uint i = 0; i < cvp.size (); i++) {
	  if (lbcd_trace > 1)
	    warn << "adding fp = " << cvp[i]->hashidx() << " to lbfsdb\n";
	  cvp[i]->location().set_fh (e->nh);
	  lbfsdb.add_entry(cvp[i]->hashidx(), 
	                   &(cvp[i]->location()), cvp[i]->location().size());
	  delete cvp[i];
	}
	delete this;
	return;
      }
    } else {
      if (err && (retries++ < 1))
	nfs3_read (cur_offst, size);
      else {
	xfs_reply_err (fd, h->header.sequence_num, err ? err : rres->status);
	delete this;
      }
    }
  }

  void nfs3_read (uint64 cur_offst, uint32 size) 
  {
    read3args ra;
    ra.file = e->nh;
    ra.offset = cur_offst;
    ra.count = size < NFS_MAXDATA ? size : NFS_MAXDATA;
    if (lbcd_trace > 1)
      warn << "getfp_obj::nfs3_read @" << cur_offst << " +" << ra.count << "\n";

    reads_sent++;
    ref<ex_read3res> rres = New refcounted <ex_read3res>;
    c->call (lbfs_NFSPROC3_READ, &ra, rres,
	     wrap (this, &getfp_obj::gotdata, cur_offst, size, rres),
	     lbfs_authof (sa));
  }

  void copy_block (uint64 cur_offst, unsigned char *buf, chunk_location *c) 
  {
    if (lseek (out_fd, cur_offst, SEEK_SET) < 0) {
      if (lbcd_trace > 1)
	warn << "copy_block: error: " << strerror (errno) 
	     << "(" << errno << ")\n";
      xfs_reply_err (fd, h->header.sequence_num, EIO);
      delete this; return;
    }
    int err;
    if ((err = write (out_fd, buf, c->count ())) < 0
	|| ((uint32) err != c->count ())) {
      if (lbcd_trace > 1)
	warn << "getfp_obj::copy_block: error: " << err << " != " 
	     << c->count () << " or " << strerror (errno) << "\n";
      xfs_reply_err (fd, h->header.sequence_num, EIO);
      delete this; return;
    }
    bytes_written += c->count();
  }

  void compose_file (int offset, ref<lbfs_getfp3res> fpres) 
  {
    int err, chfd; 
    uint64 cur_offst = offset;
    fp_db::iterator * ci = NULL;
    bool found = false;
    nfs_fh3 fh;
    chunk_location c;
    cache_entry *e1 = NULL;
    
    for (uint i=0; i<fpres->resok->fprints.size(); i++) {
      found = false;
      unsigned char buf[fpres->resok->fprints[i].count];
      u_int64_t index;
      memmove(&index, fpres->resok->fprints[i].hash.base(), sizeof(index));
      if (!lbfsdb.get_iterator (index, &ci)) {
	if (ci && !(ci->get (&c))) {
	  do {
	    found = true;
	    c.get_fh (fh);
	    if (c.count () != fpres->resok->fprints[i].count) {
	      if (lbcd_trace > 1)
		warn << "chunk size != size from server..\n";
	      continue;
	    }
	    e1 = nfsindex[fh];
	    if (!e1) {
	      if (lbcd_trace > 1)
		warn << "compose_file: null fh or Can't find node handle\n";
	      found = false;
	      continue;
	    }    
	    if (lbcd_trace > 1)
	      warn << "reading chunks from " << e1->cache_name << "\n";
	    chfd = open (e1->cache_name, O_RDWR, 0666);
	    if (chfd < 0) {
	      if (lbcd_trace > 1)
		warn << "compose_file: error: " << strerror (errno) 
		     << "(" << errno << ")\n";
	      found = false;
	      continue;
	    }
	    if (lseek (chfd, c.pos (), SEEK_SET) < 0) {
	      if (lbcd_trace > 1)
		warn << "compose_file: error: " << strerror (errno) 
		     << "(" << errno << ")\n";
	      found = false;
	      continue;
	    }
	    if ((err = read (chfd, buf, c.count ())) > -1) {
	      if ((uint32) err != c.count ()) {
		if (lbcd_trace > 1)
		  warn << "compose_file: error: " << err << " != " 
		       << c.count () << "\n";
	        found = false;
	        continue;
	      }
	      if (compare_sha1_hash (buf, c.count (),
				     fpres->resok->fprints[i].hash)) {
		if (lbcd_trace > 1)
		  warn << "compose_file: sha1 hash mismatch\n";		
		found = false;
	        continue;
	      }
	    } else {
	      if (lbcd_trace > 1)
		warn << "compose_file: error: " << strerror (errno) 
		     << "(" << errno << ")\n";
	      found = false;
	      continue;
	    }
	    close (chfd);
	    if (found) {
	      if (lbcd_trace > 1)
		warn << "FOUND!! getfp_obj::compose_file: fp = " 
		     << index
		     << " in client DB\n";
	      copy_block (cur_offst, buf, &c);
	    }
	  } while (!found && !(ci->next (&c)));
	}
      	delete ci;
      }
      if (!found) {
	if (lbcd_trace > 1)
	  warn << "compose_file: fp = "
	       << index << " not in DB\n";
#if 1
	int want = fpres->resok->fprints[i].count;
	uint64 want_pos = cur_offst;
	while(want > 0) {
	  int get = want > NFS_MAXDATA ? NFS_MAXDATA : want;
	  if (lbcd_trace > 1)
	    warn << "reading " << want_pos << "+" << get << "\n";
	  nfs3_read (want_pos, get);
	  want_pos += get;
	  want -= get;
	}
#else
	if (lbcd_trace > 1)
	  warn << "reading " << cur_offst << "+"
	       << fpres->resok->fprints[i].count << "\n";
	nfs3_read(cur_offst, fpres->resok->fprints[i].count);
#endif
      }
      cur_offst += fpres->resok->fprints[i].count;
    }
    if (bytes_written == total_bytes && eof) {
      installdata ();
      delete this; return;
    }
  }

  void gotfp(uint64 offset, ref<lbfs_getfp3res> fpres,
             time_t rqt, clnt_stat err) 
  {
    if (!err && fpres->status == NFS3_OK) {
      e->nfs_attr = *(fpres->resok->file_attributes.attributes);
      e->set_exp (rqt);
      e->ltime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
      
      eof = fpres->resok->eof;
      int prev_offset = offset;
      for (uint i=0; i < fpres->resok->fprints.size (); i++) {
        offset += fpres->resok->fprints[i].count;
	total_bytes += fpres->resok->fprints[i].count;
      }
      
      if (!eof) {
	gfa.offset = offset;
	if (fpres->resok->fprints.size () == 0)
	  gfa.count *= 2;

	getfps_sent++;
	ref<lbfs_getfp3res> res = New refcounted <lbfs_getfp3res>;
	c->call (lbfs_GETFP, &gfa, res, 
		 wrap (this, &getfp_obj::gotfp, gfa.offset, res, timenow),
		 lbfs_authof (sa));
      }
      compose_file (prev_offset, fpres);
    } else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : fpres->status);      
      delete this;
    }
  }

  ~getfp_obj () 
  { 
    if (out_fd) close(out_fd);
    (*cb) ();
    lbfsdb_is_dirty = true;
  }

  getfp_obj (int fd1, ref<xfs_message_header> h1, cache_entry *e1, 
             sfs_aid sa1, ref<aclnt> c1, getfp_obj::cb_t cb1) :
    cb(cb1), fd(fd1), c(c1), hh(h1), sa(sa1), e(e1), bytes_written(0),
    total_bytes(0), eof(false), retries(0), bytes_recv(0), getfps_sent(0),
    reads_sent(0)    
  {
    h = msgcast<xfs_message_open> (hh);
    str fhstr = armor32(e->nh.data.base(), e->nh.data.size());
    int r = rnd.getword();
    str rstr = armor32((void*)&r, sizeof(int));
    out_fname = strbuf("cache/%02X/sfslbcd.%s.%s", 
		       e->xh.a >> 8, fhstr.cstr(), rstr.cstr());

    out_fd = open (out_fname, O_CREAT | O_WRONLY, 0666);
    if (out_fd < 0) {
      if (lbcd_trace > 1)
	warn << "getfp_obj::getfp_obj: " << strerror (errno) << "\n";
      xfs_reply_err (fd, h->header.sequence_num, EIO);
      delete this; return;
    }
    gfa.file = e->nh;
    gfa.offset = 0;
    gfa.count = LBFS_MAXDATA;
    
    getfps_sent++;
    ref<lbfs_getfp3res> fpres = New refcounted <lbfs_getfp3res>;
    c->call (lbfs_GETFP, &gfa, fpres,
	     wrap (this, &getfp_obj::gotfp, gfa.offset, fpres, timenow),
	     lbfs_authof (sa));
  }
};

void
lbfs_getfp (int fd, ref<xfs_message_header> h, cache_entry *e, sfs_aid sa, 
	    ref<aclnt> c, getfp_obj::cb_t cb)
{
  vNew getfp_obj (fd, h, e, sa, c, cb);
}

struct read_obj {
  static const int OUTSTANDING_READS = 16;
  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_open *h;
  sfs_aid sa;
  cache_entry *e;
  bool eof;
  int out_fd;
  struct xfs_message_installdata msg;
  struct xfs_message_header *h0;
  size_t h0_len;
  size_t next_offset;
  bool notified;
  int readrpc_window;
  unsigned outstanding_reads;

  void done()
  {
    if (outstanding_reads == 0)
      delete this;
  }
    
  void compose_file (uint64 offset, ref<ex_read3res> res)
  {
    if (lseek (out_fd, offset, SEEK_SET) < 0) {
      xfs_reply_err(fd, h->header.sequence_num, errno);
      delete this; return;
    }
    int err = write (out_fd, res->resok->data.base (), res->resok->count);
    if (err < 0 || (uint)err != res->resok->count) {
      if (err >= 0)
	warn << "short write..wierd\n";
      xfs_reply_err(fd, h->header.sequence_num, errno);
      delete this; return;      
    }
  }

  void gotdata (uint64 offset, size_t size, 
                ref<ex_read3res> res, time_t rqt, clnt_stat err)
  {
    outstanding_reads--;
    if (!notified) {
      if (!err && res->status == NFS3_OK) {
        assert (res->resok->file_attributes.present);
        e->nfs_attr = *(res->resok->file_attributes.attributes);
        e->set_exp (rqt);
        e->ltime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);      
        compose_file (offset, res);
        if (res->resok->eof)
	  eof = true;
  
        if (res->resok->count < size && !res->resok->eof) {
	  nfs3_read(offset+res->resok->count, size-res->resok->count);
	  return;
        }
        else if (!eof) {
          int maxwin = 1+((e->nfs_attr.size-next_offset)/NFS_MAXDATA);
	  nfs3_read(next_offset, NFS_MAXDATA);
	  next_offset+=NFS_MAXDATA;
	  if (lbcd_trace > 0 &&
	      readrpc_window < OUTSTANDING_READS && readrpc_window < maxwin)
	    warn << "increasing read window from " << readrpc_window 
	         << " to " << maxwin << "\n";
	  for (; readrpc_window < maxwin && readrpc_window < OUTSTANDING_READS;
	       readrpc_window++) {
	    nfs3_read(next_offset, NFS_MAXDATA);
	    next_offset+=NFS_MAXDATA;
	  }
	  return;
        }
        else if (outstanding_reads == 0) {
	  close (out_fd);
	  nfsobj2xfsnode (h->cred, e, &msg.node);
	  msg.node.tokens |= XFS_OPEN_NR|XFS_DATA_R|XFS_OPEN_NW|XFS_DATA_W;
	  msg.header.opcode = XFS_MSG_INSTALLDATA;
	  h0 = (struct xfs_message_header *) &msg;
	  h0_len = sizeof (msg);
	  e->incache = true;
	  xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					    h0, h0_len, NULL, 0);
	  notified = true;
        }
      } else { 
        xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
        notified = true;
      }
    }
    done();
  }

  void nfs3_read(uint64 offset, unsigned size)
  {
    read3args ra;
    ra.file = e->nh;
    ra.offset = offset;
    ra.count = size;
    ref<ex_read3res> rres = New refcounted <ex_read3res>;
    outstanding_reads++;
    c->call (lbfs_NFSPROC3_READ, &ra, rres,
	     wrap(this, &read_obj::gotdata, ra.offset, ra.count, rres, timenow),
	     lbfs_authof (sa));
  }

  ~read_obj ()
  {
    if (out_fd) close (out_fd);
    (*cb) ();
  }

  read_obj (int fd1, ref< xfs_message_header> h1, cache_entry *e1, sfs_aid sa1, 
	    ref<aclnt> c1, read_obj::cb_t cb1)
    : cb(cb1), fd(fd1), c(c1), hh(h1), sa(sa1), e(e1),
      eof(false), notified(false), outstanding_reads(0)
  {
    h = msgcast<xfs_message_open> (hh);
    out_fd = assign_cachefile (fd, h->header.sequence_num, e, 
			       msg.cache_name, &msg.cache_handle, 
			       O_CREAT | O_WRONLY | O_TRUNC);
    nfs3_read(0, NFS_MAXDATA);
    nfs3_read(NFS_MAXDATA, NFS_MAXDATA);
    readrpc_window = 2;
    warn << "readrpc window starts at 2\n";
    next_offset = NFS_MAXDATA*2;
  }
};

void 
lbfs_read (int fd, ref<xfs_message_header> h, cache_entry *e, sfs_aid sa, 
	    ref<aclnt> c, read_obj::cb_t cb)
{
  vNew read_obj (fd, h, e, sa, c, cb);
}

struct readfile_obj {
  typedef callback<void>::ref cb_t;
  cb_t cb;
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_open *h;
  sfs_aid sa;
  cache_entry *e;

  void get_updated_copy (ptr<ex_getattr3res> res, time_t rqt, clnt_stat err) {
    if (res) {
      if (!err && res->status == NFS3_OK) {
	e->nfs_attr = *(res->attributes);
	e->set_exp (rqt, true);
      } else {
	xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
        (*cb) ();
	delete this;
	return;
      }
    } 
    nfstime3 maxtime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
    if (greater (maxtime, e->ltime)) {
      if (lbcd_trace > 0)
	warn << e->name << " m/ctime greater than ltime, update\n";
      if (lbfs > 1)
	lbfs_getfp (fd, hh, e, sa, c, cb);
      else
	lbfs_read (fd, hh, e, sa, c, cb);
      delete this;
      return;
    }
    else {
      lbfs_readexist (fd, hh, e);
      (*cb) ();
      delete this;
      return;
    }
  }
  
  ~readfile_obj () 
  {
  }

  readfile_obj (int fd1, ref<xfs_message_header> h1, cache_entry *e1,
                sfs_aid sa1, ref<aclnt> c1, readfile_obj::cb_t cb1) :
    cb(cb1), fd(fd1), c(c1), hh(h1), sa(sa1), e(e1)    
  {
    h = msgcast<xfs_message_open> (hh);
    uint32 owriters = e->writers;
    if ((h->tokens & XFS_OPEN_MASK) & (XFS_OPEN_NW|XFS_OPEN_EW)) {
      e->writers = 1;
      if (lbcd_trace > 1)
	warn << "open for write: " << e->writers << " writers\n";
    }
    
    if (!e->incache) {
      if (lbcd_trace > 0)
        warn << e->name << " not in cache, need to read\n";
      if (lbfs > 1)
	lbfs_getfp (fd, hh, e, sa, c, cb);
      else
	lbfs_read (fd, hh, e, sa, c, cb);
      delete this;
      return;
    }
    else {
      if (owriters > 0) {
	lbfs_readexist (fd, hh, e);
        (*cb) ();
	delete this;
	return;
      } else {
	if (e->nfs_attr.expire < (uint32) timenow) {
	  lbfs_attr (fd, hh, sa, e->nh, c,
		     wrap (this, &readfile_obj::get_updated_copy));
	}
	else
	  get_updated_copy (NULL, 0, clnt_stat (0));
      }
    }
  }
};

void 
lbfs_readfile (int fd, ref<xfs_message_header> h, cache_entry *e, sfs_aid sa, 
	      ref<aclnt> c, readfile_obj::cb_t cb) 
{
  vNew readfile_obj (fd, h, e, sa, c, cb);
}

struct open_obj {
private:
  PRIVDEST ~open_obj () {}
public:
  int fd;
  ref<aclnt> c;

  ref<xfs_message_header> hh;
  xfs_message_open *h;
  sfs_aid sa;
  cache_entry *e;

  void done () 
  {
    delete this;
  }

  open_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), hh(h1), sa(sa1)
  {
    h = msgcast<xfs_message_open> (hh);
    e = xfsindex[h->handle];
    if (!e) {
      if (lbcd_trace > 1)
	warn << h->header.sequence_num << ":"  
	     << "open_obj: Can't find node handle\n";
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      delete this; return;
    } else {
      switch (e->nfs_attr.type) {
      case NF3DIR:
	lbfs_readdir (fd, h1, e, sa, c, wrap (this, &open_obj::done));
	break;
      case NF3LNK:
	lbfs_readlink (fd, h1, e, sa, c, wrap (this, &open_obj::done)); 
	break;
      case NF3REG:
	lbfs_readfile (fd, h1, e, sa, c, wrap (this, &open_obj::done)); 
	break;
      default:
	warn << h->header.sequence_num << ":"  
	     << "open_obj: File type " << e->nfs_attr.type
	     << " not handled\n";
	break;
      }
    }
  }
};

void 
lbfs_open (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c) 
{
  vNew open_obj (fd, h, sa, c);
}

struct create_obj {
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_create *h;
  sfs_aid sa;
  cache_entry *e;
  ptr<ex_diropres3> res;
  
  void install (time_t rqt, clnt_stat err) 
  {
    if (!err && res->status == NFS3_OK) {
      struct xfs_message_installdata msg1; //change content of parent dir
      struct xfs_message_installnode msg2; //New file node
      struct xfs_message_installdata msg3; //New file content (null)
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;
      struct xfs_message_header *h2 = NULL;
      size_t h2_len = 0;

      assert (res->resok->obj.present && res->resok->obj_attributes.present);
      cache_entry *e1 = update_cache (*res->resok->obj.handle, 
				      *res->resok->obj_attributes.attributes,
				      h->name);
      e1->set_exp (rqt);
      xfs_cred cred = h->cred;
      if (e1->nfs_attr.type == NF3DIR) {
	struct xfs_message_mkdir *hm = (xfs_message_mkdir *) h;
	cred = hm->cred;
      }
      nfsobj2xfsnode (cred, e1, &msg2.node);
    
      int cfd = assign_cachefile (fd, h->header.sequence_num, e1, 
				  msg3.cache_name, &msg3.cache_handle);
      close (cfd);
      
      int parent_fd = assign_cachefile (fd, h->header.sequence_num, e, 
					msg1.cache_name, &msg1.cache_handle,
					O_CREAT | O_WRONLY | O_APPEND);
      e->incache = false;
      close (parent_fd);
      assert (res->resok->dir_wcc.after.present);
      e->nfs_attr = *(res->resok->dir_wcc.after.attributes);
      e->set_exp (rqt, true);
      e->ltime = max(e->nfs_attr.mtime, e->nfs_attr.ctime);
      nfsobj2xfsnode (cred, e, &msg1.node);

      msg1.flag = 0; //XFS_ID_INVALID_DNLC;
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);
      
      msg2.node.tokens = XFS_ATTR_R
	| XFS_OPEN_NW | XFS_OPEN_NR
	| XFS_DATA_R | XFS_DATA_W;      
      msg2.parent_handle = h->parent_handle;
      strlcpy (msg2.name, h->name, sizeof (msg2.name));
    
      msg2.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);

      if (e1->nfs_attr.type == NF3REG) {
	e1->incache = true;
	e1->writers = 1;
      }
      msg3.node = msg2.node;
      msg3.flag = 0;
      msg3.header.opcode = XFS_MSG_INSTALLDATA;
      
      h2 = (struct xfs_message_header *) &msg3;
      h2_len = sizeof (msg3);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					0, h0, h0_len, h1, h1_len, h2, h2_len,
					NULL, 0);
    } else xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
    delete this;
  }

  void do_mkdir ()
  {
    struct xfs_message_mkdir *h1 = (xfs_message_mkdir *) h;    
    mkdir3args ma;
    ma.where.dir = e->nh;
    ma.where.name = h1->name;
    xfsattr2nfsattr (h1->header.opcode, h1->attr, &ma.attributes);

    res = New refcounted <ex_diropres3>;
    c->call (lbfs_NFSPROC3_MKDIR, &ma, res,
	     wrap (this, &create_obj::install, timenow), lbfs_authof (sa));
  }

  void do_create ()
  {
    create3args ca;
    ca.where.dir = e->nh;
    ca.where.name = h->name;
    ca.how.set_mode (GUARDED);
    assert (ca.how.mode == UNCHECKED || ca.how.mode == GUARDED);
    xfsattr2nfsattr (h->header.opcode, h->attr, &(*ca.how.obj_attributes));

    res = New refcounted <ex_diropres3>;
    c->call (lbfs_NFSPROC3_CREATE, &ca, res,
		wrap (this, &create_obj::install, timenow), lbfs_authof (sa));
  }

  create_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, 
	      ref<aclnt> c1) :
    fd(fd1), c(c1), hh(h1), sa(sa1)
  {
    h = msgcast<xfs_message_create> (hh);
    e = xfsindex[h->parent_handle];
    if (!e) {
      if (lbcd_trace > 1)
	warn << "create_obj: Can't find parent_handle\n";
      xfs_reply_err (fd, h->header.sequence_num, ENOENT);
      delete this; return;
    }

    switch (h->header.opcode) {
    case XFS_MSG_CREATE:
      do_create ();
      break;
    case XFS_MSG_MKDIR:
      do_mkdir ();
      break;
    }
  }
};

void 
lbfs_create (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c)
{
  vNew create_obj (fd, h, sa, c);
}

struct link_obj {
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_link *h;
  sfs_aid sa;
  cache_entry *e1, *e2;
  ptr<ex_link3res> res;

  void do_link (time_t rqt, clnt_stat err) 
  {
    if (!err && res->status == NFS3_OK) {
      struct xfs_message_installdata msg1; //update parent dir's data
      struct xfs_message_installnode msg2; //update attr of from_handle
      
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;

      assert (res->res->linkdir_wcc.after.present);
      e2->nfs_attr = *(res->res->linkdir_wcc.after.attributes);
      e2->set_exp (rqt, true);
      nfsobj2xfsnode (h->cred, e2, &msg1.node);
      msg1.flag = 0;

      assert (res->res->file_attributes.present);
      e1->nfs_attr = *(res->res->file_attributes.attributes);
      e1->set_exp (rqt, e1->nfs_attr.type == NF3DIR ? true : false);
      nfsobj2xfsnode (h->cred, e1, &msg2.node);

      int parent_fd = assign_cachefile (fd, h->header.sequence_num, e2, 
					msg1.cache_name, &msg1.cache_handle,
					O_CREAT | O_WRONLY | O_APPEND);
      e2->incache = false; // sad mtime update problem with openbsd nfsd
      close(parent_fd);
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);

      msg2.node.tokens = XFS_ATTR_R;
      msg2.parent_handle = h->parent_handle;
      strcpy (msg2.name, h->name);

      msg2.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);
    
      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num, 0,
					h0, h0_len, h1, h1_len, NULL, 0);
    } 
    else
      xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
    delete this;
  }

  link_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, 
	      ref<aclnt> c1) : fd(fd1), c(c1), hh(h1), sa(sa1) 
  {
    h = msgcast<xfs_message_link> (hh);
    e1 = xfsindex[h->from_handle];
    if (!e1) {
      if (lbcd_trace > 1)
	warn << "link_obj: Can't find from_handle\n";
      xfs_reply_err (fd, h->header.sequence_num, ENOENT);
      delete this; return;
    }
    link3args la;
    la.file = e1->nh;

    e2 = xfsindex[h->parent_handle];
    if (!e2) {
      if (lbcd_trace > 1)
	warn << "xfs_message_link: Can't find parent_handle\n";
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      delete this; return;
    }
    la.link.dir = e2->nh;
    la.link.name = h->name;

    res = New refcounted < ex_link3res >;
    c->call (lbfs_NFSPROC3_LINK, &la, res,
		wrap (this, &link_obj::do_link, timenow), lbfs_authof (sa));    
  }

};

void 
lbfs_link (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c)
{
  vNew link_obj (fd, h, sa, c);
}

struct symlink_obj {
  int fd;
  ref<aclnt> c;

  ref<xfs_message_header> hh;
  xfs_message_symlink *h;
  sfs_aid sa;
  cache_entry *e;
#if OLD
  ptr<ex_diropres3> res;
#endif
  
  void do_symlink (ex_diropres3 *res, time_t rqt, clnt_stat err) 
  {
    if (!err && res->status == NFS3_OK) {
      struct xfs_message_installdata msg1;	//install change in parent dir
      struct xfs_message_installnode msg2;	//install symlink node

      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;

      assert (res->resok->dir_wcc.after.present);
      e->nfs_attr = *(res->resok->dir_wcc.after.attributes);

      assert (res->resok->obj.present && res->resok->obj_attributes.present);
      cache_entry *e2 = update_cache (*(res->resok->obj.handle), 
				      *res->resok->obj_attributes.attributes, 
				      h->name);

      int parent_fd = assign_cachefile (fd, h->header.sequence_num, e, 
					msg1.cache_name, &msg1.cache_handle,
					O_CREAT | O_WRONLY | O_APPEND);
      e->incache = false; // sad mtime update problem with openbsd nfsd
      close (parent_fd);

      nfsobj2xfsnode (h->cred, e, &msg1.node);
      msg1.flag = 0;
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);
      
      e2->set_exp (rqt);
      nfsobj2xfsnode (h->cred, e2, &msg2.node);
      msg2.node.tokens = XFS_ATTR_R;
      msg2.parent_handle = h->parent_handle;
      strlcpy (msg2.name, h->name, sizeof (msg2.name));
      
      msg2.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg2;
      h1_len = sizeof (msg2);

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					0, h0, h0_len, h1, h1_len, NULL, 0);   
      //
      xdr_delete (lbfs_program_3.tbl[lbfs_NFSPROC3_LINK].xdr_res, res);
    } else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
    }
    delete this;
  }

  symlink_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, 
	       ref<aclnt> c1) : 
    fd(fd1), c(c1), hh(h1), sa(sa1)
  {
    h = msgcast<xfs_message_symlink> (hh);
    e = xfsindex[h->parent_handle];
    if (!e) {
      if (lbcd_trace > 1)
	warn << "symlink_obj: Can't find parent handle\n";
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      delete this; return;
    }

    symlink3args sla;
    sla.where.dir = e->nh;
    sla.where.name = h->name;
    xfsattr2nfsattr (h->header.opcode, h->attr, 
		     &(sla.symlink.symlink_attributes));
    sla.symlink.symlink_data.setbuf (h->contents, strlen (h->contents));
#if OLD
    res = New refcounted <ex_diropres3>;
    c->call (lbfs_NFSPROC3_SYMLINK, &sla, res,
	     wrap (this, &symlink_obj::do_symlink, timenow), lbfs_authof (sa));
#else
    void *res = lbfs_program_3.tbl[lbfs_NFSPROC3_SYMLINK].alloc_res ();
    c->call (lbfs_NFSPROC3_SYMLINK, &sla, res, 
	     wrap (&process_reply, res, 
		   wrap (this, &symlink_obj::do_symlink, (ex_diropres3 *) res, timenow)),
	     lbfs_authof (sa));
#endif
  }
};

void 
lbfs_symlink (int fd, ref<xfs_message_header> h, sfs_aid sa, 
	      ref<aclnt> c)
{
  vNew symlink_obj (fd, h, sa, c);
}

struct remove_obj {
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_remove *h;
  sfs_aid sa;
  time_t rqt1;
  cache_entry *e1, *e2;
  diropargs3 doa;
  ptr<ex_lookup3res> lres;
  ptr<ex_wccstat3> wres;

  void install (time_t rqt, clnt_stat err) 
  {
    if (!err && wres->status == NFS3_OK) {

      assert (wres->wcc->after.present);

      struct xfs_message_installdata msg1;
      struct xfs_message_header *h0 = NULL;
      size_t h0_len = 0;
      struct xfs_message_installattr msg2;
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;
      
      int pfd1 = assign_cachefile (fd, h->header.sequence_num, e1, 
					msg1.cache_name, &msg1.cache_handle,
					O_CREAT | O_RDWR);
      int pfd2 = open (msg1.cache_name, O_WRONLY, 0666);
      e1->incache = false;
      close (pfd1);
      close (pfd2);

      nfsobj2xfsnode (h->cred, e1, &msg1.node);
      e1->nfs_attr = *(wres->wcc->after.attributes);
      e1->set_exp (rqt, true);
      e1->ltime = max(e1->nfs_attr.mtime, e1->nfs_attr.ctime);
      msg1.node.tokens |= XFS_DATA_R;
      msg1.flag = XFS_ID_INVALID_DNLC;
    
      msg1.header.opcode = XFS_MSG_INSTALLDATA;
      h0 = (struct xfs_message_header *) &msg1;
      h0_len = sizeof (msg1);

      assert (lres->resok->obj_attributes.present ||
	      lres->resok->dir_attributes.present);
      ex_post_op_attr a = lres->resok->obj_attributes.present ? 
	lres->resok->obj_attributes : lres->resok->dir_attributes;
      if ((a.attributes->type == NF3DIR && a.attributes->nlink > 2) ||
	  (a.attributes->type == NF3REG && a.attributes->nlink > 1)) {
	e2 = nfsindex[lres->resok->object];
	if (!e2) {
	  if (lbcd_trace > 1)
	    warn << "remove_obj::install: Can't find handle\n";
	  delete this; return;
	}
	
	msg2.header.opcode = XFS_MSG_INSTALLATTR;
	--(a.attributes->nlink);
	e2->nfs_attr = *a.attributes;
	e2->set_exp (rqt1, e2->nfs_attr.type == NF3DIR); 
	nfsobj2xfsnode (h->cred, e2, &msg2.node);
	h1 = (struct xfs_message_header *) &msg2;
	h1_len = sizeof (msg2);
	
	xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					  0, h0, h0_len, h1, h1_len,
					  NULL, 0);
      } else 
	xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					  0, h0, h0_len, NULL, 0);      
    } 
    else
      xfs_reply_err (fd, h->header.sequence_num, err ? err : wres->status);

    delete this;
  }

  void do_remove (time_t rqt, clnt_stat err) 
  {
    if (!err && lres->status == NFS3_OK) {
      rqt1 = rqt;
      wres =  New refcounted <ex_wccstat3>;
      switch (h->header.opcode) {
      case XFS_MSG_REMOVE:
	c->call (lbfs_NFSPROC3_REMOVE, &doa, wres,
		 wrap (this, &remove_obj::install, timenow), lbfs_authof (sa));
	break;
      case XFS_MSG_RMDIR:
	c->call (lbfs_NFSPROC3_RMDIR, &doa, wres,
		 wrap (this, &remove_obj::install, timenow), lbfs_authof (sa));
      }	
    } else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : wres->status);
      delete this; return;
    }
  }

  remove_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), hh(h1), sa(sa1) 
  {
    h = msgcast<xfs_message_remove> (hh);
    e1 = xfsindex[h->parent_handle];
    if (!e1) {
      if (lbcd_trace > 1)
	warn << "xfs_message_remove: Can't find parent_handle\n";
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      delete this; return;
    }

    doa.dir = e1->nh;
    doa.name = h->name;

    lres = New refcounted <ex_lookup3res>;
    c->call (lbfs_NFSPROC3_LOOKUP, &doa, lres,
	     wrap (this, &remove_obj::do_remove, timenow), lbfs_authof (sa));
  }
};

void
lbfs_remove (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c)
{
  vNew remove_obj (fd, h, sa, c);
}

struct rename_obj {
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_rename *h;
  sfs_aid sa;
  time_t rqt1, rqt2;
  cache_entry *e1, *e2, *e3;
  ptr<ex_lookup3res> lres;
  ptr<ex_rename3res> rnres;
  ptr<ex_getattr3res> gares;

  void update_attr (ex_fattr3 attr1, ex_fattr3 attr2)
  {
    nfstime3 cache_time = e3->ltime;
    attr2.expire += rqt2;
    e3->nfs_attr = attr2;
    if (!greater(attr2.mtime, attr1.mtime) &&
	!greater(attr1.mtime, cache_time))
      e3->ltime = attr2.mtime;
  }

  void do_install (time_t rqt, clnt_stat err) 
  {
    if (!err && gares->status == NFS3_OK) {
      struct xfs_message_installnode msg1;	//update attr of file renamed 
      struct xfs_message_installdata msg2;	//new parent dir content
      struct xfs_message_installdata msg3;	//old parent dir content
      struct xfs_message_header *h1 = NULL;
      size_t h1_len = 0;
      struct xfs_message_header *h2 = NULL;
      size_t h2_len = 0;
      struct xfs_message_header *h3 = NULL;
      size_t h3_len = 0;
      
      e3 = nfsindex[lres->resok->object];
      if (!e3) {
	if (lbcd_trace > 1)
	  warn << "rename_obj::do_install: Can't find file handle\n";
	xfs_reply_err(fd, h->header.sequence_num, ENOENT);
	delete this; return;
      }
      ex_fattr3 lattr = lres->resok->obj_attributes.present ?
	*(lres->resok->obj_attributes.attributes) : 
	*(lres->resok->dir_attributes.attributes);
      update_attr (lattr, *gares->attributes);
      
      nfsobj2xfsnode (h->cred, e3, &msg1.node);
      msg1.parent_handle = h->new_parent_handle;
      strlcpy (msg1.name, h->new_name, sizeof (msg1.name));

      msg1.header.opcode = XFS_MSG_INSTALLNODE;
      h1 = (struct xfs_message_header *) &msg1;
      h1_len = sizeof (msg1);
      
      int parent_fd = assign_cachefile (fd, h->header.sequence_num, e2,
	                                msg2.cache_name, &msg2.cache_handle);
      e2->incache = false;
      close (parent_fd);

      assert (rnres->res->todir_wcc.after.present);
      e2->nfs_attr = *(rnres->res->todir_wcc.after.attributes);
      nfsobj2xfsnode (h->cred, e2, &msg2.node);
      e2->incache = false; // sad mtime update problem with openbsd nfsd
      msg2.flag = XFS_ID_INVALID_DNLC;
      msg2.header.opcode = XFS_MSG_INSTALLDATA;
      h2 = (struct xfs_message_header *) &msg2;
      h2_len = sizeof (msg2);

      if (!xfs_handle_eq (&h->old_parent_handle,
			  &h->new_parent_handle)) {
	parent_fd = assign_cachefile(fd, h->header.sequence_num, e1,
	                             msg3.cache_name, &msg3.cache_handle);
	close (parent_fd);

	assert (rnres->res->fromdir_wcc.after.present);
	e1->nfs_attr = *(rnres->res->fromdir_wcc.after.attributes);
	nfsobj2xfsnode (h->cred, e1, &msg3.node);
	
	e1->incache = false; // sad mtime update problem with openbsd nfsd
	msg3.flag = XFS_ID_INVALID_DNLC;
	msg3.header.opcode = XFS_MSG_INSTALLDATA;
	h3 = (struct xfs_message_header *) &msg3;
	h3_len = sizeof (msg3);
      }

      xfs_send_message_wakeup_multiple (fd, h->header.sequence_num,
					0, h1, h1_len, h2, h2_len,
					h3, h3_len, NULL, 0);
    } else
      xfs_reply_err (fd, h->header.sequence_num, err ? err : gares->status);
    delete this;
  }

  void do_rename (clnt_stat err) 
  {
    if (!err && rnres->status == NFS3_OK) {
      gares = New refcounted <ex_getattr3res>;
      c->call (lbfs_NFSPROC3_GETATTR, &lres->resok->object, gares,
	       wrap (this, &rename_obj::do_install, timenow),
	       lbfs_authof (sa));
    } else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : rnres->status);
      delete this;
    }
  }

  void do_lookup (time_t rqt, clnt_stat err) 
  {
    if (!err && lres->status == NFS3_OK) {

      rqt1 = rqt;
      e2 = xfsindex[h->new_parent_handle];
      if (!e2) {
	if (lbcd_trace > 1)
	  warn << "rename_obj::do_lookup: Can't find new_parent_handle\n";
	xfs_reply_err(fd, h->header.sequence_num, ENOENT);
	delete this; return;
      }   
      
      rename3args rna;
      rna.from.dir = e1->nh;
      rna.from.name = h->old_name;
      rna.to.dir = e2->nh;
      rna.to.name = h->new_name;
      
      rnres = New refcounted <ex_rename3res>;
      c->call (lbfs_NFSPROC3_RENAME, &rna, rnres,
	       wrap (this, &rename_obj::do_rename), lbfs_authof (sa));
    } else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : lres->status);
      delete this;
    }
  }

  rename_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), hh(h1), sa(sa1) 
  {
    h = msgcast<xfs_message_rename> (hh);
    e1 =  xfsindex[h->old_parent_handle];
    if (!e1) {
      if (lbcd_trace > 1)
	warn << "rename_obj: Can't find old_parent_handle\n";
      xfs_reply_err(fd, h->header.sequence_num, ENOENT);
      delete this;
      return;
    }
    
    diropargs3 doa;
    doa.dir = e1->nh;
    doa.name = h->old_name;
    
    lres = New refcounted <ex_lookup3res>;
    c->call (lbfs_NFSPROC3_LOOKUP, &doa, lres,
	     wrap (this, &rename_obj::do_lookup, timenow), lbfs_authof (sa));
  }    
};

void
lbfs_rename (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c)
{
  vNew rename_obj (fd, h, sa, c);
}

struct putdata_obj {
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_putdata *h;
  sfs_aid sa;
  cache_entry *e;
  nfs_fh3 tmpfh;
  unsigned tmpfd;
  uint blocks_written;
  uint total_blocks;
  Chunker *chunker;
  bool eof;
  int retries;
  bool committed;
  off_t cur_pos;
  const int OUTSTANDING_CONDWRITES;
  int outstanding_condwrites;
  ptr<ex_diropres3> mtres;
  uint64 bytes_sent;
  uint condwrites_sent, writes_sent;
  
  void do_committmp (ref<ex_commit3res> res, time_t rqtime, clnt_stat err)
  {
    if (!err && res->status == NFS3_OK) {
      committed = true;
      ex_fattr3 attr = *(res->resok->file_wcc.after.attributes);
      e->nfs_attr = attr;
      e->set_exp (rqtime, attr.type == NF3DIR);
      e->ltime = max(attr.mtime, attr.ctime);
      xfs_send_message_wakeup (fd, h->header.sequence_num, 0);    
      if (lbcd_trace) {
	warn << "***********************************\n";
	warn << "File name             = " << e->name << "\n";
	warn << "File data sent        = " << bytes_sent << " bytes\n";
	warn << "File size             = " << e->nfs_attr.size << " bytes\n";
	warn << "CONDWRITE RPCs sent   = " << condwrites_sent << "\n";
	warn << "WRITE RPCs sent       = " << writes_sent << "\n";
	total_bytes_sent += bytes_sent;
	total_write_filesize += e->nfs_attr.size;
	warn << "Total bytes sent      = " << total_bytes_sent << " bytes\n";
	warn << "Total write file size = "
	     << total_write_filesize << " bytes\n";
      }
     } else
      xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
    delete this;
  }

  void sendcommittmp () 
  {
    lbfs_committmp3args ct;
    ct.commit_to = e->nh;
    ct.fd = tmpfd;
    if (lbcd_trace > 1)
      warn << h->header.sequence_num << " COMMITTMP: "
	   << blocks_written << " blocks written " 
	   << total_blocks << " needed, eof? "
	   << eof << "\n";

    ref<ex_commit3res> cres = New refcounted <ex_commit3res>;
    c->call (lbfs_COMMITTMP, &ct, cres,
	     wrap (this, &putdata_obj::do_committmp, cres, timenow),
	     lbfs_authof (sa));
  }

  void do_sendwrite (chunk *chunk, ref<ex_write3res> res, clnt_stat err)
  {
    if (outstanding_condwrites > 0) outstanding_condwrites--;
    if (!err && res->status == NFS3_OK) {
      if (lbcd_trace > 1)
	warn << h->header.sequence_num << " do_sendwrite: @"
	     << chunk->location().pos() << ", "
	     << res->resok->count << " total needed "
	     << chunk->location().count() << "\n"; 
      if (lbcd_trace > 1)
	warn << h->header.sequence_num << " nfs3_write: @"
	     << chunk->location().pos() << " +"
	     << chunk->location().count() << " "
	     << blocks_written << " blocks written " 
	     << total_blocks << " needed, eof? "
	     << eof << "\n";
    } else {
      if (err && retries < 1) {
	sendwrite (chunk);
	retries++;
      } else {
	xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
	delete this; return;
      }
    }
    if (!eof && outstanding_condwrites < OUTSTANDING_CONDWRITES) 
      condwrite_chunk();    
  }

  void sendwrite (chunk *chunk)
  {
    int err, ost;
    uint64 offst = chunk->location().pos ();
    uint32 count = chunk->location().count ();

    assert (!committed);

    int rfd = open (e->cache_name, O_RDWR, 0666);
    if (rfd < 0) {
      xfs_reply_err(fd, h->header.sequence_num, EIO);
      delete this; return;
    }

    while (count > 0) {
      char iobuf[NFS_MAXDATA];
      ost = lseek (rfd, offst, SEEK_SET);
      if (count < NFS_MAXDATA)
	err = read (rfd, iobuf, count);
      else
	err = read (rfd, iobuf, NFS_MAXDATA);
      if (err < 0) {
	xfs_reply_err(fd, h->header.sequence_num, EIO);
	delete this; return;
      }
      count -= err;
      offst += err;
      write3args wa;
      lbfs_tmpwrite3args twa;
      twa.data.setsize(err);
      memcpy(twa.data.base(), iobuf, err);
      twa.commit_to = e->nh;
      twa.fd = tmpfd;
      twa.offset = ost;
      twa.stable = UNSTABLE;
      twa.count = err;
      bytes_sent += twa.count;
      writes_sent++;
      ref<ex_write3res> res = New refcounted <ex_write3res>;
      outstanding_condwrites++;
      c->call (lbfs_TMPWRITE, &twa, res,
	       wrap (this, &putdata_obj::do_sendwrite, chunk, res),
	       lbfs_authof (sa));
    }
    blocks_written++;
    if (blocks_written == total_blocks && eof)
      sendcommittmp ();
    close (rfd);
  }

  void do_sendcondwrite (chunk *chunk, ref<ex_write3res> res, clnt_stat err)
  {
    if (outstanding_condwrites > 0) outstanding_condwrites--;
    if (!err && res->status == NFS3_OK) {
      if (res->resok->count != chunk->location().count ()) {
	if (lbcd_trace > 1)
	  warn << "do_sendcondwrite: did not write the whole chunk...\n";
	sendwrite (chunk);
	return;
      }
      blocks_written++;
      if (lbcd_trace > 1)
	warn << h->header.sequence_num << " condwrite: @"
	     << chunk->location().pos() << " +"
	     << chunk->location().count() << " "
	     << blocks_written << " blocks written " 
	     << total_blocks << " needed, eof? "
	     << eof << "\n";
      if (blocks_written == total_blocks && eof)
	sendcommittmp ();
    } else {
      if (err || res->status == NFS3ERR_FPRINTNOTFOUND) {
	if (lbcd_trace > 1) {
	  warn << "do_sendcondwrite: " << err << "\n";
	  warn << "-> " << h->header.sequence_num << " condwrite: "
	       << blocks_written << " blocks written " 
	       << total_blocks << " needed, eof? "
	       << eof << "\n";
	}
	sendwrite (chunk);
      } else
	xfs_reply_err (fd, h->header.sequence_num, err ? err : res->status);
    }

    if (!eof && outstanding_condwrites < OUTSTANDING_CONDWRITES) 
      condwrite_chunk();
  }

  void sendcondwrite (chunk *chunk) 
  {
    assert (!committed);

    lbfs_condwrite3args cw;
    cw.commit_to = e->nh;
    cw.fd = tmpfd;
    cw.offset = chunk->location().pos();
    cw.count = chunk->location().count();
    cw.hash = chunk->hash();

    condwrites_sent++;
    ref<ex_write3res> res = New refcounted <ex_write3res>;
    c->call (lbfs_CONDWRITE, &cw, res,
	     wrap (this, &putdata_obj::do_sendcondwrite, chunk, res), 
	     lbfs_authof (sa));    
  }

  void condwrite_chunk ()
  {
    int data_fd = open (e->cache_name, O_RDWR, 0666);
    if (data_fd < 0) {
      if (lbcd_trace > 1)
	warn << "putdata_obj::condwrite_chunk: " << strerror (errno) << "\n";
      xfs_reply_err (fd, h->header.sequence_num, EIO);
      delete this; return;
    }
  
    uint index, v_size;
    index = v_size = chunker->chunk_vector().size();
    if (lseek (data_fd, cur_pos, SEEK_SET) < 0) {
      if (lbcd_trace > 1)
	warn << "putdata_obj::condwrite_chunk: " << strerror (errno) << "\n";
      xfs_reply_err (fd, h->header.sequence_num, EIO);
      delete this; return;
    }
    
    uint count;
    unsigned char buf[4096];
    while ((count = read(data_fd, buf, 4096)) > 0) {
      cur_pos += count;
      chunker->chunk_data(buf, count);
      if (chunker->chunk_vector().size() > v_size) {
	v_size = chunker->chunk_vector().size();
	total_blocks = v_size;
	for (; index < v_size; index++) {
	  if (lbcd_trace > 1) {
	    warn << "chindex = " << index << " size = " << v_size << "\n";
	    warn << "adding fp = " 
	         << chunker->chunk_vector()[index]->hashidx()
		 << " to lbfsdb \n";
	  }
	  outstanding_condwrites++;
	  sendcondwrite(chunker->chunk_vector()[index]);
	  chunker->chunk_vector()[index]->location().set_fh (e->nh);
	  lbfsdb.add_entry
	    (chunker->chunk_vector()[index]->hashidx(),
	     &(chunker->chunk_vector()[index]->location()),
	     chunker->chunk_vector()[index]->location().size());
	}
      if (outstanding_condwrites >= OUTSTANDING_CONDWRITES) 
	break;
      }
    }
    close(data_fd);
    assert (count >= 0);
    if (count == 0) {
      chunker->stop();
      v_size = chunker->chunk_vector().size();
      total_blocks = chunker->chunk_vector().size();
      for (; index < v_size; index++) {
	if (lbcd_trace > 1) {
	  warn << "chindex = " << index << " size = " <<  total_blocks<< "\n";
	  warn << "adding fp = "
	       << chunker->chunk_vector()[index]->hashidx()
	       << " to lbfsdb \n";
	}
	sendcondwrite(chunker->chunk_vector()[index]);
	chunker->chunk_vector()[index]->location().set_fh (e->nh);
	lbfsdb.add_entry
	  (chunker->chunk_vector()[index]->hashidx(),
	   &(chunker->chunk_vector()[index]->location()),
	   chunker->chunk_vector()[index]->location().size());
      }
      eof = true;
    }
    if (lbcd_trace > 1)
      warn << "total_blocks = "  << total_blocks << " " 
	   << count << " eof " << eof << "\n";
    if (eof && total_blocks == 0)
      sendcommittmp();    
  }
  
  void mktmpfile (clnt_stat err)
  {
    if (!err && mtres->status == NFS3_OK)
      assert (mtres->resok->obj.present);
    else {
      xfs_reply_err (fd, h->header.sequence_num, err ? err : mtres->status);
      delete this;
    }
  }

  ~putdata_obj () 
  {
    delete chunker;
    lbfsdb_is_dirty = true;
  }

  putdata_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, ref<aclnt> c1) 
    : fd(fd1), c(c1), hh(h1), sa(sa1), blocks_written(0), total_blocks(0), 
      eof(false), retries(0), committed(false), cur_pos(0), 
      OUTSTANDING_CONDWRITES(16), outstanding_condwrites(0), 
      bytes_sent(0), condwrites_sent(0), writes_sent(0)
  {
    h = msgcast<xfs_message_putdata> (hh);
    chunker = New Chunker;
    e = xfsindex[h->handle];
    if (!e) {
      if (lbcd_trace > 1)
	warn << "xfs_putdata: Can't find node handle\n";
      xfs_reply_err (fd, h->header.sequence_num, ENOENT);
      delete this; return;
    }

    lbfs_mktmpfile3args mt;
    mt.commit_to = e->nh;
    tmpfd = FILE_DES++;
    mt.fd = tmpfd;
    xfsattr2nfsattr (h->header.opcode, h->attr, &mt.obj_attributes);
    
    mtres = New refcounted <ex_diropres3>;
    
    c->call (lbfs_MKTMPFILE, &mt, mtres,
	     wrap (this, &putdata_obj::mktmpfile), lbfs_authof (sa));
    e->writers = 0;
    condwrite_chunk ();
  }
};

struct write_obj {
  static const int OUTSTANDING_WRITES = 16;
  int fd;
  ref<aclnt> c;
  
  ref<xfs_message_header> hh;
  xfs_message_putdata *h;
  sfs_aid sa;
  cache_entry *e;
  int data_fd;
  bool had_error;
  int next_offset;
  unsigned outstanding_writes;

  void done()
  {
    if (outstanding_writes == 0) delete this;
  }

  void committed(ptr<ex_commit3res> res, time_t rqt, clnt_stat err)
  {
    outstanding_writes--;
    assert(outstanding_writes==0);
    if (!err && res->status == NFS3_OK) {
      e->nfs_attr = *(res->resok->file_wcc.after.attributes);
      e->set_exp (rqt, e->nfs_attr.type == NF3DIR);
      e->ltime = max (e->nfs_attr.mtime, e->nfs_attr.ctime);
      xfs_send_message_wakeup (fd, h->header.sequence_num, 0);
    }
    else
      xfs_reply_err(fd, h->header.sequence_num, err ? err : res->status);
    done();
  }

  void send_data (int n, ptr<ex_write3res> res, time_t rqt, clnt_stat err) 
  {
    if (res)
      outstanding_writes--;

    if (had_error) {
      done();
      return;
    }

    if (!res || (!err && res->status == NFS3_OK)) {
      for (int i=0; i<n; i++) {
        if (lseek (data_fd, next_offset, SEEK_SET) < 0) {
	  had_error = true;
	  xfs_reply_err(fd, h->header.sequence_num, errno);
	  done();
	  return;
        }
	char iobuf[NFS_MAXDATA];
        int error = read (data_fd, iobuf, NFS_MAXDATA);
        if (error < 0) {
	  had_error = true;
	  xfs_reply_err(fd, h->header.sequence_num, errno);
	  done();
	  return;
        }
        if (error > 0) {
          write3args wa;
	  wa.data.setsize(error);
	  memmove(wa.data.base(), iobuf, error);
          wa.file = e->nh;
          wa.stable = UNSTABLE;
	  wa.offset = next_offset;
	  wa.count = error;
	  next_offset += error;
	  outstanding_writes++;
	  ref<ex_write3res> wres = New refcounted <ex_write3res>;
	  c->call (lbfs_NFSPROC3_WRITE, &wa, wres,
		   wrap (this, &write_obj::send_data,
		         1, wres, timenow), lbfs_authof (sa));
        } else {
	  if (outstanding_writes == 0) {
	    if (next_offset > 0) {
              commit3args ca; 
	      ca.file = e->nh;
	      ca.offset = 0;
	      ca.count = next_offset;
	      ref<ex_commit3res> cres = New refcounted <ex_commit3res>;
	      outstanding_writes++;
	      c->call (lbfs_NFSPROC3_COMMIT, &ca, cres,
		       wrap (this, &write_obj::committed, cres, rqt),
		       lbfs_authof (sa));
	    }
	    else {
	      xfs_send_message_wakeup (fd, h->header.sequence_num, 0);
	      done();
	    }
	  }
	  return;
        }
      }
    } else {
      had_error = true;
      xfs_reply_err(fd, h->header.sequence_num, err ? err : res->status);
      done();
      return;
    }
  }

  ~write_obj () 
  {
    if (data_fd) close (data_fd);
  }

  write_obj (int fd1, ref<xfs_message_header> h1, sfs_aid sa1, ref<aclnt> c1) :
    fd(fd1), c(c1), hh(h1), sa(sa1),
    had_error(false), next_offset(0), outstanding_writes(0)
  {
    h = msgcast<xfs_message_putdata> (hh);
    e = xfsindex[h->handle];
    if (!e) {
      if (lbcd_trace > 1)
	warn << "write_obj: Can't find node handle\n";
      xfs_reply_err (fd, h->header.sequence_num, ENOENT);
      done();
      return;
    }

    data_fd = open (e->cache_name, O_RDWR, 0666);
    if (data_fd < 0) {
      xfs_reply_err (fd, h->header.sequence_num, errno);
      done();
      return;
    }
    send_data (OUTSTANDING_WRITES, NULL, 0, clnt_stat(0));
  }
};

void
lbfs_putdata (int fd, ref<xfs_message_header> h, sfs_aid sa, ref<aclnt> c)
{

  xfs_message_putdata *hh = msgcast<xfs_message_putdata> (h);
  cache_entry *e = xfsindex[hh->handle];
  if (!e) {
    if (lbcd_trace > 1)
      warn << "lbfs_putdata: Can't find node handle\n";
    xfs_reply_err (fd, h->sequence_num, ENOENT);
    return;
  }

  struct stat sb;
  if (!lstat (e->cache_name, &sb))
    if (sb.st_size < 8192) {
      vNew write_obj (fd, h, sa, c);
      return;
    }
  if (lbfs > 1)
    vNew putdata_obj (fd, h, sa, c);
  else vNew write_obj (fd, h, sa, c);
}

