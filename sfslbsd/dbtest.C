
// usage: dbtest host file
//
// test lbfs database by chunking "file", search through the local DB for
// matching fingerprints, then connect to the nfs server on "host" to retrieve
// existing chunks.
//
// for example:
//
// ./dbtest localhost /disk/pw0/benjie/play/db-3.1.17/LICENSE
//
// should return the list of reusable chunks, fingerprints of the chunks of
// "file", and fingerprints of the chunks retrieved via the nfs server. the
// last two sets of fingerprints should match.


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "fingerprint.h"
#include "lbfsdb.h"
#include "sfsrwsd.h"
#include "lbfs.h"
  
lbfs_db _db;
static int _requests = 0;
static char *_host;
static char *_file;
ptr<aclnt> _c;

int
lbfs_search_reusable_chunks(vec<lbfs_chunk *> &new_chunks,
                            vec<lbfs_chunk_loc *> &reusable_chunks)
{
  if (new_chunks.size() == 0)
    return -1;

  reusable_chunks.setsize(new_chunks.size());
  for (unsigned i = 0; i < new_chunks.size(); i++) {
    lbfs_db::chunk_iterator *iter = 0;
    if (_db.get_chunk_iterator(new_chunks[i]->fingerprint, &iter) == 0) {
      if (iter) {
        lbfs_chunk_loc *c = new lbfs_chunk_loc();
        if (iter->get(c) == 0)
          reusable_chunks[i] = c;
        else {
	  reusable_chunks[i] = 0L;
	  delete c;
	}
        delete iter;
      }
    }
    else 
      reusable_chunks[i] = 0L;
  }
  return 0;
}


void 
gotdata(u_int64_t fp, unsigned char *data, size_t count, str err)
{
  if (!err && count > 0) {
    vec<lbfs_chunk *> chunks;
    chunk_data(CHUNK_SIZES(0), &chunks, data, count);
    for (unsigned i=0; i<chunks.size(); i++) {
      printf("0x%016qx %d 0x%016qx\n", fp, i, chunks[i]->fingerprint);
      delete chunks[i];
    }
  }
  delete[] data;

  if (_requests > 0) _requests--;
  if (_requests == 0)
    exit(0);
}

void
gotdata0(unsigned char *buf, off_t pos0,
         unsigned char *data, size_t count, off_t pos)
{
  memmove(buf+(pos-pos0), data, count);
}


void 
getnfsc(ptr<aclnt> nc, clnt_stat stat)
{
  if (!nc) {
    warn << _host << ": NFS3: " << stat << "\n";
    exit(-1);
  }
  _c = nc;

  vec<lbfs_chunk *> new_chunks;
  if (chunk_file(CHUNK_SIZES(0), &new_chunks, _file) < 0) {
    printf("cannot open %s for chunking\n", _file);
    exit(-1);
  }

  vec<lbfs_chunk_loc *> reusable_chunks;
  _db.open();
  lbfs_search_reusable_chunks(new_chunks, reusable_chunks);
 
  for (unsigned i=0; i<new_chunks.size(); i++) {
    if (reusable_chunks[i]) {
      printf("%s: reuse %" OTF "d %d\n",
	     _file, reusable_chunks[i]->pos(), reusable_chunks[i]->count());
      nfs_fh3 fh;
      reusable_chunks[i]->get_fh(fh);
      unsigned char *buf = new unsigned char[reusable_chunks[i]->count()];
      nfs3_read(_c, fh, 
	        wrap(gotdata0, buf, reusable_chunks[i]->pos()),
	        wrap(gotdata, new_chunks[i]->fingerprint, buf),
	        reusable_chunks[i]->pos(), reusable_chunks[i]->count());
      _requests++;
    }
    delete new_chunks[i];
    delete reusable_chunks[i];
  }
  if (_requests == 0) 
    exit(0);
}

int 
main(int argc, char *argv[])
{
  if (argc != 3) {
    printf("usage: %s host file\n", argv[0]);
    return -1;
  }
  _host = argv[1];
  _file = argv[2];
  aclntudp_create (_host, 0, nfs_program_3, wrap(getnfsc));
  amain();
}


