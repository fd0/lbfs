#include "fh_map.h"

str
dirname (uint32 index)
{
  return strbuf ("cache/%02X", index >> 8);
}

str 
filename (uint32 index)
{
  return strbuf ("cache/%02X/%02X", index >> 8, index & 0xff);
}

str 
setcache_name (uint32 index)
{

  str fname = filename (index);
  int fd = open (fname, O_CREAT | O_RDWR | O_TRUNC, 0666);
  if (fd < 0) {
    if (errno == ENOENT) {
      str dname = dirname (index);
#if DEBUG > 0
      warn << "Creating dir: " << dname << "\n";
#endif
      if (mkdir (dname, 0777) < 0) {
#if DEBUG > 0
	warn << strerror (errno) << "(" << errno << ") mkdir " << dname << "\n";
#endif
	return NULL;
      }
      fd = open (fname, O_CREAT | O_RDWR | O_TRUNC, 0666);
      if (fd < 0) {
#if DEBUG > 0
	warn << strerror (errno) << "(" << errno << ") on file =" 
	     << fname << "\n";
#endif
	return NULL;
      }
      close (fd);
    }
    else {
#if DEBUG > 0
      warn << strerror (errno) << "(" << errno << ") on file =" 
	   << fname << "\n";
#endif
      return NULL;
    }
  }
  close (fd);
  return fname;
}

nfstime3 
max (nfstime3 mtime, nfstime3 ctime)
{
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

