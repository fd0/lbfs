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

#include "kernel.h"

/*
 * The fd we use to talk with the kernel on.
 */

int kernel_fd = -1;

/* count of the number of messages in a read */

static unsigned recv_count[20];

/* for more than above... */

static unsigned recv_count_overflow;


static int
process_message (int msg_length, char *msg)
{
     struct xfs_message_header *header;
     char *p;
     int cnt;

     cnt = 0;
     for (p = msg;
	  msg_length > 0;
	  p += header->size, msg_length -= header->size) {
	 header = (struct xfs_message_header *)p;
	 xfs_message_receive (kernel_fd, header, header->size);
	 ++cnt;
     }
     if (u_int(cnt) < sizeof(recv_count)/sizeof(recv_count[0]))
	 ++recv_count[cnt];
     else
	 ++recv_count_overflow;
     
     return 0;
}

#if 0

/*
 * The work threads.
 */

struct worker {
    char data[MAX_XMSG_SIZE];
    PROCESS pid;
    int  msg_length;
    int  busyp;
    int  number;
} *workers;

static void
sub_thread (void *v_myself)
{
    struct worker *self = (struct worker *)v_myself;

    for (;;) {
	arla_warnx (ADEBKERNEL, "worker %d waiting", self->number);
	LWP_WaitProcess (self);
	self->busyp = 1;
	++workers_used;
	arla_warnx (ADEBKERNEL, "worker %d: processing", self->number);
	process_message (self->msg_length, self->data);
	arla_warnx (ADEBKERNEL, "worker %d: done", self->number);
	--workers_used;
	self->busyp = 0;
    }
}

PROCESS version_pid;

static void
version_thread (void *foo)
{
    xfs_probe_version (kernel_fd, XFS_VERSION);
}


/*
 * The tcp communication unit
 */

static int
tcp_open (const char *filename)
{
    int s, ret, port;
    struct sockaddr_in addr;

    if (strlen (filename) == 0)
	arla_errx (1, ADEBERROR, "tcp_open doesn't contain tcp-port");

    port = atoi (filename);
    if (port == 0)
	arla_errx (1, ADEBERROR, "tcp_open couldn't parse %s as a port#",
		   filename);

    s = socket (PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s < 0) {
	arla_warn (ADEBWARN, errno, "tcp_open: socket failed");
	return s;
    }

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = 0x7f000001; /* 127.0.0.1 */
    addr.sin_port = htons(port);
    ret = connect (s, (struct sockaddr *)&addr, sizeof(addr));
    if (ret < 0) {
	arla_warn (ADEBWARN, errno, "tcp_open: connect failed");
	return ret;
    }
    return ret;
}

static int
tcp_opendef (const char *filename)
{
    if (strlen (filename) != 0)
	arla_warnx (ADEBWARN, "tcp_opendef ignoring extra data");

    return tcp_open ("5000"); /* XXX */
}

static ssize_t
tcp_read (int fd, void *data, size_t len)
{
    int32_t slen;
    char in_len[4];
    if (recv (fd, in_len, sizeof(in_len), 0) != sizeof(in_len)) {
	arla_warn (ADEBWARN, errno, "tcp_read: failed to read length");
	return -1;
    }
    memcpy(&slen, in_len, sizeof(slen));
    slen = ntohl(slen);
    if (len < slen) {
	arla_warnx (ADEBWARN, 
		    "tcp_read: recv a too large messsage %d",
		    slen);	
	return -1;
    }
    return recv (fd, data, slen, 0) == slen ? slen : -1;
}

static ssize_t
tcp_write (int fd, const void *data, size_t len)
{
    int32_t slen = htonl(len);
    char out_len[4];
    memcpy (out_len, &slen, sizeof(len));
    if (send (fd, out_len, sizeof(len), 0) != sizeof(out_len)) {
	arla_warn (ADEBWARN, errno, "tcp_write: failed to write length");
	return -1;
    }
    return send (fd, data, len, 0) == len ? len : -1;
}

#endif 

/*
 * The cdev communication unit
 */

static int
dev_open (const char *filename)
{
    char fn[MAXPATHLEN];
    snprintf (fn, MAXPATHLEN, "/%s", filename);
    return open (fn, O_RDWR);
}

#if 0
static int
dev_fileopen (const char *filename)
{
    return dev_open (filename);
}
#endif

static ssize_t
dev_read (int fd, void *msg, size_t len)
{
    return read (fd, msg, len);
}

static ssize_t
dev_write (int fd, const void *msg, size_t len)
{
    return write (fd, msg, len);
}

#if 0

/*
 * The null communication unit
 */

static int
null_open (const char *filename)
{
    return 0;
}

static ssize_t
null_read (int fd, void *msg, size_t len)
{
    return 0;
}

static ssize_t
null_write (int fd, const void *msg, size_t len)
{
    return len;
}

#endif

/*
 * Ways to communticate with the kernel
 */ 

struct kern_interface {
    char *prefix;
    int (*open) (const char *filename);
    ssize_t (*read) (int fd, void *msg, size_t len);
    ssize_t (*write) (int fd, const void *msg, size_t len);
} kern_comm[] = {
    { "/",	dev_open, dev_read, dev_write},
#if 0
    { "file:/",	dev_fileopen, dev_read, dev_write},
    { "tcpport:", tcp_open, tcp_read, tcp_write},
    { "tcp",	tcp_opendef, tcp_read, tcp_write},
    { "null",	null_open, null_read, null_write},
    { NULL }
#endif
} ;

struct kern_interface *kern_cur = NULL;

static int
kern_open (const char *filename)
{
    struct kern_interface *ki = &kern_comm[0];
    int len;

    while (ki->prefix) {
	len = strlen (ki->prefix);
	if (strncasecmp (ki->prefix, filename, len) == 0) {
	    break;
	}    
	ki++;
    }
    if (ki->prefix == NULL)
	return -1;
    kern_cur = ki;
    return (ki->open) (filename+len); /*use the right interface for each type of dev */
}

ssize_t
kern_read (int fd, void *data, size_t len)
{
    assert (kern_cur != NULL);
    return (kern_cur->read) (fd, data, len);
}

ssize_t
kern_write (int fd, const void *data, size_t len)
{
    assert (kern_cur != NULL);
    return (kern_cur->write) (fd, data, len);
}

int
kernel_opendevice (const char *dev)
{

    int fd = kern_open (dev);
    if (fd < 0) {
      warn << strerror(errno) << ": kern_open " << dev << "\n";
      return -1;
    }
    /*
    int err = ioctl(fd, FIONBIO, 1);
    if (err == -1) {
      warn << strerror(errno) << "(" << errno << ")" << ": ioctl " << dev << "\n";
      return -1;
    }*/
    //make_async(fd);
    //close_on_exec(fd);
    kernel_fd = fd;
    return 0;
}

char data[MAX_XMSG_SIZE];

void akernel() {
  u_int32_t data_size = MAX_XMSG_SIZE;
  //char* data = New char[data_size]; //(char *) malloc(data_size);
  int len = kern_read(kernel_fd, data, data_size);

  if ((len == -1) && (errno != EAGAIN)) { 
    warn << "can't read from kernel_fd = " << kernel_fd << ":" 
	 << strerror(errno) << "\n";
    return;
  } else if (len == 0) {
    warn << "akernel: len = 0..wierd..maybe device is closed\n";
    fdcb(kernel_fd, selread, NULL);
    return;
  } else {
    warn << "akernel: received data\n";
    process_message(len, data);
  }
  //delete [] data;
  fdcb(kernel_fd, selread, wrap(&akernel));
}

#if 0

void skernel() {
  fd_set readset;
  int ret;
  
  for (;;) {
    FD_ZERO(&readset);
    FD_SET(kernel_fd, &readset);
  
    ret = select (kernel_fd + 1, &readset, NULL, NULL, NULL); 
    
    if (ret < 0) {
      warn << strerror(errno) << ": select\n";
      return;
    }
    if (FD_ISSET(kernel_fd, &readset)) {
      u_int32_t data_size = MAX_XMSG_SIZE;
      char* data = (char *) malloc(data_size);
      int len = kern_read(kernel_fd, data, data_size);
      if ((len == -1) && (errno != EAGAIN)) { 
	warn << "can't read from kernel_fd = " << kernel_fd << ":" 
	     << strerror(errno) << "\n";
	return;
      } else if (len == 0) {
	warn << "skernel: len = 0..wierd\n";
	fdcb(kernel_fd, selread, NULL);
	return;
      } else {
	warn << "skernel: received data\n";
	process_message(len, data);
      }
    }
  }
}

#define WORKER_STACKSIZE (16*1024)

void
kernel_interface (struct kernel_args *args)
{
     int i;

     assert (kernel_fd >= 0);

     workers = malloc (sizeof(*workers) * args->num_workers);
     if (workers == NULL)
	 arla_err (1, ADEBERROR, errno, "malloc %lu failed",
		   (unsigned long)sizeof(*workers) * args->num_workers);

     workers_high = args->num_workers;
     workers_used = 0;
 
    for (i = 0; i < args->num_workers; ++i) {
	 workers[i].busyp  = 0;
	 workers[i].number = i;
	 if (LWP_CreateProcess (sub_thread, WORKER_STACKSIZE, 1,
				(char *)&workers[i],
				"worker", &workers[i].pid))
	     arla_errx (1, ADEBERROR, "CreateProcess of worker failed");
     }

     if (LWP_CreateProcess (version_thread, WORKER_STACKSIZE, 1,
			    NULL,
			    "version", &version_pid))
	 arla_errx (1, ADEBERROR, "CreateProcess of version thread failed");

     arla_warnx(ADEBKERNEL, "Arla: selecting on fd: %d", kernel_fd);

     for (;;) {
       fd_set readset;
       int ret;
       
       FD_ZERO(&readset);
       FD_SET(kernel_fd, &readset);

	  ret = IOMGR_Select (kernel_fd + 1, &readset, NULL, NULL, NULL); 

	  if (ret < 0)
	      arla_warn (ADEBKERNEL, errno, "select");
	  else if (ret == 0)
	      arla_warnx (ADEBKERNEL,
			  "Arla: select returned with 0. strange.");
	  else if (FD_ISSET(kernel_fd, &readset)) {
	      for (i = 0; i < args->num_workers; ++i) {
		  if (workers[i].busyp == 0) {
		      ret = kern_read (kernel_fd, workers[i].data,
				       sizeof(workers[i].data));
		      if (ret <= 0) {
			  arla_warn (ADEBWARN, errno, "read");
		      } else {
			  workers[i].msg_length = ret;
			  LWP_SignalProcess (&workers[i]);
		      }
		      break;
		  }
	      }
	      if (i == args->num_workers)
		  arla_warnx (ADEBWARN, "kernel: all workers busy");
	  }
     }
}

#endif

