#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <iostream.h>

int main(int argc, char **argv) {
  
  int fd = open("/sfs/h", O_RDWR | O_APPEND | O_CREAT, 0666);
  int k = 0;
  int i = 0;
  int j;
  cout << "Opened /sfs/h \n";

  char buf[2000];
  strcpy(buf, "This is a test sentence.\n");
  write(fd, buf, 25);
  cout << "Wrote to /sfs/h \n";

  for(k=0; k<5; k++) {
  for(i=0;i<100000000;i++) {
    j = j * i;
  }
  }

  write(fd, buf, 25);
  cout << "Wrote to /sfs/h \n";

  close(fd);
  cout << "Closed fd of /sfs/h\n";
  cout << j << "\n";
  return 0;
}
