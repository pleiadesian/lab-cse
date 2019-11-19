#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>

char d1[512], d2[512];
extern int errno;

char big[20001];
char huge[65536];

void
create1(const char *d, const char *f)  //, const char *in)
{
  int fd;
  char n[512];

  /*
   * Do not wait the client to invalidates its caches
   * sleep(1);
   */

  sprintf(n, "%s/%s", d, f);
  fd = creat(n, 0666);

  // test create
  if(fd < 0){
    fprintf(stderr, "mytest: create(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }

  // test write
  // if(write(fd, in, strlen(in)) != strlen(in)){
  //   fprintf(stderr, "mytest: write(%s): %s\n",
  //           n, strerror(errno));
  //   exit(1);
  // }

  // test close
  if(close(fd) != 0){
    fprintf(stderr, "mytest: close(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }
}

void
check1(const char *d, const char *f, const char *in)
{
  int fd, cc;
  char n[512], buf[21000];

  sprintf(n, "%s/%s", d, f);
  fd = open(n, 0);

  // test open
  if(fd < 0){
    fprintf(stderr, "mytest: open(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }

  // test read
  errno = 0;
  cc = read(fd, buf, sizeof(buf) - 1);
  if(cc != strlen(in)){
    fprintf(stderr, "mytest: read(%s) returned too little %d%s%s\n",
            n,
            cc,
            errno ? ": " : "",
            errno ? strerror(errno) : "");
    exit(1);
  }

  close(fd);
  buf[cc] = '\0';
  if(strncmp(buf, in, strlen(n)) != 0){
    fprintf(stderr, "mytest: read(%s) got \"%s\", not \"%s\"\n",
            n, buf, in);
    exit(1);
  }
}

void
unlink1(const char *d, const char *f)
{
  char n[512];

  /*
   * Do not wait the client to invalidates its caches
   * sleep(1);
   */

  sprintf(n, "%s/%s", d, f);
  if(unlink(n) != 0){
    fprintf(stderr, "mytest: unlink(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }
}

void
checknot(const char *d, const char *f)
{
  int fd;
  char n[512];

  sprintf(n, "%s/%s", d, f);
  fd = open(n, 0);
  if(fd >= 0){
    fprintf(stderr, "mytest: open(%s) succeeded for deleted file\n", n);
    exit(1);
  }
}

void
append1(const char *d, const char *f, const char *in)
{
  int fd;
  char n[512];

  /*
   * Do not wait the client to invalidates its caches
   * sleep(1);
   */

  sprintf(n, "%s/%s", d, f);
  fd = open(n, O_WRONLY|O_APPEND);
  if(fd < 0){
    fprintf(stderr, "mytest: append open(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }
  if(write(fd, in, strlen(in)) != strlen(in)){
    fprintf(stderr, "mytest: append write(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }
  if(close(fd) != 0){
    fprintf(stderr, "mytest: append close(%s): %s\n",
            n, strerror(errno));
    exit(1);
  }
}

void
write1(const char *d, const char *f, int start, int n, char c)
{
  int fd;
  char name[512];

  /*
   * Do not wait the client to invalidates its caches
   * sleep(1);
   */

  sprintf(name, "%s/%s", d, f);
  fd = open(name, O_WRONLY|O_CREAT, 0666);
  if (fd < 0 && errno == EEXIST)
    fd = open(name, O_WRONLY, 0666);
  if(fd < 0){
    fprintf(stderr, "mytest: open(%s): %s\n",
            name, strerror(errno));
    exit(1);
  }
  if(lseek(fd, start, 0) != (off_t) start){
    fprintf(stderr, "mytest: lseek(%s, %d): %s\n",
            name, start, strerror(errno));
    exit(1);
  }
  for(int i = 0; i < n; i++){
    if(write(fd, &c, 1) != 1){
      fprintf(stderr, "mytest: write(%s): %s\n",
              name, strerror(errno));
      exit(1);
    }
    if(fsync(fd) != 0){
      fprintf(stderr, "mytest: fsync(%s): %s\n",
              name, strerror(errno));
      exit(1);
    }
  }
  if(close(fd) != 0){
    fprintf(stderr, "mytest: close(%s): %s\n",
            name, strerror(errno));
    exit(1);
  }
}

void
checkread(const char *d, const char *f, int start, int n, char c)
{
  int fd;
  char name[512];

  /*
   * Do not wait the client to invalidates its caches
   * sleep(1);
   */

  sprintf(name, "%s/%s", d, f);
  fd = open(name, 0);
  if(fd < 0){
    fprintf(stderr, "mytest: open(%s): %s\n",
            name, strerror(errno));
    exit(1);
  }
  if(lseek(fd, start, 0) != (off_t) start){
    fprintf(stderr, "mytest: lseek(%s, %d): %s\n",
            name, start, strerror(errno));
    exit(1);
  }
  for(int i = 0; i < n; i++){
    char xc;
    if(read(fd, &xc, 1) != 1){
      fprintf(stderr, "mytest: read(%s): %s\n",
              name, strerror(errno));
      exit(1);
    }
    if(xc != c){
      fprintf(stderr, "mytest: checkread off %d %02x != %02x\n",
              start + i, xc, c);
      exit(1);
    }
  }
  close(fd);
}

void
createn(const char *d, const char *prefix, int nf, bool possible_dup)
{
  int fd, i;
  char n[512];

  /*
   * Do not wait the client to invalidates its caches
   * sleep(1);
   */

  for(i = 0; i < nf; i++){
    sprintf(n, "%s/%s-%d", d, prefix, i);
    fd = creat(n, 0666);
    if (fd < 0 && possible_dup && errno == EEXIST)
      continue;
    if(fd < 0){
      fprintf(stderr, "mytest: create(%s): %s\n",
              n, strerror(errno));
      exit(1);
    }
    if(write(fd, &i, sizeof(i)) != sizeof(i)){
      fprintf(stderr, "mytest: write(%s): %s\n",
              n, strerror(errno));
      exit(1);
    }
    if(close(fd) != 0){
      fprintf(stderr, "mytest: close(%s): %s\n",
              n, strerror(errno));
      exit(1);
    }
  }
}

void
checkn(const char *d, const char *prefix, int nf)
{
  int fd, i, cc, j;
  char n[512];

  for(i = 0; i < nf; i++){
    sprintf(n, "%s/%s-%d", d, prefix, i);
    fd = open(n, 0);
    if(fd < 0){
      fprintf(stderr, "mytest: open(%s): %s\n",
              n, strerror(errno));
      exit(1);
    }
    j = -1;
    cc = read(fd, &j, sizeof(j));
    if(cc != sizeof(j)){
      fprintf(stderr, "mytest: read(%s) returned too little %d%s%s\n",
              n,
              cc,
              errno ? ": " : "",
              errno ? strerror(errno) : "");
      exit(1);
    }
    if(j != i){
      fprintf(stderr, "mytest: checkn %s contained %d not %d\n",
              n, j, i);
      exit(1);
    }
    close(fd);
  }
}

void
unlinkn(const char *d, const char *prefix, int nf)
{
  char n[512];
  int i;

  sleep(1);

  for(i = 0; i < nf; i++){
    sprintf(n, "%s/%s-%d", d, prefix, i);
    if(unlink(n) != 0){
      fprintf(stderr, "mytest: unlink(%s): %s\n",
              n, strerror(errno));
      exit(1);
    }
  }
}

int
compar(const void *xa, const void *xb)
{
  char *a = *(char**)xa;
  char *b = *(char**)xb;
  return strcmp(a, b);
}

void
dircheck(const char *d, int nf)
{
  DIR *dp;
  struct dirent *e;
  char *names[1000];
  int nnames = 0, i;

  dp = opendir(d);
  if(dp == 0){
    fprintf(stderr, "mytest: opendir(%s): %s\n", d, strerror(errno));
    exit(1);
  }
  while((e = readdir(dp))){
    if(e->d_name[0] != '.'){
      if(nnames >= sizeof(names)/sizeof(names[0])){
        fprintf(stderr, "warning: too many files in %s\n", d);
      }
      names[nnames] = (char *) malloc(strlen(e->d_name) + 1);
      strcpy(names[nnames], e->d_name);
      nnames++;
    }
  }
  closedir(dp);

  if(nf != nnames){
    fprintf(stderr, "mytest: wanted %d dir entries, got %d\n", nf, nnames);
    exit(1);
  }

  /* check for duplicate entries */
  qsort(names, nnames, sizeof(names[0]), compar);
  for(i = 0; i < nnames-1; i++){
    if(strcmp(names[i], names[i+1]) == 0){
      fprintf(stderr, "mytest: duplicate directory entry for %s\n", names[i]);
      exit(1);
    }
  }

  for(i = 0; i < nnames; i++)
    free(names[i]);
}

void
reap (int pid)
{
  int wpid, status;
  wpid = waitpid (pid, &status, 0);
  if (wpid < 0) {
    perror("waitpid");
    exit(1);
  }
  if (wpid != pid) {
    fprintf(stderr, "unexpected pid reaped: %d\n", wpid);
    exit(1);
  }
  if(!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    fprintf(stderr, "child exited unhappily\n");
    exit(1);
  }
}

int main(int argc, char *argv[])
{
  int pid, i;

  if(argc != 3){
    fprintf(stderr, "Usage: mytest dir1 dir2\n");
    exit(1);
  }

  sprintf(d1, "%s/d%d", argv[1], getpid());
  if(mkdir(d1, 0777) != 0){
    fprintf(stderr, "mytest: failed: mkdir(%s): %s\n",
            d1, strerror(errno));
    exit(1);
  }
  sprintf(d2, "%s/d%d", argv[2], getpid());
  if(access(d2, 0) != 0){
    fprintf(stderr, "mytest: failed: access(%s) after mkdir %s: %s\n",
            d2, d1, strerror(errno));
    exit(1);
  }

  setbuf(stdout, 0);

  printf("A large number of creates and removes, inode should not be used up: ");
  for(int i=0; i < 1000; i++) {
    char buf[512];
    sprintf(buf, "xxx-%d", i);
    create1(d1, buf);
    unlink1(d1, buf);
  }
  for(int i=0; i < 1000; i++) {
    char buf[512];
    sprintf(buf, "yyy-%d", i);
    create1(d2, buf);
    unlink1(d2, buf);
  }
  dircheck(d1, 0);
  dircheck(d2, 0);
  printf("OK\n");

  printf("Concurrent creates, same directories, different yfs client: ");
  pid = fork();
  if(pid < 0){
    perror("mytest: fork");
    exit(1);
  }
  if(pid == 0){
    createn(d2, "zz", 10, true);
    createn(d1, "xx", 3, false);
    unlinkn(d1, "xx", 2);
    exit(0);
  }
  createn(d1, "zz", 10, true);
  createn(d2, "yy", 3, false);
  unlinkn(d2, "yy", 2);
  reap(pid);
  dircheck(d1, 12);
  dircheck(d2, 12);
  checkn(d1, "zz", 10);
  checkn(d2, "zz", 10);
  unlinkn(d1, "zz", 10);
  for(i = 0; i < 10; i++){
    char buf[512];
    sprintf(buf, "zz-%d", i);
    checknot(d1, buf);
    checknot(d2, buf);
  }
  dircheck(d1, 2);
  dircheck(d2, 2);
  printf("OK\n");

  printf("mytest: Passed all tests of mine\n");

  exit(0);
  return(0);
}