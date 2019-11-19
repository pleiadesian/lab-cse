### My testcases

---

##### % ./mytest.sh

…

mytest: Passed all tests of mine



##### 跑完测试用例约需要1分钟



##### 对测试用例中函数的修改

*  FreeBSD NFS client 在每一个write或者create操作后会调用sleep函数进行等待。我的测试用例删去了函数中所有的sleep()，在更苛刻的并发条件下对文件系统进行测试
* 删去了create1()中对新建的文件的写操作。因为在大量创建和删除文件时的测试主要关注inode的分配和回收是否正常，文件内容的正确性在其他测试用例中进行测试。



##### 大量的创建和删除文件

```c++
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
```

* 本测试用例对yfs1发送了1000个create/remove请求，然后对yfs2发送了1000个create/remove请求
* 正常情况下，inode_manager会反复分配inode、回收inode，最后目录中的文件数目为0。
* 如果在extent_client中对create和remove操作进行cache，有可能会导致文件系统的inode被分配后没有及时回收。如果没有处理好这种情况下inode的回收策略，就有可能在运行本测试用例时卡住。



##### 并发创建和删除文件

```c++
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
```

* 父进程对yfs1发送10个create，对yfs2发送3个create、2个unlink；子进程对yfs2发送10个create，对yfs1发送3个create、2个unlink
  * 考察对并发请求的处理能力
    * 两个进程对同一个yfs_client有并发，yfs_client中没有全局的变量或者结构体，应正确运行；extent_client和lock_client中的全局变量或者结构体应该被mutex保护，应正确运行
    * 两个yfs_client对同一个inode有并发的拿锁，lock_server应正确将锁授权和回收，避免在extent_server、inode_manager等文件系统底层中发生并发的对同一个inode进行操作的情况
    * 如果没有做好上述对并发情况的处理，则有可能在运行时程序崩溃，无法通过本测试
  * 考察文件系统实现是否正确
    * 两个进程分别对zz进行了10个create操作，文件名有重复（"zz-0"、"zz-1"、……、"zz-9"）
      * 运行结束后目录中应有"zz-0"、"zz-1"、……、"zz-9"共10个文件
      * 如果没有做好对同一个文件名的并发create的处理，或者在建立文件时错误判断本来存在的文件为不存在，则会报错，无法通过本测试
    * 在上述20个对"zz"为前缀的文件创建的同时，两个进程分别进行了3个create和2个unlink。父进程创建和删除的文件前缀为"xx"，子进程创建和删除的文件前缀为"yy"。不会有在创建文件前要求删除同名文件的错误请求。
      * 运行结束后目录中应只留下2个文件
      * 如果没有做好在并发create/unlink时对目录的处理，最后检查文件数目会不正确，无法通过本测试