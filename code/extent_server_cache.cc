// the extent server implementation

#include "extent_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "handle.h"
#include "lang/verify.h"
#include "tprintf.h"


extent_server_cache::extent_server_cache() 
{
  im = new inode_manager();
  pthread_mutex_init(&lock, NULL);

  // init root dir inode
  inode_entry *ie = new inode_entry();
  ie->stat = SHARED;
  inode_list[1] = ie;
}

int extent_server_cache::create(uint32_t type, std::string cid, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  pthread_mutex_lock(&lock);
  id = im->alloc_inode(type);

  // creater client hold the cache as an exclusive writer
  inode_entry *ie = new inode_entry();
  ie->stat = EXCLUSIVE;
  ie->writer = cid;
  inode_list[id] = ie;

  pthread_mutex_unlock(&lock);
  return extent_protocol::OK;
}

int extent_server_cache::put(extent_protocol::extentid_t id, std::string cid, std::string buf, int &)
{
  pthread_mutex_lock(&lock);

  id &= 0x7fffffff;

  // handle INIT when start a new yfs_client
  if (id == 1 && buf == "") {
    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

    pthread_mutex_unlock(&lock);
    return extent_protocol::OK;
  }

  int r;
  inode_entry *ie = inode_list[id];

  if (ie->stat == SHARED) {
    // change inode entry to exclusive mode, invalid all sharing reader
    ie->stat = EXCLUSIVE;
    ie->writer = cid;
    while (!ie->reader_queue.empty()) {
      std::string rid = ie->reader_queue.front();
      // send invalid to reader except the requesting exclusive writer
      if (rid != cid) {
        pthread_mutex_unlock(&lock);
        handle(rid).safebind()->call(rextent_protocol::invalid, id, r);
        pthread_mutex_lock(&lock);
      }
      ie->reader_queue.pop_front();
    }

    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

    pthread_mutex_unlock(&lock);
    return extent_protocol::OK;
  } else {
    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

    VERIFY (ie->writer != cid);
    pthread_mutex_unlock(&lock);
    handle(ie->writer).safebind()->call(rextent_protocol::invalid, id, r);
    pthread_mutex_lock(&lock);
    ie->writer = cid;

    pthread_mutex_unlock(&lock);
    return extent_protocol::OK;
  }
}

int extent_server_cache::get(extent_protocol::extentid_t id, std::string cid , std::string &buf)
{
  pthread_mutex_lock(&lock);

  id &= 0x7fffffff;

  inode_entry *ie = inode_list[id];
  VERIFY (ie != NULL);

  if (ie->stat == EXCLUSIVE) {
    // revoke dirty data in writer's cache
    std::string revoke_buf;
    pthread_mutex_unlock(&lock);
    handle(ie->writer).safebind()->call(rextent_protocol::revoke, id, revoke_buf);
    pthread_mutex_lock(&lock);

    // write data onto disk
    const char * cbuf = revoke_buf.c_str();
    int size = revoke_buf.size();
    im->write_file(id, cbuf, size);

    ie->stat = SHARED;
  }

  // read data from disk
  int size = 0;
  char *cbuf = NULL;
  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  ie->reader_queue.push_back(cid);
  pthread_mutex_unlock(&lock);
  return extent_protocol::OK;
}

int extent_server_cache::getattr(extent_protocol::extentid_t id, std::string cid, extent_protocol::attr &a)
{
  pthread_mutex_lock(&lock);

  id &= 0x7fffffff;

  inode_entry *ie = inode_list[id];
  VERIFY (ie != NULL);

  if (ie->stat == EXCLUSIVE && ie->writer != cid) {
    // revoke dirty data in writer's cache
    std::string buf;
    pthread_mutex_unlock(&lock);
    handle(ie->writer).safebind()->call(rextent_protocol::revoke, id, buf);
    pthread_mutex_lock(&lock);

    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

    ie->stat = SHARED;
  }

  // read attr from disk
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;
  pthread_mutex_unlock(&lock);
  return extent_protocol::OK;
}

int extent_server_cache::remove(extent_protocol::extentid_t id, std::string cid, int &)
{
  pthread_mutex_unlock(&lock);

  id &= 0x7fffffff;
  im->remove_file(id);

  int r;
  inode_entry *ie = inode_list[id];

  VERIFY (ie != NULL);

  // invalid all cache of this inode on client
  if (ie->stat == EXCLUSIVE) {
    // send invalid to writer except the requestig client, which already invalid itself locally
    if (ie->writer != cid) {
      pthread_mutex_unlock(&lock);
      handle(ie->writer).safebind()->call(rextent_protocol::invalid, id, r);
      pthread_mutex_lock(&lock);
    }
  } else {
    while (!ie->reader_queue.empty()) {
      std::string rid = ie->reader_queue.front();

      // send invalid to reader except the requesting client, which already invalid itself locally
      if (rid != cid) {
        pthread_mutex_unlock(&lock);
        handle(rid).safebind()->call(rextent_protocol::invalid, id, r);
        pthread_mutex_lock(&lock);
      }
      ie->reader_queue.pop_front();
    }
  }

  // remove inode cache entry on server
  delete inode_list[id];
  pthread_mutex_unlock(&lock);
  return extent_protocol::OK;
}

