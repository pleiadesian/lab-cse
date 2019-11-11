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

#define LOCK_TEST
#define DEBUG

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
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif

#ifdef DEBUG
  tprintf("extent_server: create inode\n");
#endif
  id = im->alloc_inode(type);

  inode_entry *ie = new inode_entry();
  // ie->stat = SHARED;
  ie->stat = EXCLUSIVE;
  ie->writer = cid;
  inode_list[id] = ie;


#ifdef DEBUG
  tprintf("extent_server: end create inode %llu\n", id);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return extent_protocol::OK;
}

int extent_server_cache::put(extent_protocol::extentid_t id, std::string cid, std::string buf, int &)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent_server: %s put %llu start\n", cid.c_str(), id);
#endif
  id &= 0x7fffffff;

  // handle INIT when start a new yfs_client
  if (id == 1 && buf == "") {
    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

#ifdef DEBUG
  tprintf("extent_server: %s put init to inode 1\n", cid.c_str());
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
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
#ifdef DEBUG
        tprintf("extent_server: %s put %llu --> invalid %s\n", cid.c_str(), id, rid.c_str());
#endif
#ifdef LOCK_TEST
        pthread_mutex_unlock(&lock);
#endif
        handle(rid).safebind()->call(rextent_protocol::invalid, id, r);
#ifdef LOCK_TEST
        pthread_mutex_lock(&lock);
#endif
      }

      ie->reader_queue.pop_front();
    }

    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

#ifdef DEBUG
    tprintf("extent_server: %s put %llu --> exclusive\n", cid.c_str(), id);
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    return extent_protocol::OK;
  } else {
    // exclusive writer write back data, wake up a reader waiting in the queue
    VERIFY (ie->writer == cid);
    // write data onto disk
    const char * cbuf = buf.c_str();
    int size = buf.size();
    im->write_file(id, cbuf, size);

    // get newest attr
    // extent_protocol::attr attr;
    // memset(&attr, 0, sizeof(attr));
    // im->getattr(id, attr);

    ie->stat = SHARED;
    VERIFY (ie->reader_queue.empty());
    // VERIFY (ie->reader_queue.size() == 1);
    // std::string reader_id = ie->reader_queue.front();
#ifdef DEBUG
    tprintf("extent_server: %s put %llu --> wait other to retry\n", cid.c_str(), id);
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    // handle(reader_id).safebind()->call(rextent_protocol::retry, id, buf, attr, r);
    return extent_protocol::OK;
  }
}

int extent_server_cache::get(extent_protocol::extentid_t id, std::string cid , std::string &buf)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent_server: %s get %lld start\n", cid.c_str(), id);
#endif
  id &= 0x7fffffff;

  inode_entry *ie = inode_list[id];
  VERIFY (ie != NULL);

  if (ie->stat == EXCLUSIVE) {
    // revoke dirty data in writer's cache and let client wait
    int r;
    VERIFY (ie->reader_queue.empty());
    // ie->reader_queue.push_back(cid);
#ifdef DEBUG
    tprintf("extent_server: %s get %lld --> start revoke from %s\n", cid.c_str(), id, ie->writer.c_str());
#endif
    VERIFY (ie->writer != cid);
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    handle(ie->writer).safebind()->call(rextent_protocol::revoke, id, r);
#ifdef LOCK_TEST
    pthread_mutex_lock(&lock);
#endif
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
#ifdef DEBUG
  tprintf("extent_server: %s get %lld end\n", cid.c_str(), id);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return extent_protocol::OK;
}

int extent_server_cache::getattr(extent_protocol::extentid_t id, std::string cid, extent_protocol::attr &a)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent_server: getattr %llu start\n", id);
#endif
  id &= 0x7fffffff;

  inode_entry *ie = inode_list[id];
  VERIFY (ie != NULL);

  if (ie->stat == EXCLUSIVE && ie->writer != cid) {
    // revoke dirty data in writer's cache and let client wait
    int r;
    VERIFY (ie->reader_queue.empty());
    // ie->reader_queue.push_back(cid);
#ifdef DEBUG
    tprintf("extent_server: %s getattr %lld --> revoke from %s\n", cid.c_str(), id, ie->writer.c_str());
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    handle(ie->writer).safebind()->call(rextent_protocol::revoke, id, r);
#ifdef LOCK_TEST
    pthread_mutex_lock(&lock);
#endif
  }

  // read attr from disk
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;

#ifdef DEBUG
  tprintf("extent_server: getattr %llu end\n", id);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return extent_protocol::OK;
}

int extent_server_cache::remove(extent_protocol::extentid_t id, std::string cid, int &)
{
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent_server: write %lld\n", id);
#endif
  id &= 0x7fffffff;
  im->remove_file(id);

  int r;
  inode_entry *ie = inode_list[id];

  VERIFY (ie != NULL);

  // invalid all cache of this inode on client
  if (ie->stat == EXCLUSIVE) {
    // send invalid to writer except the requestig client, which already invalid itself locally
    if (ie->writer != cid) {
#ifdef LOCK_TEST
      pthread_mutex_unlock(&lock);
#endif
      handle(ie->writer).safebind()->call(rextent_protocol::invalid, id, r);
#ifdef LOCK_TEST
      pthread_mutex_lock(&lock);
#endif
    }
  } else {
    while (!ie->reader_queue.empty()) {
      std::string rid = ie->reader_queue.front();
      // send invalid to reader except the requesting client, which already invalid itself locally
      if (rid != cid) {
#ifdef DEBUG
        tprintf("extent_server: %s remove %llu --> invalid %s\n", cid.c_str(), id, rid.c_str());
#endif
#ifdef LOCK_TEST
        pthread_mutex_unlock(&lock);
#endif
        handle(rid).safebind()->call(rextent_protocol::invalid, id, r);
#ifdef LOCK_TEST
        pthread_mutex_lock(&lock);
#endif
      }
      ie->reader_queue.pop_front();
    }
  }

  // remove inode cache entry on server
  delete inode_list[id];
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return extent_protocol::OK;
}

