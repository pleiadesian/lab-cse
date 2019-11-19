// RPC stubs for clients to talk to extent_server

#include "extent_client_cache.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "rpc.h"
#include "lang/verify.h"
#include "tprintf.h"


int extent_client_cache::last_port = ~0; // diff from last_port of lock client cache

extent_client_cache::extent_client_cache(std::string dst)
  : extent_client(dst)
{
  srand(time(NULL)^last_port);
  rextent_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rextent_port;
  id = host.str();
  last_port = rextent_port;
  rpcs *rlsrpc = new rpcs(rextent_port);
  rlsrpc->reg(rextent_protocol::invalid, this, &extent_client_cache::invalid_handler);
  rlsrpc->reg(rextent_protocol::revoke, this, &extent_client_cache::revoke_handler);
  pthread_mutex_init(&lock, NULL);
}

extent_protocol::status
extent_client_cache::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create, type, this->id, id);

  pthread_mutex_lock(&lock);

  // init cache, creater hold the cache as an exclusive writer, attr cached
  if (inode_list.find(id) == inode_list.end()) {
    inode_cache *ic = new inode_cache();
    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = "";
    ic->atime = (unsigned int)time(NULL);
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);
    extent_protocol::attr attr;
    attr.atime = ic->atime;
    attr.ctime = ic->ctime;
    attr.mtime = ic->mtime;
    attr.size = 0;
    attr.type = type;
    ic->attr = attr;
    ic->attr_cached = 1;
    inode_list[id] = ic;
  } else {
    inode_cache *ic = inode_list[id];
    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = "";
    ic->atime = (unsigned int)time(NULL);
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);
    extent_protocol::attr attr;
    attr.atime = ic->atime;
    attr.ctime = ic->ctime;
    attr.mtime = ic->mtime;
    attr.size = 0;
    attr.type = type;
    ic->attr = attr;
    ic->attr_cached = 1;
  }

  pthread_mutex_unlock(&lock);
  return ret;
}

extent_protocol::status
extent_client_cache::get(extent_protocol::extentid_t eid, std::string &buf)
{
  pthread_mutex_lock(&lock);
  extent_protocol::status ret = extent_protocol::OK;

  // cold cache, acquire cache from server as a shared reader
  if (inode_list.find(eid) == inode_list.end()) {
    inode_cache *ic = new inode_cache();
    inode_list[eid] = ic;

    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::get, eid, id, buf);
    pthread_mutex_lock(&lock);

    ic->valid = 1;
    ic->buf = buf;
    ic->atime = (unsigned int)time(NULL);

    pthread_mutex_unlock(&lock);
    return ret;
  }

  inode_cache *ic = inode_list[eid];

  // cache miss, acquire cache from server as a shared reader
  if (!ic->valid) {
    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::get, eid, id, buf);
    pthread_mutex_lock(&lock);

    ic->stat = SHARED;
    ic->valid = 1;
    ic->buf = buf;
    ic->atime = (unsigned int)time(NULL);
  
    pthread_mutex_unlock(&lock);
    return ret;
  }

  // cache hit, read from cache
  buf = ic->buf;
  ic->atime = (unsigned int)time(NULL);

  pthread_mutex_unlock(&lock);
  return ret;
}

extent_protocol::status
extent_client_cache::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  pthread_mutex_lock(&lock);
  extent_protocol::status ret = extent_protocol::OK;
  
  // cold cache
  if (inode_list[eid] == NULL) {
    inode_cache *ic = new inode_cache();
    inode_list[eid] = ic;

    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::getattr, eid, id, attr);
    pthread_mutex_lock(&lock);

    ic->attr_cached = 1;
    ic->attr = attr;

    pthread_mutex_unlock(&lock);
    return ret;
  }

  inode_cache *ic = inode_list[eid];

  // data cache hit
  if (ic->valid) {
    if (!ic->attr_cached) {
      // attr cache miss, get attr from server
      pthread_mutex_unlock(&lock);
      ret = cl->call(extent_protocol::getattr, eid, id, attr);
      pthread_mutex_lock(&lock);

      // critical attr should be adjusted to the newest version
      attr.atime = ic->atime > attr.atime ? ic->atime : attr.atime;
      attr.mtime = ic->mtime > attr.mtime ? ic->mtime : attr.mtime;
      attr.ctime = ic->ctime > attr.ctime ? ic->ctime : attr.ctime;
      attr.size = ic->buf.size();

      ic->attr_cached = 1;
      ic->attr = attr;
    } else {
      // attr cache hit, critical attr should be adjusted to the newest version
      ic->attr.atime = ic->atime > ic->attr.atime ? ic->atime : ic->attr.atime;
      ic->attr.mtime = ic->mtime > ic->attr.mtime ? ic->mtime : ic->attr.mtime;
      ic->attr.ctime = ic->ctime > ic->attr.ctime ? ic->ctime : ic->attr.ctime;
      ic->attr.size = ic->buf.size();

      attr = ic->attr;
    }
  } else {
    // client doesn't hold the data cache, so attr in cache may be stale, get attr from server
    std::string buf;
    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::getattr, eid, id, attr);
    ret = cl->call(extent_protocol::get, eid, id, buf);  // prefetch the data to reduce getattr rpc
    pthread_mutex_lock(&lock);

    ic->stat = SHARED;
    ic->valid = 1;
    ic->buf = buf;
    ic->atime = (unsigned int)time(NULL);
    ic->attr_cached = 1;
    ic->attr = attr;
  }

  pthread_mutex_unlock(&lock);
  return ret;
}

extent_protocol::status
extent_client_cache::put(extent_protocol::extentid_t eid, std::string buf)
{
  pthread_mutex_lock(&lock);
  int r;
  extent_protocol::status ret = extent_protocol::OK;

  // handle INIT when start a new yfs_client
  if (eid == 1 && buf == "") {
    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
    return extent_protocol::OK;
  }

  // cold cache
  if (inode_list.find(eid) == inode_list.end()) {
    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
    pthread_mutex_lock(&lock);
    inode_cache *ic = new inode_cache();
    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = buf;
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);
    inode_list[eid] = ic;
  
    pthread_mutex_unlock(&lock);
    return ret;
  }

  inode_cache *ic = inode_list[eid];

  // cache miss
  if (!ic->valid) {
    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
    pthread_mutex_lock(&lock);

    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = buf;
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);

    pthread_mutex_unlock(&lock);
    return ret;
  }

  // cache is SHARED, put data to server and acquire the cache as an exclusive writer
  if (ic->stat == SHARED) {
    pthread_mutex_unlock(&lock);
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
    pthread_mutex_lock(&lock);

    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = buf;
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);

    pthread_mutex_unlock(&lock);
    return ret;
  }

  // cache hit, write into cache
  ic->buf = buf;
  ic->mtime = (unsigned int)time(NULL);
  ic->ctime = (unsigned int)time(NULL);

  pthread_mutex_unlock(&lock);
  return ret;
}

extent_protocol::status
extent_client_cache::remove(extent_protocol::extentid_t eid)
{
  pthread_mutex_lock(&lock);
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  
  // clear cache
  inode_cache *ic = inode_list[eid];
  ic->valid = 0;
  ic->attr_cached = 0;

  pthread_mutex_unlock(&lock);
  ret = cl->call(extent_protocol::remove, eid, id, r);
  return ret;
}

rextent_protocol::status
extent_client_cache::invalid_handler(extent_protocol::extentid_t eid, int &r)
{
  pthread_mutex_lock(&lock);

  inode_cache *ic = inode_list[eid];
  ic->valid = 0;
  ic->attr_cached = 0;
  ic->atime = 0;
  ic->mtime = 0;
  ic->ctime = 0;

  pthread_mutex_unlock(&lock);
  return rextent_protocol::OK;
}

rextent_protocol::status
extent_client_cache::revoke_handler(extent_protocol::extentid_t eid, std::string &buf)
{
  pthread_mutex_lock(&lock);

  inode_cache *ic = inode_list[eid];  
  ic->valid = 0;
  // std::string buf = ic->buf;
  buf = ic->buf;

  pthread_mutex_unlock(&lock);
  return rextent_protocol::OK;
}
