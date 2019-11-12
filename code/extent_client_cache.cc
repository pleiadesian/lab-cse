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
// #include "handle.h"


#define LOCK_TEST
// #define DEBUG


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
  // rlsrpc->reg(rextent_protocol::retry, this, &extent_client_cache::retry_handler);
  pthread_mutex_init(&lock, NULL);
#ifdef DEBUG
  tprintf("finish init extent client\n");
#endif
}

extent_protocol::status
extent_client_cache::create(uint32_t type, extent_protocol::extentid_t &id)
{
    extent_protocol::status ret = extent_protocol::OK;
    ret = cl->call(extent_protocol::create, type, this->id, id);
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: %s CREATE start\n", this->id.c_str());
#endif

    // init cache
  if (inode_list.find(id) == inode_list.end()) {
#ifdef DEBUG
    tprintf("extent client: %s create - init cache for %llu\n", this->id.c_str(), id);
#endif
// #ifdef LOCK_TEST
//     pthread_mutex_unlock(&lock);
// #endif
//     ret = cl->call(extent_protocol::put, eid, id, buf, r);
// #ifdef LOCK_TEST
//     pthread_mutex_lock(&lock);
// #endif
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
    VERIFY (ret == extent_protocol::OK);
#ifdef DEBUG
    tprintf("extent client: %s create %llu --> *new\n", this->id.c_str(), id);
#endif
  } else {
#ifdef DEBUG
    tprintf("extent client: %s create - reuse cache for %llu\n", this->id.c_str(), id);
#endif
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
#ifdef DEBUG
  tprintf("extent client: %s CREATE end, inode=%llu\n", this->id.c_str(), id);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return ret;
}

extent_protocol::status
extent_client_cache::get(extent_protocol::extentid_t eid, std::string &buf)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: %s get %llu start\n", id.c_str(), eid);
#endif
  extent_protocol::status ret = extent_protocol::OK;

  // cold cache
  if (inode_list.find(eid) == inode_list.end()) {
    inode_cache *ic = new inode_cache();
    inode_list[eid] = ic;
#ifdef LOCK_TEST
      pthread_mutex_unlock(&lock);
#endif
      ret = cl->call(extent_protocol::get, eid, id, buf);
#ifdef LOCK_TEST
      pthread_mutex_lock(&lock);
#endif
//     if (ret == extent_protocol::RETRY) {
//       // VERIFY (!writer_id.empty());
// #ifdef LOCK_TEST
//       pthread_mutex_unlock(&lock);
// #endif
//       // handle(writer_id).safebind()->call(rextent_protocol::revoke, eid, r);
//       ret = cl->call(extent_protocol::get, eid, id, buf);
// #ifdef LOCK_TEST
//       pthread_mutex_lock(&lock);
// #endif
//       VERIFY (ret == extent_protocol::OK);
// //       pthread_cond_init(&ic->retry_reader, NULL);
// //       while (ic->retry == 0)
// //         pthread_cond_wait(&ic->retry_reader, &lock);
// //       VERIFY (ic->valid == 1);
// //       VERIFY (ic->stat == SHARED);
// //       buf = ic->buf;
// //       ic->atime = (unsigned int)time(NULL);
// // #ifdef DEBUG
// //   tprintf("%s get %llu --> *new\n", id.c_str(), eid);
// // #endif
// // #ifdef LOCK_TEST
// //   pthread_mutex_unlock(&lock);
// // #endif
// //       return extent_protocol::OK;
//     }

    ic->valid = 1;
    ic->buf = buf;
    ic->atime = (unsigned int)time(NULL);
#ifdef DEBUG
    tprintf("extent client: %s get %llu --> *new status=%d\n", id.c_str(), eid, ret);
#endif
    VERIFY (ret == extent_protocol::OK);

#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    return ret;
  }

  inode_cache *ic = inode_list[eid];

  // cache miss
  if (!ic->valid) {
    // std::string writer_id;
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    ret = cl->call(extent_protocol::get, eid, id, buf);
#ifdef LOCK_TEST
    pthread_mutex_lock(&lock);
#endif

//     if (ret == extent_protocol::RETRY) {
// #ifdef DEBUG
//       tprintf("%s get %llu - miss - start wait\n", id.c_str(), eid);
// #endif
//       // VERIFY (!writer_id.empty());
// #ifdef LOCK_TEST
//       pthread_mutex_unlock(&lock);
// #endif
//       // handle(writer_id).safebind()->call(rextent_protocol::revoke, eid, r);
//       ret = cl->call(extent_protocol::get, eid, id, buf);
// #ifdef LOCK_TEST
//       pthread_mutex_lock(&lock);
// #endif
//       VERIFY (ret == extent_protocol::OK);
// //       pthread_cond_init(&ic->retry_reader, NULL);
// //       while (ic->retry == 0)
// //         pthread_cond_wait(&ic->retry_reader, &lock);
// //       VERIFY (ic->valid == 1);
// //       VERIFY (ic->stat == SHARED);
// //       buf = ic->buf;
// //       ic->atime = (unsigned int)time(NULL);
// // #ifdef DEBUG
// //   tprintf("%s get %llu - miss - wait -> valid\n", id.c_str(), eid);
// // #endif
// // #ifdef LOCK_TEST
// //   pthread_mutex_unlock(&lock);
// // #endif
// //       return extent_protocol::OK;
//     }
    ic->stat = SHARED;
    ic->valid = 1;
    ic->buf = buf;
    ic->atime = (unsigned int)time(NULL);
#ifdef DEBUG
    tprintf("extent client: %s get %llu - miss -> valid\n", id.c_str(), eid);
#endif
    VERIFY (ret == extent_protocol::OK);
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    return ret;
  }

  // cache hit
  buf = ic->buf;
  ic->atime = (unsigned int)time(NULL);

#ifdef DEBUG
  tprintf("extent client: %s get %llu --> hit!\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return ret;
}

extent_protocol::status
extent_client_cache::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: %s getattr %llu start\n", id.c_str(), eid);
#endif
  extent_protocol::status ret = extent_protocol::OK;
  
  // cold cache
  if (inode_list[eid] == NULL) {
    inode_cache *ic = new inode_cache();
    inode_list[eid] = ic;
#ifdef DEBUG
    tprintf("extent client: %s getattr %llu cold cache, start request server\n", id.c_str(), eid);
#endif
    // std::string writer_id;
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    ret = cl->call(extent_protocol::getattr, eid, id, attr);
#ifdef LOCK_TEST
    pthread_mutex_lock(&lock);
#endif

//     if (ret == extent_protocol::RETRY) {
//       // ask writer to put newest data onto server
//       // VERIFY (!writer_id.empty());
// #ifdef LOCK_TEST
//       pthread_mutex_unlock(&lock);
// #endif
//       // handle(writer_id).safebind()->call(rextent_protocol::revoke, eid, r);
//       ret = cl->call(extent_protocol::getattr, eid, id, attr);
// #ifdef LOCK_TEST
//       pthread_mutex_lock(&lock);
// #endif
//       VERIFY (ret == extent_protocol::OK);  // this client hold lock for eid, no other client will get the lock



// // #ifdef DEBUG
// //       tprintf("%s getattr %llu - miss - start wait\n", id.c_str(), eid);
// // #endif
// //       pthread_cond_init(&ic->retry_reader, NULL);
// //       while (ic->retry == 0)
// //         pthread_cond_wait(&ic->retry_reader, &lock);
// //       VERIFY (ic->valid == 1);
// //       VERIFY (ic->attr_cached == 1);
// //       VERIFY (ic->stat == SHARED);
// //       attr = ic->attr;
// // #ifdef DEBUG
// //       tprintf("%s getattr %llu - miss - wait -> valid\n", id.c_str(), eid);
// // #endif
// // #ifdef LOCK_TEST
// //       pthread_mutex_unlock(&lock);
// // #endif
// //       return extent_protocol::OK;
//     }

    ic->attr_cached = 1;
    ic->attr = attr;
    VERIFY (ret == extent_protocol::OK);
#ifdef DEBUG
    tprintf("extent client: %s getattr %llu -cold cache-> *new\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    return ret;
  }

  inode_cache *ic = inode_list[eid];

  // data cache hit
  if (ic->valid) {
    if (!ic->attr_cached) {
      // attr cache miss, get attr from server
#ifdef DEBUG
      tprintf("extent client: %s getattr %llu cache miss, start request server\n", id.c_str(), eid);
#endif
      // std::string writer_id;
#ifdef LOCK_TEST
      pthread_mutex_unlock(&lock);
#endif
      ret = cl->call(extent_protocol::getattr, eid, id, attr);
#ifdef LOCK_TEST
      pthread_mutex_lock(&lock);
#endif

//       if (ret == extent_protocol::RETRY) {
// #ifdef DEBUG
//         tprintf("%s getattr %llu - hit - attr nocache - start revoke\n", id.c_str(), eid);
// #endif
//         // VERIFY (!writer_id.empty());
// #ifdef LOCK_TEST
//         pthread_mutex_unlock(&lock);
// #endif
//         // handle(writer_id).safebind()->call(rextent_protocol::revoke, eid, r);
//         ret = cl->call(extent_protocol::getattr, eid, id, attr);
// #ifdef LOCK_TEST
//         pthread_mutex_lock(&lock);
// #endif
// #ifdef DEBUG
//         tprintf("%s getattr %llu - hit - attr nocache - revoke success\n", id.c_str(), eid);
// #endif
//         VERIFY (ret == extent_protocol::OK);

//         // pthread_cond_init(&ic->retry_reader, NULL);
//         // while (ic->retry == 0)
//         //   pthread_cond_wait(&ic->retry_reader, &lock);
//         // VERIFY (ic->valid == 1);
//         // VERIFY (ic->attr_cached == 1);
//         // VERIFY (ic->stat == SHARED);

//         // critical attr should be adjusted to the newest version
// //         ic->attr.atime = ic->atime > ic->attr.atime ? ic->atime : ic->attr.atime;
// //         ic->attr.mtime = ic->mtime > ic->attr.mtime ? ic->mtime : ic->attr.mtime;
// //         ic->attr.ctime = ic->ctime > ic->attr.ctime ? ic->ctime : ic->attr.ctime;
// //         ic->attr.size = ic->buf.size();
// //         attr = ic->attr;
// // #ifdef DEBUG
// //         tprintf("%s getattr %llu - hit - attr nocache - wait -> valid\n", id.c_str(), eid);
// // #endif
// // #ifdef LOCK_TEST
// //         pthread_mutex_unlock(&lock);
// // #endif
// //         return extent_protocol::OK;
//       }

      // critical attr should be adjusted to the newest version
      attr.atime = ic->atime > attr.atime ? ic->atime : attr.atime;
      attr.mtime = ic->mtime > attr.mtime ? ic->mtime : attr.mtime;
      attr.ctime = ic->ctime > attr.ctime ? ic->ctime : attr.ctime;
      attr.size = ic->buf.size();

      ic->attr_cached = 1;
      ic->attr = attr;
#ifdef DEBUG
      tprintf("extent client: %s getattr %llu -miss-> *new\n", id.c_str(), eid);
#endif
    } else {
      // attr cache hit
#ifdef DEBUG
      tprintf("extent client: %s getattr %llu --> hit!\n", id.c_str(), eid);
#endif
      // critical attr should be adjusted to the newest version
      ic->attr.atime = ic->atime > ic->attr.atime ? ic->atime : ic->attr.atime;
      ic->attr.mtime = ic->mtime > ic->attr.mtime ? ic->mtime : ic->attr.mtime;
      ic->attr.ctime = ic->ctime > ic->attr.ctime ? ic->ctime : ic->attr.ctime;
      ic->attr.size = ic->buf.size();

      attr = ic->attr;
    }
  } else {
    // attr in cache may be stale, get attr from server
#ifdef DEBUG
      tprintf("extent client: %s getattr %llu data and attr cache miss, start request server\n", id.c_str(), eid);
#endif
      // std::string writer_id;
      std::string buf;
#ifdef LOCK_TEST
      pthread_mutex_unlock(&lock);
#endif
      ret = cl->call(extent_protocol::getattr, eid, id, attr);

      ret = cl->call(extent_protocol::get, eid, id, buf);  // prefetch the data to reduce getattr rpc
#ifdef LOCK_TEST
      pthread_mutex_lock(&lock);
#endif

      ic->stat = SHARED;
      ic->valid = 1;
      ic->buf = buf;
      ic->atime = (unsigned int)time(NULL);
#ifdef DEBUG
      tprintf("extent client: %s get %llu - miss -> valid\n", id.c_str(), eid);
#endif
      VERIFY (ret == extent_protocol::OK);

//       if (ret == extent_protocol::RETRY) {
// #ifdef DEBUG
//         tprintf("%s getattr %llu - miss - start wait\n", id.c_str(), eid);
// #endif
//       // VERIFY (!writer_id.empty());
// #ifdef LOCK_TEST
//       pthread_mutex_unlock(&lock);
// #endif
//       // handle(writer_id).safebind()->call(rextent_protocol::revoke, eid, r);
//       ret = cl->call(extent_protocol::getattr, eid, id, attr);
// #ifdef LOCK_TEST
//       pthread_mutex_lock(&lock);
// #endif
// //         pthread_cond_init(&ic->retry_reader, NULL);
// //         while (ic->retry == 0)
// //           pthread_cond_wait(&ic->retry_reader, &lock);
// //         VERIFY (ic->valid == 1);
// //         VERIFY (ic->attr_cached == 1);
// //         VERIFY (ic->stat == SHARED);
// //         attr = ic->attr;
// // #ifdef DEBUG
// //         tprintf("%s getattr %llu - miss - wait -> valid\n", id.c_str(), eid);
// // #endif
// // #ifdef LOCK_TEST
// //         pthread_mutex_unlock(&lock);
// // #endif
// //         return extent_protocol::OK;
//       }

      ic->attr_cached = 1;
      ic->attr = attr;
#ifdef DEBUG
      tprintf("extent client: %s getattr %llu data and attr cache miss, update local attr cache and data cache\n", id.c_str(), eid);
#endif
  }

#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return ret;
}

extent_protocol::status
extent_client_cache::put(extent_protocol::extentid_t eid, std::string buf)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: %s start put %llu\n", id.c_str(), eid);
#endif
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // handle INIT when start a new yfs_client
  if (eid == 1 && buf == "") {
#ifdef DEBUG
    tprintf("extent client: %s put %llu "" for init\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
    return extent_protocol::OK;
  }

  // cold cache
  if (inode_list.find(eid) == inode_list.end()) {
#ifdef DEBUG
  tprintf("extent client: %s call put for cold cache\n", id.c_str());
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
    inode_cache *ic = new inode_cache();
    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = buf;
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);
    inode_list[eid] = ic;
    VERIFY (ret == extent_protocol::OK);
#ifdef DEBUG
  tprintf("extent client: %s put %llu --> *new\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
    return ret;
  }

  inode_cache *ic = inode_list[eid];

  // cache miss
  if (!ic->valid) {
#ifdef DEBUG
    tprintf("extent client: start put\n");
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
#ifdef LOCK_TEST
    pthread_mutex_lock(&lock);
#endif
    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = buf;
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);
    VERIFY (ret == extent_protocol::OK);
#ifdef DEBUG
    tprintf("extent client: %s put %llu - miss -> valid\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    return ret;
  }

  // cache is SHARED, put data to server
  if (ic->stat == SHARED) {
#ifdef DEBUG
    tprintf("extent client: %s call put , because it's SHARED\n", id.c_str());
#endif

#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
#ifdef LOCK_TEST
    pthread_mutex_lock(&lock);
#endif
    ic->stat = EXCLUSIVE;
    ic->valid = 1;
    ic->buf = buf;
    ic->ctime = (unsigned int)time(NULL);
    ic->mtime = (unsigned int)time(NULL);
    VERIFY (ret == extent_protocol::OK);
#ifdef DEBUG
  tprintf("extent client: %s put %llu - shared -> exclusive\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
    pthread_mutex_unlock(&lock);
#endif
    return ret;
  }

  // cache hit
  ic->buf = buf;
  ic->mtime = (unsigned int)time(NULL);
  ic->ctime = (unsigned int)time(NULL);

#ifdef DEBUG
  tprintf("extent client: %s put %llu --> hit!\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return ret;
}

extent_protocol::status
extent_client_cache::remove(extent_protocol::extentid_t eid)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: remove start\n");
#endif
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  
  inode_cache *ic = inode_list[eid];
  VERIFY (ic != NULL);
  ic->valid = 0;
  ic->attr_cached = 0;
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  ret = cl->call(extent_protocol::remove, eid, id, r);
  return ret;
}

rextent_protocol::status
extent_client_cache::invalid_handler(extent_protocol::extentid_t eid, int &r)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: invalid start\n");
#endif

  inode_cache *ic = inode_list[eid];
  VERIFY (ic != NULL);
  VERIFY (ic->valid == 1);
  ic->valid = 0;
  ic->attr_cached = 0;
  ic->atime = 0;
  ic->mtime = 0;
  ic->ctime = 0;
  
#ifdef DEBUG
  tprintf("extent client: invalid end\n");
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  return rextent_protocol::OK;
}

rextent_protocol::status
extent_client_cache::revoke_handler(extent_protocol::extentid_t eid, int &r)
{
#ifdef LOCK_TEST
  pthread_mutex_lock(&lock);
#endif
#ifdef DEBUG
  tprintf("extent client: %s receive revoke for %llu\n", id.c_str(), eid);
#endif
  inode_cache *ic = inode_list[eid];  
  VERIFY (ic != NULL);
  VERIFY (ic->valid == 1);
  VERIFY (ic->stat == EXCLUSIVE);
  ic->valid = 0;
  std::string buf = ic->buf;
#ifdef DEBUG
  tprintf("extent client: %s start put revoke to %llu\n", id.c_str(), eid);
#endif
#ifdef LOCK_TEST
  pthread_mutex_unlock(&lock);
#endif
  cl->call(extent_protocol::put, eid, id, buf, r);

  return rextent_protocol::OK;
}
