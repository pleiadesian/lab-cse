// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  pthread_mutex_init(&lock, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;

  pthread_mutex_lock(&lock);

  // initialize cache entry if lock has not been in the local cache
  if (lock_list.find(lid) == lock_list.end()) {
    lock_cache *new_lock = new lock_cache();
    new_lock->revoke = 0;
    new_lock->retry = 0;
    new_lock->stat = NONE;
    lock_list[lid] = new_lock;
  }

  lock_cache *lc = lock_list[lid];

  if (lc->stat == FREE) {
    // acquire lock in local cache
    lc->stat = LOCKED;
  } else if (lc->stat == LOCKED || lc->stat == ACQUIRING) {
    // lock is hold by local thread or acquiring from server by local thread, wait in the queue
    pthread_cond_t *thread_cond = new pthread_cond_t;
    pthread_cond_init(thread_cond, NULL);
    lc->thread_queue.push_back(thread_cond);

    // thread can be awaked when LOCKED in local cache
    pthread_cond_wait(thread_cond, &lock);
    VERIFY (lc->stat == LOCKED);
    delete lc->thread_queue.front();
    lc->thread_queue.pop_front();
  } else if (lc->stat == NONE || lc->stat == RELEASING) {
    // thread awaked when RELEASING finished, lock stat set to NONE
    if (lc->stat == RELEASING) {
      pthread_cond_t *thread_cond = new pthread_cond_t;
      pthread_cond_init(thread_cond, NULL);
      lc->thread_queue.push_back(thread_cond);

      // thread can be awaked when lock is not in local cache or lock passed by previous thread
      while (lc->stat != NONE && lc->stat != LOCKED)
        pthread_cond_wait(thread_cond, &lock);
      delete lc->thread_queue.front();
      lc->thread_queue.pop_front();
      if (lc->stat == LOCKED) {
          // wake up by local thread, do not need acquiring
          pthread_mutex_unlock(&lock);
          return lock_protocol::OK;
      }
    }

    // acquire lock from server
    int r;
    lc->stat = ACQUIRING;
    while (lc->stat == ACQUIRING) {
      pthread_mutex_unlock(&lock);
      int ret_retry = cl->call(lock_protocol::acquire, lid, id, r);
      pthread_mutex_lock(&lock);
      if (ret_retry == lock_protocol::OK) {
        lc->stat = LOCKED;
      } else {
        if (lc->retry == 0) {
          // has not received retry from server, just wait
          pthread_cond_init(&lc->retry_cond, NULL);
          pthread_cond_wait(&lc->retry_cond, &lock);
          lc->retry = 0;
        } else {
          // received retry from server, retry acquire
          lc->retry = 0;
        }
      }
    }
  }
  pthread_mutex_unlock(&lock);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&lock);

  lock_cache *lc = lock_list[lid];
  
  if (lc->thread_queue.size() >= 1) {
    // release the lock and notify next thread
    pthread_cond_t *thread_cond = lc->thread_queue.front();
    pthread_cond_signal(thread_cond);
  } else if (lc->revoke == 1) {
    // no more thread in queue, delete the lock from cache when server revokes it
    int r;
    lc->stat = RELEASING;
    pthread_mutex_unlock(&lock);
    cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&lock);
    lc->revoke = 0;
    lc->stat = NONE;

    // wake up thread in queue after releasing finished, retry to acquire lock from server
    if (lc->thread_queue.size() > 0) {
      pthread_cond_t *thread_cond = lc->thread_queue.front();
      pthread_cond_signal(thread_cond);
    }
  } else {
    // no more thread and no server revoking, release lock in local cache
    lc->stat = FREE;
  }

  pthread_mutex_unlock(&lock);
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&lock);

  lock_cache *lc = lock_list[lid];
 
  // discard revoke when stat is RELEASING, in case of double release on server
  if (lc->stat == LOCKED || lc->stat == NONE || lc->stat == ACQUIRING) {
    // notify thread send release-lock rpc to server after finish its work
    // if retry arrive before OK, set lock's revoke to 1
    lc->revoke = 1;
  } else if (lc->stat == FREE) {
    // release the lock on server when no thread owns it
    int r;
    lc->stat = RELEASING;
    pthread_mutex_unlock(&lock);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&lock);
    lc->stat = NONE;

    // wake up thread in queue after releasing finished, retry to acquire lock from server
    if (lc->thread_queue.size() > 0) {
      pthread_cond_t *thread_cond = lc->thread_queue.front();
      pthread_cond_signal(thread_cond);
    }
  }

  pthread_mutex_unlock(&lock);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&lock);
  lock_cache *lc = lock_list[lid];
  lc->retry = 1;
  pthread_cond_signal(&lc->retry_cond);
  pthread_mutex_unlock(&lock);
  
  return ret;
}
