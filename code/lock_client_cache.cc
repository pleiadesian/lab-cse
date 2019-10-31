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
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;

  pthread_mutex_lock(&lock);

  // initialize cache entry if lock has not been in the local cache
  if (lock_stat.find(lid) == lock_stat.end()) {
    lock_cache *new_lock = new lock_cache();
    new_lock->revoke = 0;
    new_lock->stat = NOT_CACHED;
    lock_list[lid] = new_lock;
  }

  if (lock_list[lid]->stat == OK) {
    // acquire lock in local cache
    lock_list[lid]->stat = LOCKED;
  } else if (lock_list[lid]->stat == LOCKED) {
    // lock is hold by local thread, wait in the queue
    block_on_lock(lid);
  } else if (lock_list[lid]->stat == NOT_CACHED) {
    int r;
    // acquire lock from server
    pthread_mutex_unlock(&lock);
    int ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&lock);
    if (ret == lock_protocol::OK) {
      lock_stat[lid] = LOCKED;
    } else if (ret == lock_protocol::RETRY) {
      // lock is not available on server
      block_on_lock(lid);
    }
  }
  pthread_mutex_unlock(&lock);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&lock);
  
  if (lock_list[lid]->thread_queue.size >= 1) {
    // release the lock and notify next thread
    pthread_cond_t *thread_cond = lock_list[lid]->thread_queue.pop_front();
    lock_list[lid]->stat = OK;
    pthread_cond_signal(thread_cond);
  } else if (lock_list[lid]->revoke == 1) {
    // delete the lock from cache when server revokes it
    int r;
    pthread_mutex_unlock(&lock);
    int ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&lock);
    lock_list[lid]->revoke = 0;
    lock_list[lid]->stat = NOT_CACHED;
  } else {
    // release lock in local cache
    lock_list[lid]->stat = OK;
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
  if (lock_list[lid]->stat = LOCKED) {
    // notify thread send release-lock rpc to server after finish its work
    lock_list[lid]->revoke = 1;
  } else {
    // release the lock on server when no thread owns it
    int r;
    pthread_mutex_unlock(&lock);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&lock);  
    lock_list[lid]->stat = NOT_CACHED;
  }

  pthread_mutex_unlock(&lock);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;

  // retry acquiring the lock
  int r = cl->call(lock_protocol::acquire, lid, id, r);
  if (r == lock_protocol::OK) {
    // release the lock and notify next thread
    pthread_cond_t *thread_cond = lock_list[lid]->thread_queue.pop_front();
    lock_list[lid]->stat = OK;
    pthread_cond_signal(thread_cond);
  }

  return ret;
}

lock_protocol::status
lock_client_cache::block_on_lock(lock_protocol::lockid_t lid)
{
    pthread_cond_t *thread_cond = new pthread_cond_t;
    pthread_cond_init(thread_cond, NULL);
    lock_list[lid]->thread_queue.push_back(thread_cond);
    while (lock_list[lid]->stat != OK) 
      pthread_cond_wait(thread_cond, &lock);
    lock_list[lid]->stat = LOCKED;
}

