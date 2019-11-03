// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&lock, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;

  pthread_mutex_lock(&lock);
  int r;

  // initialize lock entry if lock has not been used, and lock it
  if (lock_list.find(lid) == lock_list.end()) {
    lock_entry *le = new lock_entry();
    le->stat = LOCKED;
    le->owner = id;
    lock_list[lid] = le;
    pthread_mutex_unlock(&lock);
    return lock_protocol::OK;
  }

  lock_entry *le = lock_list[lid];
  
  if (le->stat == LOCKED) {
    // acquire a lock locked by another client, start send revoke to owner
    le->wait_queue.push_back(id);    
    le->stat = REVOKING;
    pthread_mutex_unlock(&lock);
    handle(le->owner).safebind()->call(rlock_protocol::revoke, lid, r);
    return lock_protocol::RETRY;
  } else if (le->stat == REVOKING) {
    // acquire a lock revoking by another client from owner, just wait
    le->wait_queue.push_back(id);
    pthread_mutex_unlock(&lock);
    return lock_protocol::RETRY;
  } else if (le->stat == RETRYING) {
    // acquire a lock revoked from owner and waiting for retry_client to retry
    if (id == le->retry_client) {
      // retry_client get the lock
      le->stat = LOCKED;
      le->owner = id;
      le->retry_client.clear();

      // every time a new client get the lock, it should check if wait queue is empty. If not, revoke itself
      if (le->wait_queue.size() > 0) {
        le->stat = REVOKING;
        pthread_mutex_unlock(&lock);
        handle(id).safebind()->call(rlock_protocol::revoke, lid, r);
        return lock_protocol::OK;
      } else {
        pthread_mutex_unlock(&lock);
        return lock_protocol::OK;
      }
    } else {
      // not retry_client, just wait
      le->wait_queue.push_back(id);
      pthread_mutex_unlock(&lock);
      return lock_protocol::RETRY;
    }
  }

  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;

  pthread_mutex_lock(&lock);
  
  lock_entry *le = lock_list[lid];

  VERIFY(le->stat == REVOKING);
  VERIFY(le->wait_queue.size() > 0);

  le->stat = RETRYING;
  le->retry_client = le->wait_queue.front();
  le->wait_queue.pop_front();

  pthread_mutex_unlock(&lock);
  handle(le->retry_client).safebind()->call(rlock_protocol::retry, lid, r);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

