// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

// #define DEBUG
// #define DEBUG_QUEUE


lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&lock, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;

  pthread_mutex_lock(&lock);
#ifdef DEBUG
  tprintf("%s start acquire %llu\n", id.c_str(), lid);
#endif
  int r;

  // initialize lock entry if lock has not been used, and lock it
  if (lock_list.find(lid) == lock_list.end()) {
    lock_entry *le = new lock_entry();
    le->stat = LOCKED;
    le->owner = id;
    lock_list[lid] = le;
    pthread_mutex_unlock(&lock);
#ifdef DEBUG
tprintf("new lock %llu -> %s\n", lid, id.c_str());
#endif
    return lock_protocol::OK;
  }

  lock_entry *le = lock_list[lid];
  
  if (le->stat == LOCKED) {
    // acquire a lock locked by another client, start send revoke to owner
    le->wait_queue.push_back(id);
#ifdef DEBUG
tprintf("lock %llu owner %s, %s start REVOKING\n", lid, le->owner.c_str(), id.c_str());
#endif
#ifdef DEBUG_QUEUE
tprintf("add %s\nclient queue :", id.c_str());
      for (std::list<std::string>::iterator iter = le->wait_queue.begin(); iter != le->wait_queue.end(); iter++) {
        std::string temp = *iter;
        printf("->%s", temp.c_str());
      }
      printf("\n\n");
#endif
    
    le->stat = REVOKING;
    pthread_mutex_unlock(&lock);
    handle(le->owner).safebind()->call(rlock_protocol::revoke, lid, r);
    return lock_protocol::RETRY;
  } else if (le->stat == REVOKING) {
    // acquire a lock revoking by another client from owner, just wait
    le->wait_queue.push_back(id);

#ifdef DEBUG_QUEUE
tprintf("add %s\nclient queue :", id.c_str());
      for (std::list<std::string>::iterator iter = le->wait_queue.begin(); iter != le->wait_queue.end(); iter++) {
        std::string temp = *iter;
        printf("->%s", temp.c_str());
      }
      printf("\n\n");
#endif
#ifdef DEBUG
tprintf("lock %llu owner %s, is REVOKING, %s start waiting in queue\n", lid, le->owner.c_str(), id.c_str());
#endif

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
#ifdef DEBUG
tprintf("retry success lock %llu -> %s, revoke itself cause other client waiting\n", lid, id.c_str());
#endif
        pthread_mutex_unlock(&lock);
        handle(id).safebind()->call(rlock_protocol::revoke, lid, r);
        return lock_protocol::OK;
      } else {
#ifdef DEBUG
tprintf("retry success lock %llu -> %s, no more client waiting\n", lid, id.c_str());
#endif
        pthread_mutex_unlock(&lock);
        return lock_protocol::OK;
      }
    } else {
      // not retry_client, just wait
      le->wait_queue.push_back(id);

#ifdef DEBUG_QUEUE
      tprintf("add %s\nclient queue :", id.c_str());
      for (std::list<std::string>::iterator iter = le->wait_queue.begin(); iter != le->wait_queue.end(); iter++) {
        std::string temp = *iter;
        printf("->%s", temp.c_str());
      }
      printf("\n\n");
#endif
#ifdef DEBUG
tprintf("retry lock %llu retryer %s, not this %s, just wait\n", lid, le->retry_client.c_str(), id.c_str());
#endif

      pthread_mutex_unlock(&lock);
      return lock_protocol::RETRY;
    }
  }
/**/
  // // tprintf("lock %llu stat is %d\n", lid, lock_list[lid]->stat)
  // if (lock_list[lid]->stat == OK) {
  //   // tprintf("grant %llu to %s\n", lid, id.c_str());
  //   client_entry *ce = new client_entry();
  //   ce->id = id;
  //   ce->stat = LOCKED;
  //   lock_list[lid]->owner = ce;
  //   lock_list[lid]->stat = INVALID;

  //   // if any client is in wait_queue, ask front of queue to retry
  //   if (lock_list[lid]->wait_queue.size() >= 1) {
  //     std::string next_client_id = lock_list[lid]->wait_queue.front()->id;
  //     // tprintf("schedule next id %s to resend revoke %llu\n", next_client_id.c_str(), lid);
  //     lock_list[lid]->wait_queue.pop_front();
  //     pthread_mutex_unlock(&lock);
  //     int r;
  //     handle(next_client_id).safebind()->call(rlock_protocol::retry, lid, r);  // this retry is blocked every time, just for one more revoke req
  //   }
  // } else {
  //   int r;
  //   std::string owner_id = lock_list[lid]->owner->id;
  //   // tprintf("%s wait for %llu, and send revoke to %s\n", id.c_str(), lid, owner_id.c_str());
  //   client_entry *ce = new client_entry();
  //   ce->id = id;
  //   ce->stat = ACQUIRING;
  //   lock_list[lid]->wait_queue.push_back(ce);

  //   pthread_mutex_unlock(&lock);
  //   handle(owner_id).safebind()->call(rlock_protocol::revoke, lid, r);

  //   return lock_protocol::RETRY;
  // }


  // pthread_mutex_unlock(&lock);

// tprintf("%s release mutex\n", id.c_str());
  return ret;
/**/
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
#ifdef DEBUG
tprintf("%s  -- %llu --> %s need retry\n", id.c_str(), lid, le->retry_client.c_str());
#endif

#ifdef DEBUG_QUEUE
tprintf("pop %s\nclient queue :", le->retry_client.c_str());
      for (std::list<std::string>::iterator iter = le->wait_queue.begin(); iter != le->wait_queue.end(); iter++) {
        std::string temp = *iter;
        printf("->%s", temp.c_str());
      }
      printf("\n\n");
#endif

  pthread_mutex_unlock(&lock);
  handle(le->retry_client).safebind()->call(rlock_protocol::retry, lid, r);
  return ret;
    
/**/
//   if (lock_list[lid]->wait_queue.size() == 0) {
//     // tprintf("%s release %llu\n", id.c_str(), lid);
//     lock_list[lid]->stat = OK;
//     lock_list[lid]->owner->id = "";
//     lock_list[lid]->owner->stat = NONE;
//   } else {
//     std::string owner_id;
//     do {
//       lock_list[lid]->owner = lock_list[lid]->wait_queue.front();
//       owner_id = lock_list[lid]->owner->id;
//       lock_list[lid]->wait_queue.pop_front();
//     } while (owner_id == id && lock_list[lid]->wait_queue.size() > 0);
//     lock_list[lid]->stat = OK;

// // tprintf("%s release %llu, and awake %s\n", id.c_str(), lid, owner_id.c_str());

//     pthread_mutex_unlock(&lock);
//     handle(owner_id).safebind()->call(rlock_protocol::retry, lid, r);
//     return lock_protocol::OK;
//   }


  // pthread_mutex_unlock(&lock);

  // return ret;
  /**/
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

