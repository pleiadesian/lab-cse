// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

// #define DEBUG


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
#ifdef DEBUG
  tprintf("%s acquire lock\n", id.c_str());
#endif
  // initialize cache entry if lock has not been in the local cache
  if (lock_list.find(lid) == lock_list.end()) {
    lock_cache *new_lock = new lock_cache();
    new_lock->revoke = 0;
    new_lock->retry = 0;
    new_lock->stat = NONE;
    lock_list[lid] = new_lock;
  }

  // tprintf("%s start acquire, status %d\n", id.c_str(), lock_list[lid]->stat);
  lock_cache *lc = lock_list[lid];

  if (lc->stat == FREE) {
    // acquire lock in local cache
    lc->stat = LOCKED;
#ifdef DEBUG
    tprintf("%s get lock %llu locally\n", id.c_str(), lid);
#endif
  } else if (lc->stat == LOCKED || lc->stat == ACQUIRING) {
    // lock is hold by local thread or acquiring from server by local thread, wait in the queue
    pthread_cond_t *thread_cond = new pthread_cond_t;
    pthread_cond_init(thread_cond, NULL);
    lc->thread_queue.push_back(thread_cond);
#ifdef DEBUG
    tprintf("%s wait %llu locally, queue size = %lu, status=%d\n", id.c_str(), lid, lc->thread_queue.size(),lc->stat);
#endif
    // thread can be awaked when LOCKED in local cache
    pthread_cond_wait(thread_cond, &lock);
    VERIFY (lc->stat == LOCKED);
    delete lc->thread_queue.front();
    lc->thread_queue.pop_front();
#ifdef DEBUG
  tprintf("client %s awaked , wait lock %llu in thread queue , after pop queue size =%lu\n", id.c_str(), lid,lc->thread_queue.size());
#endif  
  } else if (lc->stat == NONE || lc->stat == RELEASING) {
#ifdef DEBUG
    tprintf("%s no local cache stat=%d\n", id.c_str(), lc->stat);
#endif
    // wait until releasing done
    
    // pthread_mutex_unlock(&lock);
    // while (lc->stat != NONE) {}
    // pthread_mutex_lock(&lock);

    // thread awaked when RELEASING finished, lock stat set to NONE
    if (lc->stat == RELEASING) {
      pthread_cond_t *thread_cond = new pthread_cond_t;
      pthread_cond_init(thread_cond, NULL);
      lc->thread_queue.push_back(thread_cond);
#ifdef DEBUG
      tprintf("%s wait %llu to finish RELEASING, queue size = %lu, status=%d\n", id.c_str(), lid, lc->thread_queue.size(),lc->stat);
#endif
      // thread can be awaked when lock is not in local cache or lock passed by previous thread
      while (lc->stat != NONE && lc->stat != LOCKED)
        pthread_cond_wait(thread_cond, &lock);
      delete lc->thread_queue.front();
      lc->thread_queue.pop_front();
      if (lc->stat == LOCKED) {
          // wake up by local thread, do not need acquiring
#ifdef DEBUG
          tprintf("%s lock %llu locally after finish RELEASING and acquired by another thread, queue size = %lu, status=%d\n", id.c_str(), lid, lc->thread_queue.size(),lc->stat);
#endif
          pthread_mutex_unlock(&lock);
          return lock_protocol::OK;
      }
#ifdef DEBUG
      tprintf("%s re-acquire %llu after finish RELEASING, queue size = %lu, status=%d\n", id.c_str(), lid, lc->thread_queue.size(),lc->stat);
#endif
    }


    // tprintf("%s leave loop for NONE stat=%d\n", id.c_str(), lc->stat);
    // if (lc->stat == RELEASING) {
    //   pthread_cond_init(&lc->releasing_cv, NULL);
    //   while (lc->stat != NONE) {
    //     pthread_cond_wait(&lc->releasing_cv, &lock);
    //   }
    // }

    // acquire lock from server
    int r;
    lc->stat = ACQUIRING;
    while (lc->stat == ACQUIRING) {
#ifdef DEBUG
tprintf("%s start acquire server in acquiring loop, stat=%d\n", id.c_str(),lc->stat);
#endif
      pthread_mutex_unlock(&lock);
      int ret_retry = cl->call(lock_protocol::acquire, lid, id, r);
      pthread_mutex_lock(&lock);
#ifdef DEBUG
tprintf("%s stop acquire server in acquiring loop, stat=%d\n", id.c_str(),lc->stat);
#endif
      if (ret_retry == lock_protocol::OK) {
        lc->stat = LOCKED;
#ifdef DEBUG
    tprintf("%s get %llu after retry\n", id.c_str(), lid);
#endif
      } else {
        if (lc->retry == 0) {
          // has not received retry from server, just wait
          pthread_cond_init(&lc->retry_cond, NULL);
#ifdef DEBUG
  tprintf("%s wait %llu in retry while loop, stat=%d\n", id.c_str(), lid,lc->stat);
#endif
          pthread_cond_wait(&lc->retry_cond, &lock);
#ifdef DEBUG
  tprintf("client %s awaked after retrying for lock %llu , queue size=%lu, stat=%d\n", id.c_str(), lid, lc->thread_queue.size(), lc->stat);
#endif
          lc->retry = 0;
#ifdef DEBUG
  tprintf("client %s pop queue success\n", id.c_str());
#endif
        } else {
          // received retry from server, retry acquire
          lc->retry = 0;
        }
      }
    }
    // tprintf("release lock\n");
/**/
//     pthread_mutex_unlock(&lock);
//     int ret_rpc = cl->call(lock_protocol::acquire, lid, id, r);
//     pthread_mutex_lock(&lock);
//     // tprintf("acquire lock\n");

//     // tprintf("lock %llu thread queue size = %lu\n", lid, lc->thread_queue.size());

//     if (ret_rpc == lock_protocol::OK) {
//       // get lock from server
//       lc->stat = LOCKED;
// #ifdef DEBUG
//       tprintf("%s got lock %llu from server\n", id.c_str(), lid);
// #endif
//     } else {
//       // lock is not available on server, keep acquiring until retry_handler notify the thread
// #ifdef DEBUG
//       tprintf("%s wait %llu on server\n", id.c_str(), lid);
// #endif
//     }
/**/
  }
#ifdef DEBUG
  tprintf("%s get lock\n", id.c_str());
#endif
  pthread_mutex_unlock(&lock);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&lock);
  // tprintf("acquire lock\n");
  lock_cache *lc = lock_list[lid];
#ifdef DEBUG
  tprintf("%s start release %llu\n", id.c_str(), lid);
#endif
  if (lc->thread_queue.size() >= 1) {
    // release the lock and notify next thread
#ifdef DEBUG
    tprintf("%s release lock %llu locally and pass it to next, queue size=%lu\n", id.c_str(), lid, lc->thread_queue.size());
#endif
    pthread_cond_t *thread_cond = lc->thread_queue.front();
    pthread_cond_signal(thread_cond);
  } else if (lc->revoke == 1) {
    // no more thread in queue, delete the lock from cache when server revokes it
    int r;
    lc->stat = RELEASING;
    // tprintf("release lock\n");
#ifdef DEBUG
    tprintf("%s start RELEASING %llu\n", id.c_str(), lid);
#endif
    pthread_mutex_unlock(&lock);
    cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&lock);
    // tprintf("acquire lock\n");
    lc->revoke = 0;
    lc->stat = NONE;
#ifdef DEBUG
    tprintf("%s finish RELEASING %llu, wait queue size=%lu\n", id.c_str(), lid, lc->thread_queue.size());
#endif
    // wake up thread in queue after releasing finished, retry to acquire lock from server
    if (lc->thread_queue.size() > 0) {
#ifdef DEBUG
    tprintf("%s RELEASING %llu finished, try to wake up a thread in queue\n", id.c_str(), lid);
#endif
      pthread_cond_t *thread_cond = lc->thread_queue.front();
      pthread_cond_signal(thread_cond);
    }
#ifdef DEBUG
    tprintf("%s release lock %llu on server\n", id.c_str(), lid);
#endif
  } else {
    // no more thread and no server revoking, release lock in local cache
    lc->stat = FREE;
#ifdef DEBUG
    tprintf("%s release lock %llu locally, status is %d\n", id.c_str(), lid, lock_list[lid]->stat);
#endif
  }

  // tprintf("release lock\n");
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
  // tprintf("acquire lock\n");
  // tprintf("client %s get revoke %llu req from server, status is %d\n", id.c_str(), lid, lock_list[lid]->stat);
  // discard revoke when stat is RELEASING, in case of double release on server
  if (lc->stat == LOCKED || lc->stat == NONE || lc->stat == ACQUIRING) {
    // notify thread send release-lock rpc to server after finish its work
    // if retry arrive before OK, set lock's revoke to 1
    lc->revoke = 1;
#ifdef DEBUG
    tprintf("server wait to revoke %llu on %s\n", lid, id.c_str());
#endif
  } else if (lc->stat == FREE) {
    // release the lock on server when no thread owns it
    int r;
    lc->stat = RELEASING;
#ifdef DEBUG
    tprintf("server revoke %llu on %s directly\n", lid, id.c_str());
#endif
    // tprintf("release lock\n");
    pthread_mutex_unlock(&lock);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&lock);
    // tprintf("acquire lock\n");
    lc->stat = NONE;
#ifdef DEBUG
    tprintf("%s RELEASING %llu finished\n", id.c_str(), lid);
#endif
    // wake up thread in queue after releasing finished, retry to acquire lock from server
    if (lc->thread_queue.size() > 0) {
#ifdef DEBUG
    tprintf("%s RELEASING %llu finished, try to wake up a thread in queue\n", id.c_str(), lid);
#endif
      pthread_cond_t *thread_cond = lc->thread_queue.front();
      pthread_cond_signal(thread_cond);
    }
  }
#ifdef DEBUG
tprintf("%s release lock %llu\n", id.c_str(),lid);
#endif
  pthread_mutex_unlock(&lock);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&lock);
#ifdef DEBUG
  tprintf(" %s retry %llu , status %d \n", id.c_str(), lid, lock_list[lid]->stat);
#endif
  lock_cache *lc = lock_list[lid];
  lc->retry = 1;
  pthread_cond_signal(&lc->retry_cond);
  pthread_mutex_unlock(&lock);
  
  return ret;

/**/
//   pthread_mutex_lock(&lock);
//   // tprintf("acquire lock\n");
//   // tprintf(" %s retry %llu , status %d \n", id.c_str(), lid, lock_list[lid]->stat);
  
//   // retry before response
//   if (lock_list[lid]->stat != ACQUIRING) {
//     // tprintf("release lock\n");
//     pthread_mutex_unlock(&lock);
//     return ret;
//   }

// // tprintf("release lock\n");
//   pthread_mutex_unlock(&lock);
//   int r = cl->call(lock_protocol::acquire, lid, id, r);
//   pthread_mutex_lock(&lock);
//   // tprintf("acquire lock\n");
//   // tprintf("retry:1 on lock %llu\n", lid);
//   if (r == lock_protocol::OK) {
//     if (lock_list[lid]->stat == ACQUIRING) {
//       lock_list[lid]->stat = FREE;
//       if (lock_list[lid]->thread_queue.size() > 0) {
//       // release the lock and notify next thread
//         pthread_cond_t *thread_cond = lock_list[lid]->thread_queue.front();
        
//         // tprintf(" %s acquired %llu after retry, status %d, queue length %lu \n", id.c_str(), lid, lock_list[lid]->stat, lock_list[lid]->thread_queue.size());
//         pthread_cond_signal(thread_cond);
//       }
//     }
//   }
  
//   // tprintf("release lock\n");
//   pthread_mutex_unlock(&lock);
//   return ret;
/**/
}
