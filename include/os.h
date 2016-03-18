//
// os.h
//
// OS-specific stuff, Linux implementation
//

/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  All rights reserved. 

  Written by Marcos K. Aguilera

  MIT License

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the "Software"), to deal in the Software without restriction,
  including without limitation the rights to use, copy, modify, merge,
  publish, distribute, sublicense, and/or sell copies of the Software,
  and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

#ifndef _OS_H
#define _OS_H

#include <mutex>
#include <condition_variable>
#include <stdint.h>
#include <semaphore.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>

#include "inttypes.h"

// rtchk is supposed to exit if condition is not satisfied
#define rtchk(cond)  assert(cond)

// these definitions for fetch-and-add, atomic-increment, and atomic-decrement
// are compiler specific
#define FetchAndAdd32(ptr32, val32) __sync_fetch_and_add((u32*)ptr32, val32)
#define FetchAndAdd64(ptr64, val64) __sync_fetch_and_add((u64*)ptr64, val64)
#define AtomicInc32(ptr32) __sync_add_and_fetch((u32*)ptr32, 1)
#define AtomicInc64(ptr64) __sync_add_and_fetch((u64*)ptr64, 1)
#define AtomicDec32(ptr32) __sync_sub_and_fetch((u32*)ptr32, 1)
#define AtomicDec64(ptr64) __sync_sub_and_fetch((u64*)ptr64, 1)
#define CompareSwap32(ptr32,cmp32,val32) __sync_val_compare_and_swap((u32*)ptr32,cmp32,val32)
#define CompareSwap64(ptr64,cmp64,val64) __sync_val_compare_and_swap((u64*)ptr64,cmp64,val64)
#define CompareSwapPtr(ptr,cmp,val) __sync_val_compare_and_swap((void**)ptr,(void*)cmp,(void*)val)
#define MemBarrier __sync_synchronize


// definitions to align a variable in 4-byte or 64-byte boundaries,
// also compiler specific
#define Align4 __attribute__((aligned(4)))
#define Align8 __attribute__((aligned(8)))
#define Align16 __attribute__((aligned(16)))
#define Align64 __attribute__((aligned(64)))

#define INFINITE -1

#define Tlocal thread_local

class RWLock {
  //friend class CondVar;
protected:
  std::mutex m;
public:
  RWLock(){ }
  ~RWLock(){ }
  void lock(void){ m.lock(); }
  void lockRead(void){ lock(); }
  void unlock(void){ m.unlock(); }
  void unlockRead(void){  unlock(); }
  int trylock(void){ return m.try_lock(); }
  int trylockRead(void){ return trylock(); }
  // the try functions return true if lock was gotten, false if someone
  // else holds the lock
};

//#define Semaphore Semaphore_CV
#define Semaphore Semaphore_POSIX

// Semaphore built with condition variable and mutex
class Semaphore_CV {
  static const int MAXSEMAPHOREVALUE=2147483647; // largest LONG
private:
  Align4 u32 value;
  std::mutex m;
  std::condition_variable cv;
public:
  Semaphore_CV(int initialValue=0){
    value = initialValue;
  }

  // returns true if timeout expired, false if semaphore has been signaled
  // if msTimeout=INFINITE then wait forever
  bool wait(int msTimeout){
    std::unique_lock<std::mutex> uniquelck(m);
    while (value==0){
      if (msTimeout != INFINITE){
        std::cv_status s;
        s = cv.wait_for(uniquelck, std::chrono::milliseconds(msTimeout));
        if (s == std::cv_status::timeout) return true;
      }
      else
        cv.wait(uniquelck);
    }
    AtomicDec32(&value);
    return false;
  }

  void signal(void){
    std::unique_lock<std::mutex> uniquelck(m);
    AtomicInc32(&value);
    cv.notify_one();
  }
};

// POSIX semaphore
class Semaphore_POSIX {
private:
  sem_t sem;
public:
  Semaphore_POSIX(int initialValue=0){
    int err;
    assert(initialValue >= 0);
    err = sem_init(&sem, 0, initialValue); assert(err==0);
  }
  
  // returns true if timeout expired, false if semaphore has been signaled
  // if msTimeout=INFINITE then wait forever
  bool wait(int msTimeout){
    int res;
    if (msTimeout == INFINITE){
      do {
        res = sem_wait(&sem);
        if (res) rtchk(errno == EINTR);
      } while (res != 0);
      return 0;
    } else {
      struct timespec ts;
      res = clock_gettime(CLOCK_REALTIME, &ts); rtchk(res==0);
      ts.tv_nsec += msTimeout * 1000000L;
      do {
        res = sem_timedwait(&sem, &ts);
        if (res && errno == ETIMEDOUT) break;
      } while (res != 0);
      return res;
    }
  }
  
  void signal(void){
    int res;
    res = sem_post(&sem); rtchk(res==0);
  }
};

#define EventSync EventSyncCV // use condition-variable implementation of events

class EventSyncCV {
private:
  pthread_cond_t cv;
  pthread_mutex_t cs;
  int flag;
public:
  EventSyncCV(){
    cv = PTHREAD_COND_INITIALIZER;
    cs = PTHREAD_MUTEX_INITIALIZER;
    flag = 0;
  }
  void set(){
    pthread_mutex_lock(&cs);
    flag = 1;
    pthread_cond_signal(&cv);
    pthread_mutex_unlock(&cs);
  }
  void reset(){
    pthread_mutex_lock(&cs);
    flag = 0;
    pthread_mutex_unlock(&cs);
  }
  int wait(void){  // returns 0 if wait satisfied, non-0 otherwise
    int res;
    pthread_mutex_lock(&cs);
    while (flag == 0){
      res = pthread_cond_wait(&cv, &cs);
      rtchk(res==0);
    }
    pthread_mutex_unlock(&cs);
    return 0;
  }
};


typedef pthread_t OSThread_t;
typedef void *OSThread_return_t;
#define OSTHREAD_FUNC OSThread_return_t
typedef OSTHREAD_FUNC (*OSTHREAD_FUNC_PTR)(void *);

// The start routine of a thread should be as follows:
//  OSTHREAD_FUNC name(void *arg){
//    ..
//    return (OSThread_return_t) numeric value;
//  }

// create a new thread starting func(arg)
// Sets *thread to thread id
// Returns 0 if ok, non-zero if error
int OSCreateThread(OSThread_t *thread, OSThread_return_t (*func)(void*),
                   void *arg);

// wait for thread to end, storing its return value in *res
// Returns 0 if ok, non-zero if error
int OSWaitThread(OSThread_t thread, void **res);

// returns number of processors
int getNProcessors();

// pins a thread to a processor
// returns 0 if ok, -1 if could not set affinity to processor,
// -2 if affinity set to processor and others
int pinThread(int processor);

// sleep for the given milliseconds
void mssleep(int ms);

// returns a thread-id that is unique within the machine, even across processes
u64 gettid();

#endif
