//
// os.cpp
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

#include <pthread.h>
#include <thread>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <stdlib.h>
#include <stdio.h>

#include "os.h"

int OSCreateThread(OSThread_t *thread, OSThread_return_t (*func)(void*),
                   void *arg){
  return pthread_create(thread, 0, func, arg);
}

// wait for thread to end, storing its return value in *res
// Returns 0 if ok, non-zero if error
int OSWaitThread(OSThread_t thread, void **res){
  return pthread_join(thread, res);
}

int getNProcessors(){
  return (int) std::thread::hardware_concurrency();
}

// pins a thread to a processor
// returns 0 if ok, -1 if could not set affinity to processor, -2 if
// affinity set to processor and others
int pinThread(int processor){ 
  cpu_set_t cs1, cs2;
  int res;
  CPU_ZERO(&cs1);
  CPU_SET(processor, &cs1);
  // set the affinity
  res = pthread_setaffinity_np(pthread_self(), sizeof(cs1), &cs1);
  assert(res==0);
  // check the affinity
  res = pthread_getaffinity_np(pthread_self(), sizeof(cs2), &cs2);
  assert(res==0);
  if (!CPU_ISSET(processor, &cs2)) return -1;
  if (memcmp(&cs1, &cs2, sizeof(cs1)) != 0) return -2;
  return 0;
}

void mssleep(int ms){
  usleep((useconds_t)ms * 1000);
}

// returns a thread-id that is unique within the machine, even across processes
u64 gettid(){
  return syscall(SYS_gettid);
}
