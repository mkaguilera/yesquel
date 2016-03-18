//
// test-various.cpp
//
// Tests data structures and other general-purpose functions.
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

#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <map>
#include <list>
#include <malloc.h>
#include <signal.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>

#include "os.h"
#include "datastruct.h"
#include "datastructmt.h"
#include "util.h"
#include "task.h"
#include "prng.h"
#include "tmalloc-include.h"
#include "task.h"
#include "warning.h"

struct Int {
  int val;
  static int cmp(const Int &left, const Int &right){
    if (left.val==right.val) return 0;
    if (left.val<right.val) return -1;
    else return +1;
  }
  Int(int v){ val = v; }
  Int(){ val = 0; }
  void setinvalid(){ val = -99999; }
  bool isinvalid(){ return val == -99999; }
};

#define SLIST_HEIGHTS_OPS 1000000
void test_slist_heights(){
  int heights[28];
  int h,i;
  Int tmpint;
  SkipList<Int,int> s;
  SkipListNode<Int,int> *ptr;
  int expected_number;
  
  for (int i=0; i < SLIST_HEIGHTS_OPS; ++i){
    tmpint.val = i;
    s.insert(tmpint, 0);
  }
  memset(heights, 0, sizeof(heights));
  for (ptr = s.getFirst(); ptr != s.getLast(); ptr = s.getNext(ptr)){
    h = ptr->nlevels;
    if (h >= 28){  // heights above 28 not counted
      printf("Height %d not counted (too high)\n", h);
      continue;
    }
    ++heights[h];
  }
  expected_number = SLIST_HEIGHTS_OPS / 2;
  for (i=1; i < 28; ++i){
    assert(expected_number / 2 <= heights[i] &&
           heights[i] <= expected_number * 2 ||
           expected_number < 10);
    expected_number /= 2;
  }
}

#define PRNG_NBUCKETS 10000
#define PRNG_OPS 10000000
#define PRNG_NDEVS 6 // number of tolerable standard deviations

void test_prng(){
  Prng prng;
  int i;
  uint64_t r;
  int buckets[PRNG_NBUCKETS];
  double mean, stddev;
  double prob, n;

  int bitbuckets[64];
  int bit;

  prob = (double) 1 / PRNG_NBUCKETS;
  n = (double) PRNG_OPS;

  mean = n * prob; // expected number of items on each bucket
  stddev = sqrt(n * prob * (1-prob)); // stddev of # of items on each bucket

  memset(buckets, 0, sizeof(int) * PRNG_NBUCKETS);
  
  for (i=0; i < PRNG_OPS; ++i){
    r = prng.next();
    ++buckets[r % PRNG_NBUCKETS];
  }

  // check that number of items on each bucket falls within reasonable numbers
  for (i = 0; i < PRNG_NBUCKETS; ++i){
    //printf("Bucket %i: got %d expected %d\n", i, buckets[i], (int) mean);
    assert(mean - stddev * PRNG_NDEVS <= (double) buckets[i] &&
           (double) buckets[i] <= mean + stddev * PRNG_NDEVS);
  }

  // now check that each of all 64 bits are random
  memset(bitbuckets, 0, sizeof(int) * 64);
  for (i=0; i < PRNG_OPS; ++i){
    r = prng.next();
    for (bit = 0; bit < 64; ++bit){
      if (r & 1) ++bitbuckets[bit]; // if bit is set, increment bucket
      r >>= 1;
    }
  }

  prob = 0.5;
  mean = n * prob;
  stddev = sqrt(n * prob * (1-prob));
  for (bit = 0; bit < 64; ++bit){
    //printf("Bitbucket %i: got %d expected %d\n", bit, bitbuckets[bit],
    // (int) mean);
    assert(mean - stddev * PRNG_NDEVS <= (double) bitbuckets[bit] &&
           (double) bitbuckets[bit] <= mean + stddev * PRNG_NDEVS);
  }
}

#define LOCK_NTHREADS 10
#define LOCK_OPS 10000
RWLock lock_l;
int lock_counter1=1;
int lock_counter2=0;
u64 lock_counter3=0;
SimplePrng lock_n;

OSTHREAD_FUNC lock_thread(void *parm){
  //int i = (int)(long long) parm;
  int j;
  int r;
  for (j=0; j < LOCK_OPS; ++j){
    lock_l.lock();
    r = lock_n.next() % 2;
    lock_counter1 = lock_counter1 ^ r;
    lock_counter2 = lock_counter2 ^ r;
    lock_counter3 = lock_counter3 + 1;
    lock_l.unlock();
  }
  return 0;
}

void test_lock(){
  OSThread_t thr[LOCK_NTHREADS];
  int res;
  void *tres;

  for (int i=0; i < LOCK_NTHREADS; ++i){
    res = OSCreateThread(thr+i, lock_thread, (void*)(long long) i);
    assert(res==0);
  }
  for (int i=0; i < LOCK_NTHREADS; ++i){
    OSWaitThread(thr[i], &tres);
  }
  assert(lock_counter1 ^ lock_counter2 == 1);
  assert(lock_counter3 == LOCK_NTHREADS * LOCK_OPS);
}

void test1(){
  SkipListBK<Int,double> sl;
  int key;
  
  key = 5;
  sl.insert(new Int(key), (double)key);
  key = 10;
  sl.insert(new Int(key), (double)key);
  key = 15;
  sl.insert(new Int(key), (double)key);
  key = 20;
  sl.insert(new Int(key), (double)key);

  Int st, end;
  Int *ptr;
  st.val = 1; end.val = 3;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 1; end.val = 5;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 1; end.val = 7;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 5);

  st.val = 5; end.val = 5;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 5; end.val = 9;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 5);
  
  st.val = 7; end.val = 9;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 7; end.val = 10;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 7; end.val = 14;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 10);

  st.val = 7; end.val = 15;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 10);

  st.val = 7; end.val = 17;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 10);

  st.val = 15; end.val = 17;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 15);

  st.val = 15; end.val = 20;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 15);

  st.val = 15; end.val = 21;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 15);

  st.val = 17; end.val = 20;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 17; end.val = 21;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 20);

  st.val = 20; end.val = 20;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);

  st.val = 20; end.val = 22;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr && ptr->val == 20);

  st.val = 21; end.val = 22;
  ptr = sl.keyInInterval(&st, &end, 3);
  assert(ptr==0);
}

#define TEST2_NOPS 1000

void test2_copy_double(double d1, double &d2){ d2 = d1; }

void test2(){
  SimplePrng p;
  int i, j, v;
  int inrange = 0;
  int range1 = 5000;
  int range2 = 9000;
  int max = -1;

  for (j=0; j < TEST2_NOPS; ++j){
    SkipListBK<Int,double> sl;

    // insert elements
    for (i = 0; i < 5000; ++i){
      v = p.next() % 10000;
      if (max < v) max = v;
      if (range1 <= v && v <= range2) ++inrange; // count number to be deleted
      sl.insert(new Int(v), (double) v);
    }

    // copy list
    SkipListBK<Int,double> sl2(sl, test2_copy_double);

    // check that elements are monotonically increasing
    SkipListNodeBK<Int, double> *ptr;
    i = -1;
    for (ptr = sl2.getFirst(); ptr != sl2.getLast(); ptr = sl2.getNext(ptr)){
      assert(i <= ptr->key->val);
      i = ptr->key->val;
    }
  }
}

SkipList<Int,double> test3_sl;

void printList(int full=0){
  SkipListNode<Int,double> *ptr;
  int nitems=0;
  for (ptr = test3_sl.getFirst(); ptr != test3_sl.getLast();
       ptr = test3_sl.getNext(ptr)){
    if (!full) printf("%d[%d] ", ptr->key.val, ptr->nlevels);
    else printf("%d(%f) ", ptr->key.val, ptr->value);
    ++nitems;
  }
  printf("\nTotal %d items\n", nitems);
  putchar('\n');
}

#define TEST3_NOPS 500
#define TEST3_RANGE 10000
#define TEST3_NINSERT 500
#define TEST3_NDELETE 5000

void test3(){
  SimplePrng p;
  int i;
  Int v;
  double d1;
  double *ptr1, *ptr2;
  int inrange = 0;
  Int range1(5000);
  Int range2(9000);
  Int maxv(-1);
  int res1, res2;
  Int tmpint;

  for (int j=1; j < TEST3_NOPS; ++j){
    // insert elements
    for (i = 0; i < TEST3_NINSERT; ++i){
      v = Int(p.next() % TEST3_RANGE);
      if (maxv.val < v.val) maxv = v;
      if (range1.val <= v.val && v.val <= range2.val)
        ++inrange; // count number to be deleted
      test3_sl.insert(v, (double)v.val);
      //test3_sl.insert(&v, new double(v));
    }

    // copy skiplist
    SkipList<Int,double> sl2(test3_sl);

    Int todel;
    int n1, n2;
    // delete elements
    for (i = 0; i < TEST3_NDELETE; ++i){
      todel = Int(p.next() % TEST3_RANGE);
      n1 = 0;
      while (!test3_sl.lookupRemove(todel,0, d1)){
        assert((int)d1 == todel.val);
        ++n1;
      }
      n2 = sl2.delRange(todel, 1, todel, 1, 0, 0);
      assert(n1 == n2);
    }

    // check elements
    for (i=0; i < TEST3_RANGE; ++i){
      tmpint.val = i;
      res1 = test3_sl.lookup(tmpint, ptr1);
      res2 = sl2.lookup(tmpint, ptr2);
      assert (res1==res2);
      if (res1==0) assert(*ptr1==*ptr2);
    }
    SkipListNode<Int,double> *it1, *it2;
    it1 = test3_sl.getFirst();
    it2 = sl2.getFirst();
    while (it1 != test3_sl.getLast()){
      assert(it2 != test3_sl.getLast());
      assert(it1->key.val == it2->key.val);
      assert(it1->value == it2->value);
      it1 = test3_sl.getNext(it1);
      it2 = test3_sl.getNext(it2);
    }
    assert(it2 == sl2.getLast());

    // clear lists
    test3_sl.clear(0, 0);
    sl2.clear(0, 0);
  }
}

#define TEST4_NPHASES 20
#define TEST4_NINSERTS 5000
#define TEST4_RANGE 10000

void test4(){
  SimplePrng p;
  int i, v;
  Int tmpint;
  int inrange = 0;
  int range1 = 5000;
  int range2 = 9000;
  int max = -1;
  int nels = 0;
  double *valueptr;

  SkipList<Int,double> sl2(test3_sl);
  int lastsl = -1;

  for (int j=1; j <= TEST4_NPHASES; ++j){
    for (i = 0; i < TEST4_NINSERTS; ++i){
      v = p.next() % TEST4_RANGE;
      if (max < v) max = v;
      if (range1 <= v && v <= range2) ++inrange; // count number to be deleted
      tmpint.val = v;
      nels += test3_sl.insertOrReplace(tmpint, v, 0, 0);
      sl2.insert(tmpint, (double)v);
      //test3_sl.insert(&v, new double(v));
    }

    SkipListNode<Int,double> *ptr;
    assert(test3_sl.getNitems() == nels);
    assert(test3_sl.getNitems() <= TEST4_RANGE);
    assert(test3_sl.getNitems() <= sl2.getNitems());
    assert(lastsl <= test3_sl.getNitems());
    assert(sl2.getNitems() == j * TEST4_NINSERTS);
    lastsl = test3_sl.getNitems();

    Int prevkey = Int(-99);
    for (ptr = test3_sl.getFirst(); ptr != test3_sl.getLast();
         ptr = test3_sl.getNext(ptr)){
      assert(sl2.lookup(ptr->key,valueptr) == 0); // value is in the other table
      assert(ptr->key.val != prevkey.val); // value is unique
      prevkey = ptr->key;
    }

    for (ptr = sl2.getFirst(); ptr != sl2.getLast(); ptr = sl2.getNext(ptr)){
      assert(test3_sl.lookup(ptr->key,valueptr) == 0);
    }
  }
}

#define TEST6_NOPS 20000
OSTHREAD_FUNC test6_enqueuer(void *data){
  BoundedQueue<int> *queue = (BoundedQueue<int> *) data;
  int i;
  for (i=0; i < TEST6_NOPS; ++i){
    queue->enqueue(i);
  }
  return 0;
}

OSTHREAD_FUNC test6_dequeuer(void *data){
  BoundedQueue<int> *queue = (BoundedQueue<int> *) data;
  int i, item;
  for (i=0; i < TEST6_NOPS; ++i){
    item = queue->dequeue();
    assert(item==i);
  }
  return 0;
}

void test6(){
  BoundedQueue<int> queue(1);
  OSThread_t t1, t2;
  int res;
  void *tres;
  res = OSCreateThread(&t2, test6_enqueuer, (void*) &queue); assert(res==0);
  res = OSCreateThread(&t1, test6_dequeuer, (void*) &queue); assert(res==0);
  OSWaitThread(t1, &tres);
  OSWaitThread(t2, &tres);
}

#ifdef free
#undef free
#endif


#define TEST7_NOPS 10000
#define TEST7_NBUFS 50

void test7(){
  char *buf[TEST7_NBUFS];
  FixedAllocator allocator(10, 1, 1);
  int i, j;
  for (j=0; j < TEST7_NOPS; ++j){
    for (i=0; i < TEST7_NBUFS; ++i){
      buf[i] = (char*) allocator.alloc();
      strcpy(buf[i], "012345678");
      buf[i][0]=i;
    }

    // free every other buffer
    for (i=0; i < TEST7_NBUFS; i += 2){
      allocator.free(buf[i]);
    }

    // reallocate freed buffers
    for (i=0; i < TEST7_NBUFS; i += 2){
      buf[i] = (char*) allocator.alloc();
      strcpy(buf[i], "012345678");
      buf[i][0]=i;
    }

    // check that everything is in place
    for (i=0; i < TEST7_NBUFS; ++i){
      assert(buf[i][0]==i);
      assert(strcmp(buf[i]+1, "12345678") == 0);
    }

    // free everything
    for (i=0; i < TEST7_NBUFS; ++i){
      allocator.free(buf[i]);
    }
  }
}

OSTHREAD_FUNC test8_enqueuer(void *data){
  Channel<Int> *queue = (Channel<Int> *) data;
  int i, res;
  Int tmpint;
  for (i=0; i < 1000; ++i){
    do {
      tmpint.val = i;
      res = queue->enqueue(tmpint);
    } while (res != 0);
  }
  return 0;
}

OSTHREAD_FUNC test8_dequeuer(void *data){
  Channel<Int> *channel = (Channel<Int> *) data;
  int i, res;
  Int item;
  for (i=0; i < 1000; ++i){
    do {
      res = channel->dequeue(item);
    } while(res != 0);
    assert(item.val==i);
  }
  return 0;
}

void test8(){
  Channel<Int> channel;
  OSThread_t t1, t2;
  int res;
  void *tres;
  res = OSCreateThread(&t2, test8_enqueuer, (void*) &channel); assert(res==0);
  res = OSCreateThread(&t1, test8_dequeuer, (void*) &channel); assert(res==0);
  OSWaitThread(t1, &tres);
  OSWaitThread(t2, &tres);
}

#define TEST9_NOPS 10000
#define TEST9_NBUFS 50

// Checks if count bytes in ptr are equal to v.
// Returns 0 if so, non-0 otherwise
int memchk(char *ptr, char v, int count){
  while (count--)
    if (*ptr++ != v) return -1;
  return 0;
}

void test9(){
  char *buf[TEST9_NBUFS];
  VariableAllocatorNolock allocator;
  int i,j;

  for (j=0; j < TEST9_NOPS; ++j){
    for (i=0; i < TEST9_NBUFS; ++i){
      buf[i] = (char*) allocator.alloc(i);
      memset(buf[i], i, i);
    }

    // free every other buffer
    for (i=0; i < TEST9_NBUFS; i += 2){
      allocator.free(buf[i]);
    }

    // reallocate freed buffers
    for (i=0; i < TEST9_NBUFS; i += 2){
      buf[i] = (char*) allocator.alloc(i);
      memset(buf[i], i, i);
    }

    // check that everything is in place
    for (i=0; i < TEST9_NBUFS; ++i){
      assert(memchk(buf[i], i, i) == 0);
    }

    // free everything
    for (i=0; i < TEST9_NBUFS; ++i){
      allocator.free(buf[i]);
    }
  }
}

#define TEST10_NOPS 500000

int test10_threadno_inc;
int test10_threadno_test;
EventSync test10_event;

int test10_eventhandler(void *data){
  //int d = (int) (long long) data;
  //printf("h%d\n", d);
  return 0;
}

// increments 1 and sends back an event to task who sent it
// CData[0..7]: sender task
// Cdata[8..11]: integer to increment
struct PROGIncrementData {
  TaskInfo *sender;
  int i;
};
int test10_PROGIncrement(TaskInfo *ti){
  TaskMsgData msgdata;
  if (ti->getState()==0){
    ti->setState((void*)1);
    TaskEventScheduler::AddEvent(tgetThreadNo(), test10_eventhandler,
                                 (void*) 15, 1, 2000);
  }

  if (ti->getMessage(msgdata) == 0){
    PROGIncrementData *pid = (PROGIncrementData*) &msgdata;
    int j = pid->i+1;
    TaskMsg sendmsg;
    sendmsg.dest = pid->sender;
    sendmsg.flags = 0;
    memset((void*)&sendmsg.data, 0, sizeof(TaskMsgData));
    memcpy((void*)&sendmsg.data, (void*)&j, sizeof(int));
    tsendMessage(sendmsg);
    if (j == TEST10_NOPS){ // done
      tgetTaskScheduler()->exitThread();
      return SchedulerTaskStateEnding;
    }
  }
  return SchedulerTaskStateWaiting;
 }

int test10_PROGTest(TaskInfo *ti){
  PROGIncrementData pid;
  TaskMsg sendmsg;
  TaskMsgData receivedata;
  int i, res;
  memset((void*) &sendmsg.data, 0, sizeof(TaskMsgData));
  switch((int)(long long)ti->getState()){
  case 0: // initial state
    pid.sender = ti;
    pid.i = 25;
    memcpy((void*) &sendmsg.data, (void*) &pid, sizeof(PROGIncrementData));
    sendmsg.dest = TASKID_CREATE(test10_threadno_inc,1);
    sendmsg.flags = TMFLAG_FIXDEST;
    tsendMessage(sendmsg);
    ti->setState((void*)1);
    return SchedulerTaskStateWaiting;
  case 1:
    res = ti->getMessage(receivedata);
    if (res==0){
      i = *(int*)&receivedata;
      if (i == TEST10_NOPS){
        tgetTaskScheduler()->exitThread();
        return SchedulerTaskStateEnding; // done
      }
        
      pid.sender = ti;
      pid.i = i;
      memcpy((void*) &sendmsg.data, (void*) &pid, sizeof(PROGIncrementData));
      sendmsg.dest = TASKID_CREATE(test10_threadno_inc,1);
      sendmsg.flags = TMFLAG_FIXDEST;
      tsendMessage(sendmsg);
    }
    return SchedulerTaskStateWaiting;
  default:
    assert(0);
  }
  return SchedulerTaskStateEnding; // should never get to this point
}


OSTHREAD_FUNC test10_incThread(void *parm){
  TaskInfo *ti;
  TaskScheduler *ts = tgetTaskScheduler();
  ti = ts->createTask(test10_PROGIncrement, 0);
  ts->assignFixedTask(1, ti);
  test10_event.set();
  ts->run();
  return (void*)-1;
}

OSTHREAD_FUNC test10_testThread(void *parm){
  TaskInfo *ti;
  TaskScheduler *ts = tgetTaskScheduler();
  ti = ts->createTask(test10_PROGTest, 0); assert(ti);
  ts->run();
  return (void*)-1;
}

void test10(){
  tinitScheduler(0);
  test10_threadno_inc = SLauncher->createThread("inc", test10_incThread, 0, 0);
  test10_event.wait();
  test10_threadno_test = SLauncher->createThread("test", test10_testThread,
                                                 0, 0);
  SLauncher->wait();
  return;
}

void test11(){
  StackArray<int> sa(2, 2);
  int i;
  int j;
  for (i=0; i <= 1000; ++i)
    sa.push(i);
  for (i=1000; i >= 0; --i){
    j = sa.pop();
    assert(i==j);
  }
}

struct ListItem {
  int value;
  ListItem *next;
  ListItem(int v){ value=v; }
};

void test12(){
  SLinkList<ListItem> l;
  ListItem *item;
  int i;

  for (i=0; i < 1000; ++i){
    item = new ListItem(i);
    l.pushTail(item);
  }
  for (item = l.getFirst(), i=0; item != l.getLast();
       item = l.getNext(item), ++i){
    assert(item->value == i);
  }
  i = 0;
  while (!l.empty()){
    item = l.popHead();
    assert(i == item->value);
    delete item;
    ++i;
  }
}

int main(){
  printf("Test slist heights\n");
  test_slist_heights();
  printf("Test prgn\n");
  test_prng();
  printf("test_lock\n");
  test_lock();
  printf("test1\n");
  test1();
  printf("test2\n");
  test2();
  printf("test3\n");
  test3();
  printf("test4\n");
  test4();
  printf("test6\n");
  test6();
  printf("test7\n");
  test7();
  printf("test8\n");
  test8();
  printf("test9\n");
  test9();
  printf("test10\n");
  test10();
  printf("test11\n");
  test11();
  printf("test12\n");
  test12();
  return 0;
}
