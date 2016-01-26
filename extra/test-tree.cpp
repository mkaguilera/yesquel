//
// test-tree.cpp
//
// Tests for the distributed B-tree.
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

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <string.h>
#include <malloc.h>
#include <time.h>
#include <values.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "options.h"
#include "tmalloc.h"
#include "os.h"
#include "treedirect.h"
#include "prng.h"

DdConnection *conn;
const char *dbname = "TEST";
extern StorageConfig *SC;

struct COidQueueElement {
  COidQueueElement(){ coid.cid = coid.oid = 0; }
  COidQueueElement(COid c){ coid=c; }
  COid coid;
  i64 fencemin; // exclusive (node is not supposed to have this element)
  i64 fencemax; // inclusive (node could have this element)
  COidQueueElement *prev, *next;
};

void checkNodeFence(COid coid, SuperValue *sv, i64 fencemin, i64 fencemax){
  assert(sv->CellType == 0);  // checking for non-intkey is not supported
  
  for (int i=0; i < sv->Ncells; ++i){
    if (fencemin != LLONG_MIN && sv->Cells[i].nKey  <= fencemin || fencemax < sv->Cells[i].nKey){
      printf("Error %016llx:%016llx cell %lld outside range (%lld,%lld]\n",
             (long long)coid.cid, (long long)coid.oid, (long long) sv->Cells[i].nKey,
             (long long)fencemin, (long long)fencemax);
      assert(0);
    }
  }
}

// check that keys in node are monotonic
void checkNodeMonot(SuperValue *sv){
  assert(sv->CellType == 0);  // checking for non-intkey is not supported
  i64 prevkey = LLONG_MIN;
  
  for (int i=0; i < sv->Ncells; ++i){
    assert(prevkey <= sv->Cells[i].nKey);
    prevkey = sv->Cells[i].nKey;
  }
}

void checkLeaf(COid coid, SuperValue *sv, Set<I64> *allkeys){
  int i;
  int res;

  for (i=0; i < sv->Ncells; ++i){
    if (allkeys){
      res = allkeys->insert(sv->Cells[i].nKey);
      assert(res==0);
    }
    assert(sv->Cells[i].value == 0xabcdabcdabcdabcd); // unused value
  }
  assert(sv->Attrs[DTREENODE_ATTRIB_LASTPTR] == 0xabcdabcdabcdabcd ||
         sv->Attrs[DTREENODE_ATTRIB_LASTPTR] == 0);
}

void checkInner(COid coid, SuperValue *sv){
  int i;

  for (i=0; i < sv->Ncells; ++i){
    assert(sv->Cells[i].value != 0xabcdabcdabcdabcd);
  }
  assert(sv->Attrs[DTREENODE_ATTRIB_LASTPTR] != 0);
}

// check that following right pointers lead to siblings
// direction = 0 for left, 1 for right
// if strongcheck is true, do a full horizontal check
void checkHorizontal(Transaction *tx, COid coid, int direction, bool strongcheck){
  int res;
  Ptr<Valbuf> buf, buf2;
  COid coid2;
  SuperValue *sv, *sv2;
  int nextattr, prevattr;
  
  res = tx->vsuperget(coid, buf, 0, 0); assert(res==0);
  assert(buf->type!=0);
  sv = buf->u.raw;
  checkNodeMonot(sv);
  if (direction == 0){ // left
    nextattr = DTREENODE_ATTRIB_LEFTPTR;
    prevattr = DTREENODE_ATTRIB_RIGHTPTR;
  }
  else { // right
    nextattr = DTREENODE_ATTRIB_RIGHTPTR;
    prevattr = DTREENODE_ATTRIB_LEFTPTR;
  }
  
  // if right pointer is set
  if (sv->Attrs[nextattr]){
    // read it
    coid2.cid = coid.cid;
    coid2.oid = sv->Attrs[nextattr];
    res = tx->vsuperget(coid2, buf2, 0, 0); assert(res==0);
    assert(buf2->type!=0);
    sv2 = buf2->u.raw;

    checkNodeMonot(sv2);
    
    // check that it points back to us
    assert(sv2->Attrs[prevattr] == coid.oid);
    
    // check that level is the same
    assert(sv2->Attrs[DTREENODE_ATTRIB_HEIGHT] == sv->Attrs[DTREENODE_ATTRIB_HEIGHT]);
    
    // check that leaf status is the same
    assert((sv2->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF) ==
            (sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF));

    // check that int status is the same
    assert((sv2->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY) ==
            (sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_INTKEY));

    // check that largest key in first node is < smallest key in second
    if (sv->Ncells && sv2->Ncells){
      if (direction == 0){
        assert(sv->Cells[0].nKey > sv2->Cells[sv2->Ncells-1].nKey);
      } else {
        assert(sv->Cells[sv->Ncells-1].nKey < sv2->Cells[0].nKey);
      }
    }
    
    // recursively call on right pointer
    if (strongcheck) checkHorizontal(tx, coid2, direction, strongcheck);
  }
}

// if allkeys is set, stores all found keys there
// if strongcheck is true, do full horizontal traversals for every node (slow)
void checkCoid(COid startcoid, Set<I64> *allkeys, bool strongcheck){
  Transaction *tx;
  tx = new Transaction(SC);
  LinkList<COidQueueElement> coidqueue;
  Set<COid> pastcoids;
  COidQueueElement *el, *elchild;
  COid coid;
  COid child;
  int res;
  Ptr<Valbuf> buf;
  i64 fencemin, fencemax;

  SuperValue *sv;

  el = new COidQueueElement(startcoid);
  el->fencemin = LLONG_MIN;
  el->fencemax = LLONG_MAX;
  coidqueue.pushTail(el);

  while (!coidqueue.empty()){
    el = coidqueue.popHead();
    coid = el->coid;
    fencemin = el->fencemin;
    fencemax = el->fencemax;
    delete el;

    if (pastcoids.belongs(coid)){
      printf("COid %016llx:%016llx referenced more than once\n", (long long)coid.cid, (long long)coid.oid);
      assert(0);
    } else pastcoids.insert(coid);
    
    // read coid
    res = tx->vsuperget(coid, buf, 0, 0); assert(res==0);

    if (buf->type==0){
      printf("COid %llx:%llx not a supervalue\n", (long long)coid.cid, (long long)coid.oid);
      assert(0);
      continue;
    }

    sv = buf->u.raw;

    checkNodeFence(coid, sv, fencemin, fencemax);
    checkNodeMonot(sv);
    checkHorizontal(tx, coid, 0, strongcheck);
    checkHorizontal(tx, coid, 1, strongcheck);
    
    if (sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF){ // this is a leaf node
      checkLeaf(coid, sv, allkeys);
    } else {  // this is an inner node
      checkInner(coid, sv);
    }

    if (!(sv->Attrs[DTREENODE_ATTRIB_FLAGS] & DTREENODE_FLAG_LEAF)){
      // add children to queue
      child.cid = coid.cid;
      for (int i=0; i < sv->Ncells; ++i){
        child.oid = sv->Cells[i].value;
        elchild = new COidQueueElement(child);

        if (sv->CellType == 0){ // intkey
          if (i==0) elchild->fencemin = fencemin;
          else elchild->fencemin = sv->Cells[i-1].nKey;
          elchild->fencemax = sv->Cells[i].nKey;
        } else {
          // checking functionality is disabled for non-intkey trees, so just set fences to 0
          elchild->fencemin = elchild->fencemax = 0;
        }
        coidqueue.pushTail(elchild);
      }
      // now add the last pointer
      child.oid = sv->Attrs[DTREENODE_ATTRIB_LASTPTR];
      elchild = new COidQueueElement(child);
      if (sv->CellType == 0){ // intkey
        if (sv->Ncells >= 1) elchild->fencemin = sv->Cells[sv->Ncells-1].nKey;
        else elchild->fencemin = fencemin;
        elchild->fencemax = fencemax;
      } else {
        elchild->fencemin = elchild->fencemax = 0;
      }
      coidqueue.pushTail(elchild);
    }
  }
  delete tx;
}

// checks an entire tree.
// If keys is set, check that tree stores exactly those keys
// If strongcheck is done, do full horizontal traversals for every node (slow)
void checkTree(COid startcoid, Set<I64> *keys, bool strongcheck=true){
  Set<I64> allkeys;
  int i;
  
  if (!keys){
    checkCoid(startcoid, 0, strongcheck);
  } else {
    checkCoid(startcoid, &allkeys, strongcheck);
    if (allkeys.getNitems() != keys->getNitems()){
      printf("checkTree: got %d expected %d\n", allkeys.getNitems(), keys->getNitems());
    }
    assert(allkeys.getNitems() == keys->getNitems());
    SetNode<I64> *ptr1, *ptr2;
    ptr1 = keys->getFirst();
    ptr2 = allkeys.getFirst();
    for (i = allkeys.getNitems(); i > 0; --i){
      assert(ptr1 && ptr2);
      assert(I64::cmp(ptr1->key, ptr2->key)==0);
      ptr1 = keys->getNext(ptr1);
      ptr2 = allkeys.getNext(ptr2);
    }
  }
}

// test1: without concurrency, write random keys, read them, update them,
// and read them again to see the update
#define TEST1_NITEMS 10000
#define TEST1_OLDVAL "OLD"
#define TEST1_OLDLEN 4
#define TEST1_NEWVAL "NEWVA"
#define TEST1_NEWLEN 6

int test1cb(char *buf, int len, void *arg){
  memcpy(buf, TEST1_NEWVAL, TEST1_NEWLEN);
  return TEST1_NEWLEN;
}

void test1(){
  SimplePrng prng;
  int i;
  i64 key;
  u64 itable;
  DdTable *table;
  int len;
  bool done;
  char buf[256];
  int res;
  Set<I64> allkeys;
  COid coid;

  itable = 1;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }
 
  prng.SetSeed(1);
  for (i=0; i < TEST1_NITEMS; ++i){
    key = prng.next32();
    res = allkeys.insert(key); assert(res==0);
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdInsert(table, key, TEST1_OLDVAL, TEST1_OLDLEN); assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }

  coid.cid = getCidTable(nameToDbid(dbname, false), itable);
  coid.oid = 0;
  checkTree(coid, &allkeys, false);

  prng.SetSeed(1);
  res = DdStartTx(conn); assert(res==0);
  for (i=0; i < TEST1_NITEMS; ++i){
    key = prng.next32();
    res = DdLookup(table, key, buf, sizeof(buf), &len); assert(res==0);
    assert(len==TEST1_OLDLEN);
    assert(memcmp(buf, TEST1_OLDVAL, TEST1_OLDLEN) == 0);
  }
  res = DdCommitTx(conn); assert(res==0);

  prng.SetSeed(1);
  for (i=0; i < TEST1_NITEMS; ++i){
    key = prng.next32();
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdUpdate(table, key, buf, sizeof(buf), test1cb, 0); assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }

  checkTree(coid, &allkeys, false);

  prng.SetSeed(1);
  res = DdStartTx(conn); assert(res==0);
  for (i=0; i < TEST1_NITEMS; ++i){
    key = prng.next32();
    res = DdLookup(table, key, buf, sizeof(buf), &len); assert(res==0);
    assert(len==TEST1_NEWLEN);
    assert(memcmp(buf, TEST1_NEWVAL, TEST1_NEWLEN) == 0);
  }
  res = DdCommitTx(conn); assert(res==0);

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test1(){
  pid_t pid;
  int status;
  pid = fork();
  if (!pid){ // child
    test1();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }
}

// test2: without concurrency, write random keys, read them, delete them,
// and read them again to see the update

#define TEST2_NITEMS 10000
#define TEST2_VAL "OLD"
#define TEST2_LEN 4

void test2(){
  SimplePrng prng;
  int i;
  i64 key;
  u64 itable;
  DdTable *table;
  int len;
  bool done;
  char buf[256];
  int res;
  Set<I64> allkeys;
  COid coid;

  itable = 2;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }
 
  prng.SetSeed(1);
  for (i=0; i < TEST2_NITEMS; ++i){
    key = prng.next32();
    res = allkeys.insert(key); assert(res==0);
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdInsert(table, key, TEST2_VAL, TEST2_LEN); assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }

  coid.cid = getCidTable(nameToDbid(dbname, false), itable);
  coid.oid = 0;
  checkTree(coid, &allkeys, false);
  
  prng.SetSeed(1);
  res = DdStartTx(conn); assert(res==0);
  for (i=0; i < TEST2_NITEMS; ++i){
    key = prng.next32();
    res = DdLookup(table, key, buf, sizeof(buf), &len); assert(res==0);
    assert(len==TEST2_LEN);
    assert(memcmp(buf, TEST2_VAL, TEST2_LEN) == 0);
  }
  res = DdCommitTx(conn); assert(res==0);

  prng.SetSeed(1);
  for (i=0; i < TEST2_NITEMS; ++i){
    key = prng.next32();
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdDelete(table, key); assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }

  Set<I64> nokeys;
  checkTree(coid, &nokeys, false);

  prng.SetSeed(1);
  res = DdStartTx(conn); assert(res==0);
  for (i=0; i < TEST2_NITEMS; ++i){
    key = prng.next32();
    res = DdLookup(table, key, buf, sizeof(buf), &len); assert(res!=0 || len==0);
  }
  res = DdCommitTx(conn); assert(res==0);

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test2(){
  pid_t pid;
  int status;
  pid = fork();
  if (!pid){ // child
    test1();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }
}

// test3: many processes concurrently insert
// Afterwards, one process checks the tree structure

#define TEST3_NPROCS 16
#define TEST3_NEPOCHS 10
#define TEST3_NITEMS 128
#define TEST3_VAL "ABC"
#define TEST3_LEN 4

void test3_pre(){
  u64 itable;
  DdTable *table;
  int res;

  itable = 3;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }
  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test3(int procno){
  int i, j;
  i64 key;
  u64 itable;
  DdTable *table;
  bool done;
  int res;

  itable = 3;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error opening table %llx: %d\n",
                    (long long)itable, res); exit(1); }
 
  for (i=0; i < TEST3_NEPOCHS; ++i){
    for (j=0; j < TEST3_NITEMS; ++j){
      key = ((u64)i<<48) | ((u64)j<<32) | procno;
      do {
        res = DdStartTx(conn); assert(res==0);
        res = DdInsert(table, key, TEST3_VAL, TEST3_LEN);
        if (res != 0){ printf("DdInsert returns %d\n", res); }
        assert(res==0);
        res = DdCommitTx(conn);
        done = res == 0;
      } while (!done);
    }
  }

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test3_post(){
  int i, j;
  i64 key;
  u64 itable;
  int res;
  int procno;
  Set<I64> allkeys;
  COid coid;

  itable = 3;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  for (i=0; i < TEST3_NEPOCHS; ++i){
    for (j=0; j < TEST3_NITEMS; ++j){
      for (procno = 0; procno < TEST3_NPROCS; ++procno){
        key = ((u64)i<<48) | ((u64)j<<32) | procno;
        allkeys.insert(key);
      }
    }
  }

  coid.cid = getCidTable(nameToDbid(dbname, false), itable);
  coid.oid = 0;
  checkTree(coid, &allkeys, false);
  
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test3(){
  pid_t pid;
  pid_t *pid_list;
  int status;
  int i;
  
  pid_list = new pid_t[TEST3_NPROCS];

  pid = fork();
  if (!pid){ // child
    test3_pre();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }

  for (i=0; i < TEST3_NPROCS; ++i){
    pid_list[i] = fork();
    if (!pid_list[i]){ // child
      test3(i);
      exit(0);
    }
  }
  for (i=0; i < TEST3_NPROCS; ++i){
    waitpid(pid_list[i], &status, 0);
  }
  delete [] pid_list;

  pid = fork();
  if (!pid){ // child
    test3_post();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }
}

// test4: many processes run, each process adds a bunch of its own keys then deletes it.
// Processes repeat that for several epochs. In last epoch, process adds keys without deleting
// Afterwards, one process checks the tree structure to find all keys

#define TEST4_NPROCS 16
#define TEST4_NEPOCHS 32
#define TEST4_NITEMS 16
#define TEST4_VAL "ABC"
#define TEST4_LEN 4

void test4_pre(){
  u64 itable;
  DdTable *table;
  int res;

  itable = 4;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }
  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test4(int procno){
  int i, j;
  i64 key;
  u64 itable;
  DdTable *table;
  bool done;
  int res;

  itable = 4;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error opening table %llx: %d\n",
                    (long long)itable, res); exit(1); }
 
  for (i=0; i < TEST4_NEPOCHS; ++i){
    for (j=0; j < TEST4_NITEMS; ++j){
      key = ((u64)i<<48) | ((u64)j<<32) | procno;
      do {
        res = DdStartTx(conn); assert(res==0);
        res = DdInsert(table, key, TEST4_VAL, TEST4_LEN);
        if (res != 0){ printf("DdInsert returns %d\n", res); }
        assert(res==0);
        res = DdCommitTx(conn);
        done = res == 0;
      } while (!done);
    }
    if (i == TEST4_NEPOCHS-1) continue; // do not delete the last time
    for (j=0; j < TEST4_NITEMS; ++j){
      key = ((u64)i<<48) | ((u64)j<<32) | procno;
      do {
        res = DdStartTx(conn); assert(res==0);
        res = DdDelete(table, key);
        if (res) DdRollbackTx(conn);
        else res = DdCommitTx(conn);
        done = res == 0;
      } while (!done);
      
    }
  }

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test4_post(){
  int i, j;
  i64 key;
  u64 itable;
  int res;
  int procno;
  Set<I64> allkeys;
  COid coid;

  itable = 4;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  i = TEST4_NEPOCHS-1;
  for (j=0; j < TEST4_NITEMS; ++j){
    for (procno = 0; procno < TEST4_NPROCS; ++procno){
      key = ((u64)i<<48) | ((u64)j<<32) | procno;
      allkeys.insert(key);
    }
  }

  coid.cid = getCidTable(nameToDbid(dbname, false), itable);
  coid.oid = 0;
  checkTree(coid, &allkeys);
  
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test4(){
  pid_t pid;
  pid_t *pid_list;
  int status;
  int i;
  
  pid_list = new pid_t[TEST4_NPROCS];

  pid = fork();
  if (!pid){ // child
    test4_pre();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }

  for (i=0; i < TEST4_NPROCS; ++i){
    pid_list[i] = fork();
    if (!pid_list[i]){ // child
      test4(i);
      exit(0);
    }
  }
  for (i=0; i < TEST4_NPROCS; ++i){
    waitpid(pid_list[i], &status, 0);
  }
  delete [] pid_list;

  pid = fork();
  if (!pid){ // child
    test4_post();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }
}


// test5: many processes run. Initially, one process inserts keys 0, 1000, 2000, ...
// Then, all processes run concurrently inserting random keys.
// At the end, a process checks the integrity of the tree.

#define TEST5_NPROCS 16
#define TEST5_NITEMS 100
#define TEST5_NOPS 655
#define TEST5_VAL "ABC"
#define TEST5_LEN 4

void test5_pre(){
  int i;
  u64 itable;
  DdTable *table;
  int res;
  i64 key;
  bool done;

  itable = 5;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }

  // insert initial keys 0, 1000, 2000, ...
  for (i=0; i < TEST5_NITEMS; ++i){
    key = i * 1000;
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdInsert(table, key, TEST5_VAL, TEST5_LEN);
      if (res != 0){ printf("DdInsert returns %d\n", res); }
      assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }
  
  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test5(int procno){
  SimplePrng prng;
  int i;
  i64 key;
  u64 itable;
  DdTable *table;
  bool done;
  int res;

  prng.SetSeed(procno);
  itable = 5;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error opening table %llx: %d\n",
                    (long long)itable, res); exit(1); }
 
  for (i=0; i < TEST5_NOPS; ++i){
    key = prng.next32() % (TEST5_NITEMS * 1000);
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdInsert(table, key, TEST5_VAL, TEST5_LEN);
      if (res != 0){ printf("DdInsert returns %d\n", res); }
      assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test5_post(){
  u64 itable;
  int res;
  Set<I64> allkeys;
  COid coid;

  itable = 5;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  coid.cid = getCidTable(nameToDbid(dbname, false), itable);
  coid.oid = 0;
  checkTree(coid, 0, false);
  
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test5(){
  pid_t pid;
  pid_t *pid_list;
  int status;
  int i;
  
  pid_list = new pid_t[TEST5_NPROCS];

  pid = fork();
  if (!pid){ // child
    test5_pre();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }

  for (i=0; i < TEST5_NPROCS; ++i){
    pid_list[i] = fork();
    if (!pid_list[i]){ // child
      test5(i);
      exit(0);
    }
  }
  for (i=0; i < TEST5_NPROCS; ++i){
    waitpid(pid_list[i], &status, 0);
  }
  delete [] pid_list;

  printf("  Checking integrity\n");

  pid = fork();
  if (!pid){ // child
    test5_post();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }
}


// ---------------


// test6: many processes run. Initially, one process inserts keys [0|0]...[NITEMS-1|0]
// Then, many processes run concurrently inserting or deleting random keys not ending in 0.
// Meanwhile, another process checks that the keys ending in 0 remain.

#define TEST6_NPROCS 16
#define TEST6_NITEMS 100
#define TEST6_NOPS 655
#define TEST6_VAL "ABC"
#define TEST6_LEN 4

void test6_pre(){
  int i;
  u64 itable;
  DdTable *table;
  int res;
  i64 key;
  bool done;

  itable = 6;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }

  // insert initial keys 0, 1000, 2000, ...
  for (i=0; i < TEST6_NITEMS; ++i){
    key = (i64) i << 32;
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdInsert(table, key, TEST6_VAL, TEST6_LEN);
      if (res != 0){ printf("DdInsert returns %d\n", res); }
      assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }
  
  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

i64 test6_checker_curr; // current key being checked
void test6_checker_callback(i64 key, const char *data, int len, int n, bool eof,
                            void *callbackparm){
  if (!eof){
    if ((key & 0xffffffffLL) == 0){
      key = key >> 32;
      if (key != test6_checker_curr){
        printf("key %lld wanted %lld\n", (long long) key, (long long) test6_checker_curr);
      }
      assert(key == test6_checker_curr);
      ++test6_checker_curr;
    }
  } else {
    assert(test6_checker_curr == TEST6_NITEMS);
  }
}

void test6(int procno){
  SimplePrng prng;
  int i, j;
  i64 key;
  u64 itable;
  DdTable *table;
  bool done;
  int res;
  Set<I64> existing;
  i32 nonzero, op;

  prng.SetSeed(procno);
  itable = 6;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error opening table %llx: %d\n",
                    (long long)itable, res); exit(1); }

  if (procno != 0){ // processes other than zero add or remove random keys
    for (i=0; i < TEST6_NOPS; ++i){
      op = prng.next() % 2;
      if (op){ // add key
        do {
          do {
            nonzero = prng.next32();
          } while (nonzero == 0);
          key = ((i64) prng.next32() << 32) | (i64) nonzero;
          res = existing.insert(key);
        } while (res); // repeat if key existed before

        do {
          res = DdStartTx(conn); assert(res==0);
          res = DdInsert(table, key, TEST6_VAL, TEST6_LEN);
          if (res != 0){ printf("DdInsert returns %d\n", res); }
          assert(res==0);
          res = DdCommitTx(conn);
          done = res == 0;
        } while (!done);
      } else { // remove key
        SetNode<I64> *ptr;
        // pick random existing keys
        j = existing.getNitems();
        key = prng.next() % j; // pick a random number from 0 to j-1
        for (ptr = existing.getFirst(), j = 0;
             j < key;
             ++j, ptr = existing.getNext(ptr)){
        }
        key = ptr->key.data;
        res = existing.remove(key);
        do {
          res = DdStartTx(conn); assert(res==0);
          res = DdDelete(table, key);
          if (res != 0){ printf("DdInsert returns %d\n", res); }
          assert(res==0);
          res = DdCommitTx(conn);
          done = res == 0;
        } while (!done);
      }
    }
  } else {  // process 0 is the checker, which checks for presence of keys ending in 0
    for (i=0; i < TEST6_NOPS; ++i){
      test6_checker_curr = 0;
      res = DdStartTx(conn); assert(res==0);
      res = DdScan(table, -1, 0x7fffffff, test6_checker_callback, 0);
      res = DdCommitTx(conn);
      assert(res==0);
    }
  }

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test6(){
  pid_t pid;
  pid_t *pid_list;
  int status;
  int i;
  
  pid_list = new pid_t[TEST6_NPROCS];

  pid = fork();
  if (!pid){ // child
    test6_pre();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }

  for (i=0; i < TEST6_NPROCS; ++i){
    pid_list[i] = fork();
    if (!pid_list[i]){ // child
      test6(i);
      exit(0);
    }
  }
  for (i=0; i < TEST6_NPROCS; ++i){
    waitpid(pid_list[i], &status, 0);
  }
  delete [] pid_list;
}

// test7 (load split): many processes run. Initially, one process inserts
// keys 0..NITEMS-1. Then, all processes run concurrently scanning for a single
// random key in interval FOCUS_START..FOCUS_START+FOCUS_NITEMS-1.
// At the end, a process checks that those keys
// are placed in different tree nodes.

#define TEST7_NPROCS 16
#define TEST7_NITEMS 5000
#define TEST7_FOCUS_START 1000
#define TEST7_FOCUS_NITEMS 10
#define TEST7_NOPS 20000
#define TEST7_VAL "ABC"
#define TEST7_LEN 4
#define TEST7_TRANSITION_PERCENTAGE 50 // lower bound on % of oid transactions
                                       // that should occur when reading focus keys

void test7_pre(){
  int i;
  u64 itable;
  DdTable *table;
  int res;
  i64 key;
  bool done;

  itable = 7;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdCreateTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error creating table %llx: %d\n",
                    (long long)itable, res); exit(1); }

  // insert initial keys 0, 1000, 2000, ...
  for (i=0; i < TEST7_NITEMS; ++i){
    key = i;
    do {
      res = DdStartTx(conn); assert(res==0);
      res = DdInsert(table, key, TEST7_VAL, TEST7_LEN);
      if (res != 0){ printf("DdInsert returns %d\n", res); }
      assert(res==0);
      res = DdCommitTx(conn);
      done = res == 0;
    } while (!done);
  }
  
  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test7(int procno){
  SimplePrng prng;
  int i;
  i64 key;
  u64 itable;
  DdTable *table;
  int res;

  prng.SetSeed(procno);
  itable = 7;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error opening table %llx: %d\n",
                    (long long)itable, res); exit(1); }
 
  for (i=0; i < TEST7_NOPS; ++i){
    key = TEST7_FOCUS_START + prng.next32() % TEST7_FOCUS_NITEMS;
    res = DdStartTx(conn); assert(res==0);    
    res = DdScan(table, key, 1, 0, 0, false);
    if (res != 0){ printf("DdScan returns %d\n", res); }
    assert(res==0);
    res = DdRollbackTx(conn); assert(res==0);
  }

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();
}

void test7_post(){
  u64 itable;
  DdTable *table;
  int res, i;
  i64 key;
  Oid lastoid, thisoid;
  int transitions;

  itable = 7;
  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }
  
  res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error opening table %llx: %d\n",
                    (long long)itable, res); exit(1); }

  // here check that keys are in different tree nodes
  lastoid = 0;
  res = DdStartTx(conn); assert(res==0);
  transitions = 0;
  
  for (i=0; i < TEST7_FOCUS_NITEMS; ++i){
    key = TEST7_FOCUS_START + i;
    thisoid = DdGetOid(table, key); assert(thisoid);
    if (thisoid != lastoid) ++transitions;
    lastoid = thisoid;
  }
  res = DdRollbackTx(conn); assert(res==0);

  assert(transitions >= TEST7_FOCUS_NITEMS * TEST7_TRANSITION_PERCENTAGE / 100);
  
  DdCloseConnection(conn);
  DdUninit();
}

void launch_test7(){
  pid_t pid;
  pid_t *pid_list;
  int status;
  int i;
  
  pid_list = new pid_t[TEST7_NPROCS];

  pid = fork();
  if (!pid){ // child
    test7_pre();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }

  for (i=0; i < TEST7_NPROCS; ++i){
    pid_list[i] = fork();
    if (!pid_list[i]){ // child
      test7(i);
      exit(0);
    }
  }
  for (i=0; i < TEST7_NPROCS; ++i){
    waitpid(pid_list[i], &status, 0);
  }
  delete [] pid_list;

  printf("  Checking integrity\n");

  pid = fork();
  if (!pid){ // child
    test7_post();
    exit(0);
  } else { // park
    waitpid(pid, &status, 0);
  }
}


int main(){
  printf("Test1\n");
  launch_test1();
  printf("Test2\n");
  launch_test2();
  printf("Test3\n");
  launch_test3();
  printf("Test4\n");
  launch_test4();
  printf("Test5\n");
  launch_test5();
  printf("Test6\n");
  launch_test6();
  printf("Test7\n");
#ifdef DTREE_LOADSPLITS  
  launch_test7();
#else
  printf("  Skipped (nodesplits disabled)\n");
  printf("Done\n");
#endif  
  
  exit(0);
}  
