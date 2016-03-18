//
// memkv-simple.cpp
//
// This is the key-value interface to the local key-value storage
// system (see clientlib-local.h).
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


#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <stdarg.h>

#include "options.h"
#include "memkv-simple.h"

HashTableBK<COid,StoredItem> *MemKVStore=0;
RWLock MemKVStore_l;

int memKVget(KVTransaction *tx, COid coid, char **buf, int *len){
  StoredItem *ptr;
  MemKVStore_l.lock();
  ptr = MemKVStore->Lookup(&coid);
  if (ptr){ // found
    *len = ptr->len;
    *buf = (char*) malloc(ptr->len); assert(*buf);
    memcpy(*buf, ptr->data, *len);
  } else {
    *len = 0;
    *buf = (char*) malloc(0); assert(*buf);
  }
  MemKVStore_l.unlock();
  return 0;
}

int memKVgetPad(KVTransaction *tx, COid coid, char **buf, int *len, int pad){
  StoredItem *ptr;
  MemKVStore_l.lock();
  ptr = MemKVStore->Lookup(&coid);
  if (ptr){ // found
    *len = ptr->len;
    *buf = (char*) malloc(ptr->len+pad); assert(*buf);
    memcpy(*buf, ptr->data, *len);
  } else {
    *len = 0;
    *buf = (char*) malloc(pad); assert(*buf);
  }
  MemKVStore_l.unlock();
  return 0;
}

inline int auxmemKVremove(KVTransaction *tx, COid &coid){
  StoredItem *ptr;
  MemKVStore_l.lock();
  ptr = MemKVStore->Lookup(&coid);
  if (ptr){ // found element
    MemKVStore->Remove(ptr);
    free(ptr->data);
    delete ptr;
  }
  MemKVStore_l.unlock();
  return 0;
}

// auxilliary function. Assumes len != 0 and data has been allocated for us.
inline int auxmemKVput(KVTransaction *tx, COid &coid, char *newdata, int len) {
  StoredItem *ptr;
  MemKVStore_l.lock();
  ptr = MemKVStore->Lookup(&coid);
  if (ptr){ // found element
    free(ptr->data);  // replace data and length
    ptr->data = (unsigned char*) newdata;
    ptr->len = len;
  } else { // not found
    ptr = new StoredItem; // create new entry
    ptr->coid = coid;
    ptr->data = (unsigned char*) newdata;
    ptr->len = len;
    MemKVStore->Insert(ptr);
  }
  MemKVStore_l.unlock();
  return 0;
}

int memKVput(KVTransaction *tx, COid &coid, char *data, int len) {
  if (len == 0) return auxmemKVremove(tx, coid);

  // copy data to new buffer
  char *newdata = (char*) malloc(len);
  memcpy(newdata, data, len);
  auxmemKVput(tx, coid, newdata, len);
  return 0;
}

int memKVput2(KVTransaction* tx, COid &coid,  char *data1, int len1,
              char *data2, int len2){
  int len;
  len = len1+len2;
  if (len == 0) return auxmemKVremove(tx, coid);

  // copy data to new buffer
  char *newdata = (char*) malloc(len);
  memcpy(newdata, data1, len1);
  memcpy(newdata+len1, data2, len2);
  auxmemKVput(tx, coid, newdata, len);
  return 0;
}

int memKVput3(KVTransaction *tx, COid &coid,  char* data1, int len1,
              char *data2, int len2, char *data3, int len3){
  int len;

  len = len1+len2+len3;
  if (len == 0) return auxmemKVremove(tx, coid);

  // copy data to new buffer
  char *newdata = (char*) malloc(len);
  memcpy(newdata, data1, len1);
  memcpy(newdata+len1, data2, len2);
  memcpy(newdata+len1+len2, data3, len3);
  auxmemKVput(tx, coid, newdata, len);
  return 0;
}

int membeginTx(KVTransaction **tx) {
  return 0;
}

int memcommitTx(KVTransaction* tx) {
  return 0;
}

int memabortTx(KVTransaction* tx) {
  return 0;
}

void memKVfreeall() {
  StoredItem *it, *itnext;
  for (it = MemKVStore->GetFirst(); it != MemKVStore->GetLast(); it = itnext){
    itnext = MemKVStore->GetNext(it);
    MemKVStore->Remove(it);
    free(it->data);
    delete it;
  }
  return;
}

class memKVinit {
  public:
  memKVinit() {
    MemKVStore = new HashTableBK<COid,StoredItem>(MEMKVSTORE_HASHTABLE_SIZE);
  }
  ~memKVinit() {
    memKVfreeall(); // This exists to shut up valgrind
  }
};
const memKVinit memKVinit_dummy;
