//
// memkv-simple.h
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

#ifndef _MEMKV_SIMPLE
#define _MEMKV_SIMPLE

#include <string.h>
#include "options.h"
#include "gaiatypes.h"
#include "util.h"
#include "kvinterface.h"

struct StoredItem {
  COid coid;
  int len;
  unsigned char* data;
  StoredItem(){ len=0; data=0; }
  // HashTable stuff
  StoredItem *prev, *next, *sprev, *snext;
  COid *GetKeyPtr(){ return &coid; }
  static unsigned HashKey(COid *coid){
    return *((u32*)coid) ^ *((u32*)coid+1) ^
      *((u32*)coid+2) ^ *((u32*)coid+3);
  }
  static int CompareKey(COid *c1, COid *c2){
    return memcmp((void*)c1, (void*)c2, sizeof(COid));
  }
};

extern HashTableBK<COid,StoredItem> *MemKVStore;
extern RWLock MemKVStore_l;

int memKVget(KVTransaction *tx, COid coid, char **buf, int *len);

int memKVgetPad(KVTransaction *tx, COid coid, char **buf, int *len, int pad);
int memKVput(KVTransaction *tx, COid &coid, char *data, int len);
int memKVput2(KVTransaction* tx, COid &coid,  char *data1, int len1,
              char *data2, int len2);
int memKVput3(KVTransaction *tx, COid &coid,  char* data1, int len1,
              char *data2, int len2, char *data3, int len3);
int membeginTx(KVTransaction **tx);
int memcommitTx(KVTransaction* tx);
int memabortTx(KVTransaction* tx);
void memKVfreeall();

#endif
