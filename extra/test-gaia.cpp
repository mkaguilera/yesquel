//
// test-gaia.cpp
//
// Various tests of the storage server.
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
#include <malloc.h>
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "prng.h"
#include "util.h"
#include "tmalloc.h"
#include "os.h"
#include "debug.h"
#include "options.h"
#include "storageserverstate.h"
#include "coid.h"

#include <set>
#include <map>

#ifndef LOCAL_TRANSACTION  
#include "clientlib.h"
#define TRANSACTION_VAR(x) Transaction x(SC)
#else
#include "clientlib-local.h"
#define TRANSACTION_VAR(x) LocalTransaction x
#endif

#ifndef LOCAL_TRANSACTION  
static const char *CONFIGFILENAME="config.txt";
StorageConfig *SC = 0;
#endif

Ptr<RcKeyInfo> KI = 0;

CollSeq TheCS = { "BINARY", 1, 1, 0, binCollFunc, 0 };

Ptr<RcKeyInfo> createki(void){
  Ptr<RcKeyInfo> ret = new(1,1) RcKeyInfo;
  ret->db = 0;
  ret->enc = 1; // utf8 encoding
  ret->nField = 1; // 1 entry in acoll
  ret->aSortOrder = (u8*) new char[1];
  *ret->aSortOrder = 0; // ascending order
  ret->aColl[0] = &TheCS;
  return ret;
}

// debugging function, not part of test
void printpkey(int nkey, char *pkey){
  if (!pkey) putchar('_');
  else printf("%c%c%c%c%c%c", pkey[0], pkey[1], pkey[2], pkey[3],
              pkey[4], pkey[5]);
}

// this version is taylored for the specific KeyInfo we define below
void printpkey2(int nkey, char *pkey){
  char str[256];
  if (!pkey) putchar('_');
  else {
    memcpy(str, pkey+2, nkey-2);
    str[nkey-2] = 0;
    printf("\"%s\"", str);
  }
}

// debugging function, not part of test
void printSV(SuperValue *sv){
  int i;
  printf("nattrs %d celltype %d ncells %d attrs [",
    sv->Nattrs, sv->CellType, sv->Ncells);
  for (i=0; i < sv->Nattrs; ++i){
    printf("%llx", (long long)sv->Attrs[i]);
    if (i < sv->Nattrs-1) putchar(' ');
  }
  printf("]\nCells=");
  for (i=0; i < sv->Ncells; ++i){
    if (sv->CellType==0) printf("%lld[%llx] ", (long long)sv->Cells[i].nKey,
                                (long long)sv->Cells[i].value);
    else {
      printf("%lld[", (long long)sv->Cells[i].nKey);
      printpkey(sv->Cells[i].nKey, sv->Cells[i].pKey);
      printf("][%lld] ", (long long)sv->Cells[i].value);
    }
  }
  putchar('\n');
}

// aux functions for attributes ------------------------------------------------
#define NATTRS 6
void SetAttrs(SuperValue *svp){
  svp->Nattrs = NATTRS;
  svp->Attrs = new u64[NATTRS];
  for (int i=0; i < NATTRS; ++i){
    svp->Attrs[i] = i + 1000;
  }
}

void CheckAttrs(SuperValue *svp){
  assert((int)svp->Nattrs == NATTRS);
  for (int i=0; i < (int) NATTRS; ++i)
    assert((int)svp->Attrs[i] == i + 1000);
}

// aux functions for integer cells ---------------------------------------------

ListCell SetIntCell(int v){
  ListCell r;
  r.nKey = v;
  r.pKey = 0;
  r.value = v%2;
  return r;
}

void CheckIntCell(ListCell *tochk, int i){
  assert(tochk->nKey == i);
  assert(tochk->pKey == 0);
  assert((int)tochk->value == i%2);
}

void SetIntCells(SuperValue *svp, int n){
  svp->CellType = 0; // int
  svp->Ncells = n;
  svp->Cells = new ListCell[n];
  ListCell *cellptr = svp->Cells;
  for (int i=0; i < n; ++i){
    cellptr[i] = SetIntCell(i);
  }
}

int GetIntCell(ListCell *cell){
  assert(cell->pKey == 0);
  return (int) cell->nKey;
}

void CheckIntCells(SuperValue *svp, int n){
  ListCell *cells;
  assert(svp->CellType == 0);
  assert(svp->Ncells == n);
  cells = svp->Cells;
  for (int i=0; i < n; ++i){
    CheckIntCell(cells+i, i);
  }
}

void CheckIntCellsList(SuperValue *svp, int *list, int n){
  ListCell *cells;
  assert(svp->CellType == 0);
  assert(svp->Ncells == n);
  cells = svp->Cells;
  for (int i=0; i < n; ++i){
    CheckIntCell(cells+i, list[i]);
  }
}

// aux functions for str cells -------------------------------------------------

ListCell GetStrCell(char *str){
  ListCell r;
  int slen = strlen(str);
  int serial_type = 2*slen+13;
  assert(serial_type < 128);
  r.nKey = 2+slen;
  r.pKey = new char[2+slen];
  r.pKey[0] = 2;  // size of header
  r.pKey[1] = serial_type; 
  memcpy(r.pKey+2, str, slen);
  r.value = 8;
  return r;
}

ListCell GetStrCell(int v){
  char str[256];
  sprintf(str, "%03d", v);
  return GetStrCell(str);
}

void CheckStrCell(ListCell *lc, char *str){
  int slen = strlen(str);
  int serial_type = 2*slen+13;
  assert(serial_type < 128);
  assert(lc->nKey == 2+slen);
  assert(lc->pKey[0] == 2);
  assert(lc->pKey[1] == serial_type);
  assert(memcmp(&lc->pKey[2], str, slen) == 0);
  assert(lc->value == 8);
}

void CheckStrCell(ListCell *lc, int v){
  char str[256];
  sprintf(str, "%03d", v);
  CheckStrCell(lc, str);
}

void SetStrCells(SuperValue *svp, int n){
  svp->CellType = 1; // non-int
  svp->Ncells = n;
  svp->Cells = new ListCell[n];
  ListCell *cellptr = svp->Cells;
  for (int i=0; i < n; ++i){
    cellptr[i] = GetStrCell(i);
  }
  svp->prki = KI;
}

void CheckStrCells(SuperValue *svp, int n){
  int i;
  ListCell *cells;
  assert(svp->CellType == 1);
  assert(svp->Ncells == n);
  cells = svp->Cells;
  for (i=0; i < n; ++i){
    CheckStrCell(cells+i, i);
  }
}

void CheckStrCellsList(SuperValue *svp, char **list, int n){
  int i;
  ListCell *cells;
  assert(svp->CellType == 1);
  assert(svp->Ncells == n);
  cells = svp->Cells;
  for (i=0; i < n; ++i){
    CheckStrCell(cells+i, list[i]);
  }
}

// test1: simple test to put, vget, abort,and vget
void test1(void){
  COid coid;
  Ptr<Valbuf> buf;
  TRANSACTION_VAR(t);
  int res;

  coid.cid=1;
  coid.oid=0;

  res = t.put(coid, "hi", 3); assert(res==0);
  res = t.tryCommit(); assert(res==0);

  t.start();
  res = t.put(coid, "me!", 4); assert(res==0);
  // test to see if we see our own put
  res = t.vget(coid, buf); assert(res==0);
  assert(buf->len == 4);
  assert(buf->type == 0);
  assert(strcmp(buf->u.buf, "me!") == 0);
  t.abort();
  res = t.tryCommit(); assert(res!=0);

  // check to see if abort got rid of our put
  t.start();
  res = t.vget(coid, buf); assert(res==0);
  assert(buf->len == 3);
  assert(buf->type == 0);
  assert(strcmp(buf->u.buf, "hi") == 0);
}
 
// test2: simple test of put followed by vget and vsuperget
void test2(){
  int res;
  static char *data = "DATA HERE";

  int outcome;
  COid coid;

  coid.cid = 2;
  coid.oid = 0;
  
  TRANSACTION_VAR(t);

  t.start();
  res = t.put(coid, data, (int)strlen(data)+1);
  assert(res==0);
  outcome = t.tryCommit();
  assert(outcome == 0);

  t.start();
  Ptr<Valbuf> buf;
  res = t.vget(coid, buf);
  assert(res==0);
  assert(buf->len == (int)strlen(data)+1);
  assert(strcmp(buf->u.buf, data) == 0);
  
  Ptr<Valbuf> vbuf;
  res = t.vsuperget(coid, vbuf, 0, 0);
  assert(res == GAIAERR_WRONG_TYPE);
}

// test3: contention of small read-modify-write transactions on 2 objects
OSTHREAD_FUNC test3thread(void *parm){
  int i, res, count, v1, v2;
  int tosum;
  COid coid1, coid2;

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test3", false);
#endif
  
  tosum = (int)(long long) parm;

  TRANSACTION_VAR(t);

  coid1.cid = 3;
  coid1.oid = 0;
  coid2.cid = 3;
  coid2.oid = 256;
  res = 0;
  count = 0;
  Ptr<Valbuf> buf1, buf2;
  v1 = v2 = 0;

  t.write(coid1, (char*) &v1, sizeof(int));
  t.write(coid2, (char*) &v2, sizeof(int));
  t.tryCommit();

  t.start();
  for (i=0; i<1000; ++i){
    if (rand()%2==0){
      res = t.vget(coid1, buf1); assert(res==0);
      if (buf1->len==0) assert(0);
      else v1 = *(int*)buf1->u.buf;
      v1 += tosum;
      res = t.write(coid1, (char*) &v1, sizeof(int)); assert(res==0);

      res = t.vget(coid2, buf2); assert(res==0);
      if (buf2->len==0) assert(0);
      else v2 = *(int*)buf2->u.buf;
      v2 += tosum;
      assert(v1 == v2);
      res = t.write(coid2, (char*) &v2, sizeof(int)); assert(res==0);
    } else {
      res = t.vget(coid2, buf2); assert(res==0);
      if (buf2->len==0) assert(0);
      else v2 = *(int*)buf2->u.buf;
      v2 += tosum;
      res = t.write(coid2, (char*) &v2, sizeof(int)); assert(res==0);

      res = t.vget(coid1, buf1); assert(res==0);
      if (buf1->len==0) assert(0);
      else v1 = *(int*)buf1->u.buf;
      v1 += tosum;
      assert(v1 == v2);
      res = t.write(coid1, (char*) &v1, sizeof(int)); assert(res==0);
    }
    res = t.tryCommit();
    count += res==0 ? 1 : 0;
    //if (res5==0){
    //printf("v1 %d>%d v2 %d>%d res %d %d %d %d %d\n", v1-tosum, v1,
    //       v2-tosum, v2, res1, res2, res3, res4, res5);
    t.start();
  }
  return 0;
}

void test3(){
  OSThread_t threads[2];
  int res;
  void *tres;
  res = OSCreateThread(&threads[0], test3thread, (void*) 1); assert(res==0);
  res = OSCreateThread(&threads[1], test3thread, (void*) 2); assert(res==0);
  OSWaitThread(threads[0], &tres);
  OSWaitThread(threads[1], &tres);
}

// test4: write supervalue, listadd, read supervalue
void test4(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  int i;
  SuperValue sv;

  TRANSACTION_VAR(t);

  coid.cid=4;
  coid.oid=0;

  SetAttrs(&sv);
  SetIntCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();

  ListCell lc;
  for (i=5; i <= 40; ++i){
    lc = SetIntCell(i);
    res = t.listAdd(coid, &lc, 0, 0); assert(res==0);
  }
  res = t.tryCommit(); assert(res==0);

  t.start();
  res = t.vsuperget(coid, buf, 0, 0); assert(res==0);
  
  SuperValue *svp = buf->u.raw;
  CheckAttrs(svp);
  CheckIntCells(svp, 41);

  t.start();
  for (i=41; i <= 50; ++i){
    lc = SetIntCell(i);
    res = t.listAdd(coid, &lc, 0, 0); assert(res==0);
  }
  // try to read before committing
  t.vsuperget(coid, buf, 0, 0);
  
  svp = buf->u.raw;
  CheckAttrs(svp);
  CheckIntCells(svp, 51);

  // now commit
  res = t.tryCommit();
  assert(res==0);

  // try to read from a separate transaction
  t.start();
  t.vsuperget(coid, buf, 0, 0);
  svp = buf->u.raw;
  CheckAttrs(svp);
  CheckIntCells(svp, 51);
}

// test5:  write supervalue, listadd, read supervalue with keyinfo
void test5(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  int i;
  ListCell lc;
  SuperValue sv;

  TRANSACTION_VAR(t);

  coid.cid=5;
  coid.oid=0;

  SetAttrs(&sv);
  SetStrCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  for (i=5; i <= 40; ++i){
    t.start();
    lc = GetStrCell(i);
    res = t.listAdd(coid, &lc, KI, 0); assert(res==0);
    res = t.tryCommit(); assert(res==0);
  }

  t.start();
  t.vsuperget(coid, buf, 0, 0);

  SuperValue *svp = buf->u.raw;
  CheckAttrs(svp);
  CheckStrCells(svp, 41);
}
    
// test6:  adds many values, delrange
void test6(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  int i;

  TRANSACTION_VAR(t);

  coid.cid=6;
  coid.oid=0;

  SuperValue sv;
  SetAttrs(&sv);
  SetIntCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();

  ListCell lc;
  for (i=5; i <= 40; ++i){
    lc = SetIntCell(i);
    res = t.listAdd(coid, &lc, 0, 0); assert(res==0);
  }
  res = t.tryCommit();
  assert(res==0);

  ListCell lc2;

  t.start();
  lc = SetIntCell(10);
  lc2 = SetIntCell(30);
  res = t.listDelRange(coid, 3, &lc, &lc2, 0);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();
  t.vsuperget(coid, buf, 0, 0);
  SuperValue *svp = buf->u.raw;
  CheckAttrs(svp);
  static int values[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 30, 31, 32, 33, 34, 35,
                          36, 37, 38, 39, 40 };
  CheckIntCellsList(svp, values, sizeof(values)/sizeof(int));
}

// test7:  adds many values, delrange with keyinfo
void test7(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  int i;

  TRANSACTION_VAR(t);

  coid.cid=7;
  coid.oid=0;

  SuperValue sv;
  SetAttrs(&sv);
  SetStrCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();
  ListCell lc;
  for (i=0; i <= 40; ++i){
    lc = GetStrCell(i);
    res = t.listAdd(coid, &lc, KI, 0); assert(res==0);
  }
  res = t.tryCommit();
  assert(res==0);

  ListCell lc2;

  t.start();
  lc = GetStrCell(10);
  lc2 = GetStrCell(30);
  res = t.listDelRange(coid, 4, &lc, &lc2, KI);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();
  t.vsuperget(coid, buf, 0, 0);
  SuperValue *svp = buf->u.raw;
  CheckAttrs(svp);
  static char *values[] = { "000", "001", "002", "003", "004", "005", "006",
                            "007", "008", "009", "031", "032", "033", "034",
                            "035", "036", "037", "038", "039", "040" };
  CheckStrCellsList(svp, values, sizeof(values)/sizeof(char*));
}

// test7b:  delrange with keyinfo and several interval types
void test7b_populate(int n){ // 0,2,4,...,2*(n-1)
  int res;
  COid coid;
  int i;
  TRANSACTION_VAR(t);
  ListCell lc;
  SuperValue sv;
  
  coid.cid=1007;
  coid.oid=0;
  SetAttrs(&sv); SetStrCells(&sv, 0);
  res = t.writeSuperValue(coid, &sv); assert(res==0);
  for (i=0; i < n; ++i){
    lc = GetStrCell(2*i);
    res = t.listAdd(coid, &lc, KI, 0); assert(res==0);
  }
  res = t.tryCommit(); assert(res==0);
}

// do test where we read in the same transaction
void test7b_onetx(int n, int left, int right, int intervtype, char **vals,
                  int len){
  ListCell lc, lc2;
  COid coid;
  int res;
  Ptr<Valbuf> buf;
  SuperValue *svp;
  test7b_populate(n);
  TRANSACTION_VAR(t);

  coid.cid=1007;
  coid.oid=0;
  lc = GetStrCell(left);
  lc2 = GetStrCell(right);
  res = t.listDelRange(coid, intervtype, &lc, &lc2, KI);
  assert(res==0);
  res = t.vsuperget(coid, buf, 0, 0);
  assert(res==0);
  svp = buf->u.raw;
  CheckStrCellsList(svp, vals, len);
  t.abort();
}

void test7b_twotx(int n, int left, int right, int intervtype, char **vals,
                  int len){
  ListCell lc, lc2;
  COid coid;
  int res;
  Ptr<Valbuf> buf;
  SuperValue *svp;
  test7b_populate(n);
  TRANSACTION_VAR(t);

  coid.cid=1007;
  coid.oid=0;
  lc = GetStrCell(left);
  lc2 = GetStrCell(right);
  res = t.listDelRange(coid, intervtype, &lc, &lc2, KI);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);
  t.start();
  res = t.vsuperget(coid, buf, 0, 0);
  assert(res==0);
  svp = buf->u.raw;
  CheckStrCellsList(svp, vals, len);
}

void test7b_bothtx(int n, int left, int right, int intervtype, char **vals,
                   int len){
  test7b_onetx(n, left, right, intervtype, vals, len);
  test7b_twotx(n, left, right, intervtype, vals, len);
}

void test7b0(){
  // intervtype 0
  static char *values0[] = { "000", "002", "006", "008" };
  test7b_bothtx(5, 2, 6, 0, values0, sizeof(values0)/sizeof(char*));
  static char *values0b[] = { "000", "002", "006", "008", "010" };
  test7b_bothtx(6, 2, 6, 0, values0b, sizeof(values0b)/sizeof(char*));

  // intervtype 1
  static char *values1[] = { "000", "002", "008" };
  test7b_bothtx(5, 2, 6, 1, values1, sizeof(values1)/sizeof(char*));
  static char *values1b[] = { "000", "002", "008", "010" };
  test7b_bothtx(6, 2, 6, 1, values1b, sizeof(values1b)/sizeof(char*));

  // intervtype 2
  static char *values2[] = { "000", "002" };
  test7b_bothtx(5, 2, 6, 2, values2, sizeof(values2)/sizeof(char*));
  static char *values2b[] = { "000", "002" };
  test7b_bothtx(6, 2, 6, 2, values2b, sizeof(values2b)/sizeof(char*));

  // intervtype 3
  static char *value3[] = { "000", "006", "008" };
  test7b_bothtx(5, 2, 6, 3, value3, sizeof(value3)/sizeof(char*));
  static char *value3b[] = { "000", "006", "008", "010" };
  test7b_bothtx(6, 2, 6, 3, value3b, sizeof(value3b)/sizeof(char*));
  
  // intervtype 4
  static char *value4[] = { "000", "008" };
  test7b_bothtx(5, 2, 6, 4, value4, sizeof(value4)/sizeof(char*));
  static char *value4b[] = { "000", "008", "010" };
  test7b_bothtx(6, 2, 6, 4, value4b, sizeof(value4b)/sizeof(char*));
  
  // intervtype 5
  static char *value5[] = { "000" };
  test7b_bothtx(5, 2, 6, 5, value5, sizeof(value5)/sizeof(char*));
  static char *value5b[] = { "000" };
  test7b_bothtx(6, 2, 6, 5, value5b, sizeof(value5b)/sizeof(char*));
  
  // intervtype 6
  static char *value6[] = { "006", "008" };
  test7b_bothtx(5, 2, 6, 6, value6, sizeof(value6)/sizeof(char*));
  static char *value6b[] = { "006", "008", "010" };
  test7b_bothtx(6, 2, 6, 6, value6b, sizeof(value6b)/sizeof(char*));
  
  // intervtype 7
  static char *value7[] = { "008" };
  test7b_bothtx(5, 2, 6, 7, value7, sizeof(value7)/sizeof(char*));
  static char *value7b[] = { "008", "010" };
  test7b_bothtx(6, 2, 6, 7, value7b, sizeof(value7b)/sizeof(char*));

  // intervtype 8
  static char *value8[] = { };
  test7b_bothtx(5, 2, 6, 8, value8, sizeof(value8)/sizeof(char*));
  static char *value8b[] = { };
  test7b_bothtx(6, 2, 6, 8, value8b, sizeof(value8b)/sizeof(char*));
}

void test7b1(){
  // intervtype 0
  static char *values0[] = { "000", "006", "008" };
  test7b_bothtx(5, 0, 6, 0, values0, sizeof(values0)/sizeof(char*));
  static char *values0b[] = { "000", "006", "008", "010" };
  test7b_bothtx(6, 0, 6, 0, values0b, sizeof(values0b)/sizeof(char*));

  // intervtype 1
  static char *values1[] = { "000", "008" };
  test7b_bothtx(5, 0, 6, 1, values1, sizeof(values1)/sizeof(char*));
  static char *values1b[] = { "000", "008", "010" };
  test7b_bothtx(6, 0, 6, 1, values1b, sizeof(values1b)/sizeof(char*));

  // intervtype 2
  static char *values2[] = { "000", };
  test7b_bothtx(5, 0, 6, 2, values2, sizeof(values2)/sizeof(char*));
  static char *values2b[] = { "000", };
  test7b_bothtx(6, 0, 6, 2, values2b, sizeof(values2b)/sizeof(char*));

  // intervtype 3
  static char *value3[] = { "006", "008" };
  test7b_bothtx(5, 0, 6, 3, value3, sizeof(value3)/sizeof(char*));
  static char *value3b[] = { "006", "008", "010" };
  test7b_bothtx(6, 0, 6, 3, value3b, sizeof(value3b)/sizeof(char*));
  
  // intervtype 4
  static char *value4[] = { "008" };
  test7b_bothtx(5, 0, 6, 4, value4, sizeof(value4)/sizeof(char*));
  static char *value4b[] = { "008", "010" };
  test7b_bothtx(6, 0, 6, 4, value4b, sizeof(value4b)/sizeof(char*));
  
  // intervtype 5
  static char *value5[] = { };
  test7b_bothtx(5, 0, 6, 5, value5, sizeof(value5)/sizeof(char*));
  static char *value5b[] = { };
  test7b_bothtx(6, 0, 6, 5, value5b, sizeof(value5b)/sizeof(char*));
  
  // intervtype 6
  static char *value6[] = { "006", "008" };
  test7b_bothtx(5, 0, 6, 6, value6, sizeof(value6)/sizeof(char*));
  static char *value6b[] = { "006", "008", "010" };
  test7b_bothtx(6, 0, 6, 6, value6b, sizeof(value6b)/sizeof(char*));
  
  // intervtype 7
  static char *value7[] = { "008" };
  test7b_bothtx(5, 0, 6, 7, value7, sizeof(value7)/sizeof(char*));
  static char *value7b[] = { "008", "010" };
  test7b_bothtx(6, 0, 6, 7, value7b, sizeof(value7b)/sizeof(char*));

  // intervtype 8
  static char *value8[] = { };
  test7b_bothtx(5, 0, 6, 8, value8, sizeof(value8)/sizeof(char*));
  static char *value8b[] = { };
  test7b_bothtx(6, 0, 6, 8, value8b, sizeof(value8b)/sizeof(char*));
}

void test7b2(){
  // intervtype 0
  static char *values0[] = { "000", "002", "008" };
  test7b_bothtx(5, 2, 8, 0, values0, sizeof(values0)/sizeof(char*));
  static char *values0b[] = { "000", "002", "010" };
  test7b_bothtx(6, 2, 10, 0, values0b, sizeof(values0b)/sizeof(char*));

  // intervtype 1
  static char *values1[] = { "000", "002" };
  test7b_bothtx(5, 2, 8, 1, values1, sizeof(values1)/sizeof(char*));
  static char *values1b[] = { "000", "002" };
  test7b_bothtx(6, 2, 10, 1, values1b, sizeof(values1b)/sizeof(char*));

  // intervtype 2
  static char *values2[] = { "000", "002" };
  test7b_bothtx(5, 2, 8, 2, values2, sizeof(values2)/sizeof(char*));
  static char *values2b[] = { "000", "002" };
  test7b_bothtx(6, 2, 10, 2, values2b, sizeof(values2b)/sizeof(char*));

  // intervtype 3
  static char *value3[] = { "000", "008" };
  test7b_bothtx(5, 2, 8, 3, value3, sizeof(value3)/sizeof(char*));
  static char *value3b[] = { "000", "010" };
  test7b_bothtx(6, 2, 10, 3, value3b, sizeof(value3b)/sizeof(char*));
  
  // intervtype 4
  static char *value4[] = { "000" };
  test7b_bothtx(5, 2, 8, 4, value4, sizeof(value4)/sizeof(char*));
  static char *value4b[] = { "000" };
  test7b_bothtx(6, 2, 10, 4, value4b, sizeof(value4b)/sizeof(char*));
  
  // intervtype 5
  static char *value5[] = { "000" };
  test7b_bothtx(5, 2, 8, 5, value5, sizeof(value5)/sizeof(char*));
  static char *value5b[] = { "000" };
  test7b_bothtx(6, 2, 10, 5, value5b, sizeof(value5b)/sizeof(char*));
  
  // intervtype 6
  static char *value6[] = { "008" };
  test7b_bothtx(5, 2, 8, 6, value6, sizeof(value6)/sizeof(char*));
  static char *value6b[] = { "010" };
  test7b_bothtx(6, 2, 10, 6, value6b, sizeof(value6b)/sizeof(char*));
  
  // intervtype 7
  static char *value7[] = { };
  test7b_bothtx(5, 2, 8, 7, value7, sizeof(value7)/sizeof(char*));
  static char *value7b[] = { };
  test7b_bothtx(6, 2, 10, 7, value7b, sizeof(value7b)/sizeof(char*));

  // intervtype 8
  static char *value8[] = { };
  test7b_bothtx(5, 2, 8, 8, value8, sizeof(value8)/sizeof(char*));
  static char *value8b[] = { };
  test7b_bothtx(6, 2, 10, 8, value8b, sizeof(value8b)/sizeof(char*));
}

void test7b(){
  test7b0();
  test7b1();
  test7b2();
}

// test8:  attrset
void test8(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  TRANSACTION_VAR(t);
  SuperValue *svp;

  coid.cid=8;
  coid.oid=0;

  SuperValue sv;
  SetAttrs(&sv);
  SetStrCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();
  res = t.attrSet(coid, 1, 0xbacabaca); assert(res==0);
  res = t.attrSet(coid, 3, 0xcabacaba); assert(res==0);

  // check if reads prior to commit sees the attrset
  res = t.vsuperget(coid, buf, 0, 0); assert(res==0);
  svp = buf->u.raw;
  assert(svp->Nattrs == NATTRS);
  for (int i=0; i < NATTRS; ++i){
    if (i == 1) assert(svp->Attrs[i] == 0xbacabaca);
    else if (i == 3) assert(svp->Attrs[i] == 0xcabacaba);
    else assert((int)svp->Attrs[i] == i + 1000);
  }

  // now check if reads after commit sees the attrset
  res = t.tryCommit(); assert(res==0);
  t.start();
  res = t.vsuperget(coid, buf, 0, 0); assert(res==0);
  svp = buf->u.raw;
  assert(svp->Nattrs == NATTRS);
  for (int i=0; i < NATTRS; ++i){
    if (i == 1) assert(svp->Attrs[i] == 0xbacabaca);
    else if (i == 3) assert(svp->Attrs[i] == 0xcabacaba);
    else assert((int)svp->Attrs[i] == i + 1000);
  }
}

// test9:  add 40 items, delrange [1,20) with keyinfo
void test9(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  int i;
  TRANSACTION_VAR(t);

  coid.cid=9;
  coid.oid=0;

  SuperValue sv;
  SetAttrs(&sv);
  SetStrCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  t.start();
  ListCell lc;
  for (i=5; i <= 40; ++i){
    lc = GetStrCell(i);
    res = t.listAdd(coid, &lc, KI, 0); assert(res==0);
  }

  ListCell lc2;

  lc = GetStrCell(1);
  lc2 = GetStrCell(20);
  res = t.listDelRange(coid, 4, &lc, &lc2, KI);
  assert(res==0);

  res = t.vsuperget(coid, buf, 0, 0);
  assert(res==0);
  SuperValue *svp = buf->u.raw;
  CheckAttrs(svp);
  static char *values[] = { "000", "021", "022", "023", "024", "025", "026",
                            "027", "028", "029", "030", "031", "032", "033",
                            "034", "035", "036", "037", "038", "039", "040" };
  CheckStrCellsList(svp, values, sizeof(values)/sizeof(char*));

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);

  res = t.vsuperget(coid, buf, 0, 0);
  assert(res==0);
  svp = buf->u.raw;
  CheckAttrs(svp);
  CheckStrCells(svp, 5);
  //CheckStrCellsList(svp, values, sizeof(values)/sizeof(char*));  

  res = t.tryCommit();
  assert(res==0);

  t.start();
  res = t.vsuperget(coid, buf, 0, 0);
  assert(res==0);
  svp = buf->u.raw;
  CheckAttrs(svp);
  CheckStrCells(svp, 5);
  //CheckStrCellsList(svp, values, sizeof(values)/sizeof(char*));  
}

// test10: adds 1000 items using a single tx per item
void test10(){
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  int i;
  TRANSACTION_VAR(t);

  coid.cid=10;
  coid.oid=0;

  SuperValue sv;
  SetAttrs(&sv);
  SetStrCells(&sv, 5);

  res = t.writeSuperValue(coid, &sv); assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  ListCell lc;
  for (i=5; i <= 999; ++i){
    t.start();
    lc = GetStrCell(i);
    res = t.listAdd(coid, &lc, KI, 0); assert(res==0);
    res = t.tryCommit(); assert(res==0);
  }

  t.start();
  res = t.vsuperget(coid, buf, 0, 0);
  assert(res==0);
  SuperValue *svp = buf->u.raw;
  CheckAttrs(svp);
  CheckStrCells(svp, 1000);
}

// test11: throughput test for reads with many threads
#define TEST11_NOBJECTS 10
#define TEST11_NREADS 5000
#define TEST11_NTHREADS 64

u64 *test11_durations=0;

OSTHREAD_FUNC test11Worker(void *parm){
  int threadno = (int)(long long) parm;
  COid coid;
  int i, res;
  Ptr<Valbuf> vbuf1, vbuf2;
  Prng rng(pthread_self());
  u64 start, end;

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test11", false);
#endif

  TRANSACTION_VAR(t);
  coid.cid = 11;

  start = Time::now();
  for (i=0; i<TEST11_NREADS; ++i){
    coid.oid = rng.next() % TEST11_NOBJECTS;
    t.start();
    res = t.vget(coid, vbuf1);
    if (res)
      printf("vget: error %d\n", res);
    if (vbuf1->len == 0){
      printf("Read len 0\n");
      fflush(stdout);
    }
    res = t.tryCommit();
  }
  end = Time::now();
  test11_durations[threadno] = end-start;

  return 0;
}

void test11(){
  COid coid;
  int i, res;
  int nthreads;
  OSThread_t *threads;
  char str[256];

  nthreads = TEST11_NTHREADS;

  test11_durations = new u64[nthreads];
  TRANSACTION_VAR(t);

  coid.cid = 11;
  t.start();
  for (i=0; i<TEST11_NOBJECTS; ++i){
    sprintf(str, "Value%d", i);
    coid.oid = i;
    t.write(coid, str, (int)strlen(str)+1);
  }
  res = t.tryCommit();
  assert(res==0);
  mssleep(100);

  // create threads

  threads = new OSThread_t[nthreads];
  for (i=0; i < nthreads; ++i){
    res = OSCreateThread(threads+i, test11Worker, (void*) (long long) i);
    assert(res==0);
  }

  void *tres;
  for (i=0; i < nthreads; ++i){
    OSWaitThread(threads[i], &tres);
  }

  double totaltput = 0;
  double thistput;

  for (i=0; i < nthreads; ++i){
    thistput = (double)TEST11_NREADS / test11_durations[i];
    printf("  Thread %d: tput %f ops/ms latency %f ms/op\n", i, thistput,
           1/thistput);
    totaltput += thistput;
  }

  printf(  "Total tput %f ops/ms\n", totaltput);

  delete [] threads;
  delete [] test11_durations;
}

// test12: contention test for listadd, delrange, readv on single object with
// many threads

#define TEST12_NOBJECTS 100
#define TEST12_NOPS 1000
#define TEST12_NTHREADS 64

struct Test12ThreadData {
  u64 duration;
  u64 commitfail;
  int countoverflow;
  int advance;
};

Test12ThreadData *test12td = 0;

OSTHREAD_FUNC test12Worker(void *parm){
  int threadno = (int)(long long) parm;
  int i, res;
  Ptr<Valbuf> vbuf1;
  Prng rng(threadno);
  //Prng rng(pthread_self());
  //Prng rng(55);
  u64 start, end;
  int op;
  ListCell lc1, lc2;
  int v1, v2, v3;
  int commitfail=0;
  COid coid;

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test12", false);
#endif

  coid.cid = 12;
  coid.oid = 0;

  TRANSACTION_VAR(t);

  // rng.next()
  start = Time::now();
  for (i=0; i<TEST12_NOPS; ++i){
    t.startDeferredTs();
    //if (threadno == 0) 
    op = rng.next() % 3;
    //else op = rng.next() % 2;
    switch(op){
    case 0: // read
      res = t.vsuperget(coid, vbuf1, 0, 0);
      assert(res==0);
      CheckAttrs(vbuf1->u.raw);
      break;
    case 1: // add
      v1 = rng.next() % TEST12_NOBJECTS;
      lc1 = SetIntCell(v1);
      res = t.listAdd(coid, &lc1, 0, 0); assert(res==0);
      break;
    case 2: // delrange
      v1 = rng.next() % TEST12_NOBJECTS;
      v2 = v1;
      if (v2<v1){ v3=v1; v1=v2; v2=v3; }
      lc1 = SetIntCell(v1);
      lc2 = SetIntCell(v2);
      res = t.listDelRange(coid, 4, &lc1, &lc2, 0);
      assert(res==0);
      break;
    }
    res = t.tryCommit();
    if (res) ++commitfail;
  }
  end = Time::now();
  test12td[threadno].duration = end-start;
  test12td[threadno].commitfail = commitfail;
  test12td[threadno].countoverflow = Timestamp::getcountoverflow();
  test12td[threadno].advance = Timestamp::getadvance();

  return 0;
}

void test12(){
  int i, res;
  int nthreads;
  OSThread_t *threads;
  COid coid;

  nthreads = TEST12_NTHREADS;
  coid.cid = 12;
  coid.oid = 0;

  test12td = new Test12ThreadData[nthreads];
  memset(test12td, sizeof(Test12ThreadData)*nthreads, 0);
  
  TRANSACTION_VAR(t);

  // first write objects
  t.start();
  SuperValue sv;
  SetAttrs(&sv);
  sv.CellType = 0; // int
  sv.Ncells = TEST12_NOBJECTS/2;
  sv.Cells = new ListCell[sv.Ncells];
  ListCell *cellptr = sv.Cells;
  for (i=0; i < sv.Ncells; ++i){
    cellptr[i] = SetIntCell(i*2);
  }

  res = t.writeSuperValue(coid, &sv);
  assert(res==0);
  res = t.tryCommit();
  assert(res==0);

  mssleep(100);

  // create threads

  threads = new OSThread_t[nthreads];
  for (i=0; i < nthreads; ++i){
    res = OSCreateThread(threads+i, test12Worker, (void*) (long long) i);
    assert(res==0);
  }

  void *tres;
  for (i=0; i < nthreads; ++i){
    OSWaitThread(threads[i], &tres);
  }

  double totaltput = 0;
  double thistput;

  for (i=0; i < nthreads; ++i){
    thistput = (double)TEST12_NOPS / test12td[i].duration;
    printf("  Thread %d: tput %f ops/ms latency %f ms/op commitfail %d countoverflow %d advance %d\n", i, thistput, 1/thistput, (int) test12td[i].commitfail,
           test12td[i].countoverflow, test12td[i].advance);
    totaltput += thistput;
  }

  printf("  Total tput %f ops/ms\n", totaltput);

  delete [] threads;
  delete [] test12td;
}

// test13: contention test where reader keeps reading, and writer writes
// successive values
#define TEST13_NOPS_READ 50000
#define TEST13_NOPS_WRITE 1000
Align8 int test13_failed_read;
Align8 int test13_failed_write;
 
OSTHREAD_FUNC test13Worker(void *parm){
  COid coid;
  int i, val, res;
  int writer;
  Ptr<Valbuf> vbuf;

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test13", false);
#endif

  writer = (int)(long long) parm;

  TRANSACTION_VAR(t);

  coid.cid = 13;
  coid.oid = 0;

  if (!writer){ // reader code
    int lastval = -1;
    for (i=0; i < TEST13_NOPS_READ; ++i){
      t.start();
      res = t.vget(coid, vbuf);
      if (res){
        ++test13_failed_read;
        continue;
      }
      assert(vbuf->len != 0);
      val = *(int*)(vbuf->u.buf);
      assert(val >= lastval);
      lastval = val;
    }
  } else { // writer code
    val=0;
    for (i=0; i < TEST13_NOPS_WRITE; ++i){
      t.start();
      res = t.write(coid, (char*)&val, sizeof(int)); assert(res==0);
      res = t.tryCommit();
      if (res){
        ++test13_failed_write;
        continue;
      }
      ++val;
    }
  }
  return 0;
}

void test13(){
  int res;
  OSThread_t twriter, treader;
  COid coid;
  TRANSACTION_VAR(t);
  int val = 0;
  
  coid.cid = 13;
  coid.oid = 0;
  t.start();
  res = t.write(coid, (char*)&val, sizeof(int)); assert(res==0);
  res = t.tryCommit(); assert(res==0);
  
  test13_failed_read = 0;
  test13_failed_write = 0;
  res = OSCreateThread(&treader, test13Worker, (void*) (long long) 0);
  assert(res==0);
  res = OSCreateThread(&twriter, test13Worker, (void*) (long long) 1);
  assert(res==0);
  void *tres;
  OSWaitThread(treader, &tres);
  OSWaitThread(twriter, &tres);
  MemBarrier();
  printf("  failed_read %d failed_write %d\n", test13_failed_read,
         test13_failed_write);
}

// test14: various error conditions
void test14(){
  COid coid;
  Ptr<Valbuf> buf;
  int res;
  SuperValue sv;
  ListCell lc;
  TRANSACTION_VAR(t);

  coid.cid = 14;
  coid.oid = 0;
  lc = SetIntCell(0);
  
  // write supervalue, read value
  SetAttrs(&sv);
  SetIntCells(&sv, 5);
  res = t.writeSuperValue(coid, &sv); assert(res==0);
  res = t.vget(coid, buf); assert(res==GAIAERR_WRONG_TYPE);
  res = t.tryCommit(); assert(res==0);
  t.start();
  res = t.vget(coid, buf); assert(res==GAIAERR_WRONG_TYPE);
    
  // write value, read supervalue, attrset, listadd, listdelrange
  t.start();
  res = t.put(coid, "hi", 3); assert(res==0);
  res = t.vsuperget(coid, buf, 0, 0); assert(res==GAIAERR_WRONG_TYPE);
  res = t.attrSet(coid, 0, 0); assert(res==GAIAERR_WRONG_TYPE);
  res = t.listAdd(coid, &lc, 0, 0); assert(res==GAIAERR_WRONG_TYPE);
  
  res = t.listDelRange(coid, 4, &lc, &lc, 0); assert(res==GAIAERR_WRONG_TYPE);
  
  res = t.tryCommit(); assert(res==0);
  t.start();
  res = t.vsuperget(coid, buf, 0, 0); assert(res==GAIAERR_WRONG_TYPE);
  //res = t.attrSet(coid, 0, 0);
  // assert(res==GAIAERR_WRONG_TYPE); // chk not implemented
  //res = t.listAdd(coid, &lc, 0, 0);
  // assert(res==GAIAERR_WRONG_TYPE); // chk not implemented
  //res = t.listDelRange(coid, 4, &lc, &lc, 0);
  // assert(res==GAIAERR_WRONG_TYPE); // chk not implemented
}

// test15: move random value from one place to another, check that sum is
// constant
#define TEST15_NTHREADS 16
#define TEST15_NOPS 10000
 
OSTHREAD_FUNC test15Worker(void *parm){
  COid coidx, coidy;
  int i, res;
  i32 x, y;
  int myid = (int)(long long) parm;
  Ptr<Valbuf> buf;
  int r;
  int successful = 0;
  SimplePrng prng(myid);

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test15", false);
#endif

  TRANSACTION_VAR(t);

  coidx.cid = 15;
  coidx.oid = 0;
  coidy.cid = 15;
  coidy.oid = 1;

  for (i=0; i < TEST15_NOPS; ++i){
    // read x and y
    t.start();
    res = t.vget(coidx, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    x = *(int*)buf->u.buf;
    res = t.vget(coidy, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    y = *(int*)buf->u.buf;

    assert(x+y == 0);

    t.start();
    r = (prng.next() % 41) - 20; // random number from -20 to +20
    x += r;
    y -= r;
    res = t.put(coidx, (char*) &x, 4); assert(res==0);
    res = t.put(coidy, (char*) &y, 4); assert(res==0);
    res = t.tryCommit();
    if (!res) ++successful;
  }
  
  return (void*) (long long) successful;
}

void test15(){
  int i, res;
  int nthreads;
  OSThread_t *threads;
  COid coidx, coidy;
  
  coidx.cid = 15;
  coidx.oid = 0;
  coidy.cid = 15;
  coidy.oid = 1;

  TRANSACTION_VAR(t);
  i32 val = 0;
  res = t.put(coidx, (char*) &val, 4); assert(res==0);
  res = t.put(coidy, (char*) &val, 4); assert(res==0);
  res = t.tryCommit(); assert(res==0);

  mssleep(120);

  nthreads = TEST15_NTHREADS;

  // create threads
  threads = new OSThread_t[nthreads];
  for (i=0; i < nthreads; ++i){
    res = OSCreateThread(threads+i, test15Worker, (void*) (long long) i);
    assert(res==0);
  }

  void *tres;
  int successful;
  for (i=0; i < nthreads; ++i){
    OSWaitThread(threads[i], &tres);
    successful = (int)(long long) tres;
    printf("  Thread %d successful %d\n", i, successful);
  }

  delete [] threads;
}


// test16: move random item from one supervalue to another, check that
//         supervalues partition initial list
#define TEST16_NTHREADS 16
#define TEST16_NOPS 10000
#define TEST16_NITEMS 20

void test16CheckItems(SuperValue *svpx, SuperValue *svpy){
  int i;
  int xindex, yindex;
  xindex = yindex = 0;
  for (i = 0; i < TEST16_NITEMS; ++i){
    if (xindex < svpx->Ncells && GetIntCell(svpx->Cells+xindex)==i){
      assert(yindex == svpy->Ncells || GetIntCell(svpy->Cells+yindex)!=i);
      ++xindex;
    } else if (yindex < svpy->Ncells && GetIntCell(svpy->Cells+yindex)==i){
      assert(xindex == svpx->Ncells || GetIntCell(svpx->Cells+xindex)!=i);
      ++yindex;
    } else assert(0);
  }
  assert(xindex == svpx->Ncells && yindex == svpy->Ncells);
}

OSTHREAD_FUNC test16Worker(void *parm){
  COid coidx, coidy;
  int i, res;
  int myid = (int)(long long) parm;
  Ptr<Valbuf> bufx, bufy;
  int r, bit;
  int successful = 0;
  SimplePrng prng(myid);
  SuperValue *svpx, *svpy;
  ListCell *lc;

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test16", false);
#endif

  TRANSACTION_VAR(t);

  coidx.cid = 16;
  coidx.oid = 0;
  coidy.cid = 16;
  coidy.oid = 1;

  for (i=0; i < TEST16_NOPS; ++i){
    // read x and y
    t.start();
    res = t.vsuperget(coidx, bufx, 0, 0); assert(res==0);
    assert(bufx->type == 1);
    svpx = bufx->u.raw;

    res = t.vsuperget(coidy, bufy, 0, 0); assert(res==0);
    assert(bufy->type == 1);
    svpy = bufy->u.raw;
    test16CheckItems(svpx, svpy);

    t.start();
    bit = prng.next() % 2;
    if (bit){ // move left to right
      if (svpx->Ncells >= 1){ // make sure we have at least 1 cell
        r = prng.next() % svpx->Ncells;
        lc = svpx->Cells+r;
        
        res = t.listDelRange(coidx, 4, lc, lc, 0); assert(res==0);
        res = t.listAdd(coidy, lc, 0, 0); assert(res==0);
        res = t.tryCommit();
      }
    } else { // move right to left
      if (svpy->Ncells >= 1){ // make sure we have at least 1 cell
        r = prng.next() % svpy->Ncells;
        lc = svpy->Cells+r;
        
        res = t.listDelRange(coidy, 4, lc, lc, 0); assert(res==0);
        res = t.listAdd(coidx, lc, 0, 0); assert(res==0);
        res = t.tryCommit();
      }
    }
    if (!res) ++successful;
  }
  
  return (void*) (long long) successful;
}

void test16(){
  int i, res;
  int nthreads;
  OSThread_t *threads;
  COid coidx, coidy;
  
  coidx.cid = 16;
  coidx.oid = 0;
  coidy.cid = 16;
  coidy.oid = 1;
  SuperValue sv1, sv2;

  // initialize one sv with 0..NITEMS-1, the other with empty list
  TRANSACTION_VAR(t);
  SetAttrs(&sv1);
  SetIntCells(&sv1, TEST16_NITEMS);
  SetAttrs(&sv2);
  res = t.writeSuperValue(coidx, &sv1); assert(res==0);
  res = t.writeSuperValue(coidy, &sv2); assert(res==0);
  res = t.tryCommit(); assert(res==0);

  mssleep(120);

  nthreads = TEST16_NTHREADS;

  // create threads
  threads = new OSThread_t[nthreads];
  for (i=0; i < nthreads; ++i){
    res = OSCreateThread(threads+i, test16Worker, (void*) (long long) i);
    assert(res==0);
  }

  void *tres;
  int successful;
  for (i=0; i < nthreads; ++i){
    OSWaitThread(threads[i], &tres);
    successful = (int)(long long) tres;
    printf("  Thread %d successful %d\n", i, successful);
  }

  delete [] threads;
}

// t1 adds add, t2 deletes range del1..del2 with intervtype,
// conflict indicates whether those transactions should conflict
void test17adddelrange(int add, int del1, int del2, int intervtype,
                       int conflict){
  ListCell lc, lc1, lc2;
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  SuperValue sv;
  TRANSACTION_VAR(t1);
  TRANSACTION_VAR(t2);
  
  coid.cid = 17;
  coid.oid = 0;

  // set up oid0 with supervalue
  t1.start();
  SetAttrs(&sv);
  SetIntCells(&sv, 5);
  res = t1.writeSuperValue(coid, &sv); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  
  lc = SetIntCell(add);
  lc1 = SetIntCell(del1);
  lc2 = SetIntCell(del2);

  t1.start();
  res = t1.vsuperget(coid,buf, 0, 0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf, 0, 0); assert(res==0);
  res = t1.listAdd(coid, &lc, 0, 0); assert(res==0);
  res = t2.listDelRange(coid, intervtype, &lc1, &lc2, 0); assert(res==0);  
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit();
  if (conflict) assert(res);
  else assert(res==0);

  t1.start();
  res = t1.vsuperget(coid,buf, 0, 0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf, 0, 0); assert(res==0);
  res = t1.listAdd(coid, &lc, 0, 0); assert(res==0);
  res = t2.listDelRange(coid, intervtype, &lc1, &lc2, 0); assert(res==0);  
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit();
  if (conflict) assert(res);
  else assert(res==0);
}

// t1 deletes range dela1..dela2, t2 deletes range delb1..delb2
// conflict indicates whether those transactions should conflict
void test17delranges(int dela1, int dela2, int delb1, int delb2,
                     int intervtype1, int intervtype2, int conflict){
  ListCell lca1, lca2, lcb1, lcb2;
  int res;
  COid coid;
  Ptr<Valbuf> buf;
  SuperValue sv;
  TRANSACTION_VAR(t1);
  TRANSACTION_VAR(t2);
  
  coid.cid = 17;
  coid.oid = 0;
  
  // set up oid0 with supervalue
  t1.start();
  SetAttrs(&sv);
  SetIntCells(&sv, 5);
  res = t1.writeSuperValue(coid, &sv); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  
  lca1 = SetIntCell(dela1);
  lca2 = SetIntCell(dela2);
  lcb1 = SetIntCell(delb1);
  lcb2 = SetIntCell(delb2);

  t1.start();
  res = t1.vsuperget(coid,buf, 0, 0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf, 0, 0); assert(res==0);
  res = t1.listDelRange(coid, intervtype1, &lca1, &lca2, 0); assert(res==0);  
  res = t2.listDelRange(coid, intervtype2, &lcb1, &lcb2, 0); assert(res==0);  
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit();
  if (conflict) assert(res);
  else assert(res==0);
  
  t1.start();
  res = t1.vsuperget(coid,buf, 0, 0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf, 0, 0); assert(res==0);
  res = t1.listDelRange(coid, intervtype1, &lca1, &lca2, 0); assert(res==0);  
  res = t2.listDelRange(coid, intervtype2, &lcb1, &lcb2, 0); assert(res==0);  
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit();
  if (conflict) assert(res);
  else assert(res==0);
}

// test17: conflicting and non-conflicting transactions
void test17(){
  int i, res;
  COid coid;
  ListCell lc;
  ListCell lc1,lc2;
  Ptr<Valbuf> buf;
  
  coid.cid = 17;

  TRANSACTION_VAR(t1);
  TRANSACTION_VAR(t2);
  SuperValue sv;

  // set up oid0 with uservalue, oid1 with value
  t1.start();
  coid.oid = 0;
  SetAttrs(&sv);
  SetIntCells(&sv, 5);
  res = t1.writeSuperValue(coid, &sv); assert(res==0);
  coid.oid = 1;
  i=0;
  res = t1.put(coid, (char*)&i, 4);
  res = t1.tryCommit(); assert(res==0);

  // attrset conflict
  coid.oid = 0;
  t1.start();
  res = t1.vsuperget(coid,buf,0,0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf,0,0); assert(res==0);
  res = t1.attrSet(coid, 1, 0); assert(res==0);
  res = t2.attrSet(coid, 1, 1); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res!=0);
  t1.start();
  res = t1.vsuperget(coid,buf,0,0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf,0,0); assert(res==0);
  res = t1.attrSet(coid, 1, 0); assert(res==0);
  res = t2.attrSet(coid, 1, 1); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res!=0);

  // attrset non-conflict
  coid.oid = 0;
  t1.start();
  res = t1.vsuperget(coid,buf,0,0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf,0,0); assert(res==0);
  res = t1.attrSet(coid, 1, 0); assert(res==0);
  res = t2.attrSet(coid, 2, 1); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  t1.start();
  res = t1.vsuperget(coid,buf,0,0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf,0,0); assert(res==0);
  res = t1.attrSet(coid, 1, 0); assert(res==0);
  res = t2.attrSet(coid, 2, 1); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  
  // listadd conflict
  coid.oid = 0;
  lc = SetIntCell(10);
  t1.start();
  res = t1.vsuperget(coid,buf,0,0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf,0,0); assert(res==0);
  res = t1.listAdd(coid, &lc, 0, 0); assert(res==0);
  res = t2.listAdd(coid, &lc, 0, 0); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res);
  t1.start();
  res = t1.vsuperget(coid,buf,0,0); assert(res==0);
  t2.start();
  res = t2.vsuperget(coid,buf,0,0); assert(res==0);
  res = t1.listAdd(coid, &lc, 0, 0); assert(res==0);
  res = t2.listAdd(coid, &lc, 0, 0); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res);
  
  // listadd, listdelrange conflicts
  test17adddelrange(10, 9, 11, 0, 1);
  test17adddelrange(10, 9, 11, 1, 1);
  test17adddelrange(10, 9, 11, 2, 1);
  test17adddelrange(10, 9, 11, 3, 1);
  test17adddelrange(10, 9, 11, 4, 1);
  test17adddelrange(10, 9, 11, 5, 1);
  test17adddelrange(10, 9, 11, 6, 1);
  test17adddelrange(10, 9, 11, 7, 1);
  test17adddelrange(10, 9, 11, 8, 1);

  test17adddelrange(10, 10, 11, 0, 0);
  test17adddelrange(10, 10, 11, 1, 0);
  test17adddelrange(10, 10, 11, 2, 0);
  test17adddelrange(10, 10, 11, 3, 1);
  test17adddelrange(10, 10, 11, 4, 1);
  test17adddelrange(10, 10, 11, 5, 1);
  test17adddelrange(10, 10, 11, 6, 1);
  test17adddelrange(10, 10, 11, 7, 1);
  test17adddelrange(10, 10, 11, 8, 1);

  test17adddelrange(10, 10, 12, 0, 0);
  test17adddelrange(10, 10, 12, 1, 0);
  test17adddelrange(10, 10, 12, 2, 0);
  test17adddelrange(10, 10, 12, 3, 1);
  test17adddelrange(10, 10, 12, 4, 1);
  test17adddelrange(10, 10, 12, 5, 1);
  test17adddelrange(10, 10, 12, 6, 1);
  test17adddelrange(10, 10, 12, 7, 1);
  test17adddelrange(10, 10, 12, 8, 1);
  
  test17adddelrange(10, 9, 10, 0, 0);
  test17adddelrange(10, 9, 10, 1, 1);
  test17adddelrange(10, 9, 10, 2, 1);
  test17adddelrange(10, 9, 10, 3, 0);
  test17adddelrange(10, 9, 10, 4, 1);
  test17adddelrange(10, 9, 10, 5, 1);
  test17adddelrange(10, 9, 10, 6, 0);
  test17adddelrange(10, 9, 10, 7, 1);
  test17adddelrange(10, 9, 10, 8, 1);

  test17adddelrange(10, 8, 10, 0, 0);
  test17adddelrange(10, 8, 10, 1, 1);
  test17adddelrange(10, 8, 10, 2, 1);
  test17adddelrange(10, 8, 10, 3, 0);
  test17adddelrange(10, 8, 10, 4, 1);
  test17adddelrange(10, 8, 10, 5, 1);
  test17adddelrange(10, 8, 10, 6, 0);
  test17adddelrange(10, 8, 10, 7, 1);
  test17adddelrange(10, 8, 10, 8, 1);

  // delrange delrange conflicts
  test17delranges(3, 4, 3, 4, 0, 0, 1);
  test17delranges(3, 4, 3, 4, 0, 1, 1);
  test17delranges(3, 4, 3, 4, 0, 2, 1);
  test17delranges(3, 4, 3, 4, 0, 3, 1);
  test17delranges(3, 4, 3, 4, 0, 4, 1);
  test17delranges(3, 4, 3, 4, 0, 5, 1);
  test17delranges(3, 4, 3, 4, 0, 6, 1);
  test17delranges(3, 4, 3, 4, 0, 7, 1);
  test17delranges(3, 4, 3, 4, 0, 8, 1);

  test17delranges(3, 4, 3, 4, 1, 0, 1);
  test17delranges(3, 4, 3, 4, 1, 1, 1);
  test17delranges(3, 4, 3, 4, 1, 2, 1);
  test17delranges(3, 4, 3, 4, 1, 3, 1);
  test17delranges(3, 4, 3, 4, 1, 4, 1);
  test17delranges(3, 4, 3, 4, 1, 5, 1);
  test17delranges(3, 4, 3, 4, 1, 6, 1);
  test17delranges(3, 4, 3, 4, 1, 7, 1);
  test17delranges(3, 4, 3, 4, 1, 8, 1);

  test17delranges(3, 4, 3, 4, 2, 0, 1);
  test17delranges(3, 4, 3, 4, 2, 1, 1);
  test17delranges(3, 4, 3, 4, 2, 2, 1);
  test17delranges(3, 4, 3, 4, 2, 3, 1);
  test17delranges(3, 4, 3, 4, 2, 4, 1);
  test17delranges(3, 4, 3, 4, 2, 5, 1);
  test17delranges(3, 4, 3, 4, 2, 6, 1);
  test17delranges(3, 4, 3, 4, 2, 7, 1);
  test17delranges(3, 4, 3, 4, 2, 8, 1);

  test17delranges(3, 4, 3, 4, 3, 0, 1);
  test17delranges(3, 4, 3, 4, 3, 1, 1);
  test17delranges(3, 4, 3, 4, 3, 2, 1);
  test17delranges(3, 4, 3, 4, 3, 3, 1);
  test17delranges(3, 4, 3, 4, 3, 4, 1);
  test17delranges(3, 4, 3, 4, 3, 5, 1);
  test17delranges(3, 4, 3, 4, 3, 6, 1);
  test17delranges(3, 4, 3, 4, 3, 7, 1);
  test17delranges(3, 4, 3, 4, 3, 8, 1);

  test17delranges(3, 4, 3, 4, 4, 0, 1);
  test17delranges(3, 4, 3, 4, 4, 1, 1);
  test17delranges(3, 4, 3, 4, 4, 2, 1);
  test17delranges(3, 4, 3, 4, 4, 3, 1);
  test17delranges(3, 4, 3, 4, 4, 4, 1);
  test17delranges(3, 4, 3, 4, 4, 5, 1);
  test17delranges(3, 4, 3, 4, 4, 6, 1);
  test17delranges(3, 4, 3, 4, 4, 7, 1);
  test17delranges(3, 4, 3, 4, 4, 8, 1);

  test17delranges(3, 4, 3, 4, 5, 0, 1);
  test17delranges(3, 4, 3, 4, 5, 1, 1);
  test17delranges(3, 4, 3, 4, 5, 2, 1);
  test17delranges(3, 4, 3, 4, 5, 3, 1);
  test17delranges(3, 4, 3, 4, 5, 4, 1);
  test17delranges(3, 4, 3, 4, 5, 5, 1);
  test17delranges(3, 4, 3, 4, 5, 6, 1);
  test17delranges(3, 4, 3, 4, 5, 7, 1);
  test17delranges(3, 4, 3, 4, 5, 8, 1);

  test17delranges(3, 4, 3, 4, 6, 0, 1);
  test17delranges(3, 4, 3, 4, 6, 1, 1);
  test17delranges(3, 4, 3, 4, 6, 2, 1);
  test17delranges(3, 4, 3, 4, 6, 3, 1);
  test17delranges(3, 4, 3, 4, 6, 4, 1);
  test17delranges(3, 4, 3, 4, 6, 5, 1);
  test17delranges(3, 4, 3, 4, 6, 6, 1);
  test17delranges(3, 4, 3, 4, 6, 7, 1);
  test17delranges(3, 4, 3, 4, 6, 8, 1);

  test17delranges(3, 4, 3, 4, 7, 0, 1);
  test17delranges(3, 4, 3, 4, 7, 1, 1);
  test17delranges(3, 4, 3, 4, 7, 2, 1);
  test17delranges(3, 4, 3, 4, 7, 3, 1);
  test17delranges(3, 4, 3, 4, 7, 4, 1);
  test17delranges(3, 4, 3, 4, 7, 5, 1);
  test17delranges(3, 4, 3, 4, 7, 6, 1);
  test17delranges(3, 4, 3, 4, 7, 7, 1);
  test17delranges(3, 4, 3, 4, 7, 8, 1);

  test17delranges(3, 4, 3, 4, 8, 0, 1);
  test17delranges(3, 4, 3, 4, 8, 1, 1);
  test17delranges(3, 4, 3, 4, 8, 2, 1);
  test17delranges(3, 4, 3, 4, 8, 3, 1);
  test17delranges(3, 4, 3, 4, 8, 4, 1);
  test17delranges(3, 4, 3, 4, 8, 5, 1);
  test17delranges(3, 4, 3, 4, 8, 6, 1);
  test17delranges(3, 4, 3, 4, 8, 7, 1);
  test17delranges(3, 4, 3, 4, 8, 8, 1);
  
  // read read non-conflict
  coid.oid = 1;
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.vget(coid, buf); assert(res==0);
  res = t2.vget(coid, buf); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.vget(coid, buf); assert(res==0);
  res = t2.vget(coid, buf); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res==0);

  // read write non-conflict
  coid.oid = 1;
  i=0;
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.vget(coid, buf); assert(res==0);
  res = t2.put(coid, (char*)&i, 4); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.vget(coid, buf); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.vget(coid, buf); assert(res==0);
  res = t2.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.vget(coid, buf); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res==0);

  // write write conflict
  coid.oid = 1;
  i=0;
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.put(coid, (char*)&i, 4); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res!=0);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res!=0);

  // write writeSupervalue conflict
  coid.oid = 1;
  i=0;
  SetAttrs(&sv);
  SetIntCells(&sv, 5);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.writeSuperValue(coid, &sv); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res!=0);
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.writeSuperValue(coid, &sv); assert(res==0);
  res = t2.put(coid, (char*)&i, 4); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  res = t2.tryCommit(); assert(res!=0);

  // repopulate oid1 with regular value
  t1.start();
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.writeSuperValue(coid, &sv); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res!=0);
  
  // repopulate oid1 with regular value
  t1.start();
  res = t1.put(coid, (char*)&i, 4); assert(res==0);
  res = t1.tryCommit(); assert(res==0);
  
  t1.start();
  res = t1.vget(coid,buf); assert(res==0);
  t2.start();
  res = t2.vget(coid,buf); assert(res==0);
  res = t1.writeSuperValue(coid, &sv); assert(res==0);
  res = t2.put(coid, (char*)&i, 4); assert(res==0);
  res = t2.tryCommit(); assert(res==0);
  res = t1.tryCommit(); assert(res!=0);
}  

// test18: consistent client cache
#define TEST18_NREADS  5000000
#define TEST18_NWRITES 5

COid test18_coid;

OSTHREAD_FUNC test18thread(void *parm){
  int i, res, v, lastv;
  u64 start, end;
  u32 result;
  int threadno;
  Ptr<Valbuf> buf;
  TRANSACTION_VAR(t);
  float aveop = 0.0;

#ifndef LOCAL_TRANSACTION  
  initThreadContext("test18", false);
#endif
  
  threadno = (int)(long long) parm;

  v = lastv = 0;

  if (threadno == 0){ // reader
    start = Time::now();
    for (i=0; i < TEST18_NREADS; ++i){
      t.start();
      res = t.vget(test18_coid, buf); assert(res==0);
      assert(buf.isset());
      v = *(int*)buf->u.buf;
      if (v != lastv && v != lastv+1)
        printf("  Warning: v=%d lastv=%d\n", v, lastv);
      assert(v >= lastv);
      lastv = v;
    }
    end = Time::now();
    aveop = (double) (end-start) / i;
  } else { // writer
    start = Time::now();
    for (i=0; i < TEST18_NWRITES; ++i){
      t.start();
      t.write(test18_coid, (char *) &v, sizeof(int));
      res = t.tryCommit(); assert(res==0);
      ++v;
    }
    end = Time::now();
    aveop = (double) (end-start) / i;
  }
  
  result = *(u32*)&aveop;
  return (void*)(long long)result;
}

void test18(){
  OSThread_t threads[2];
  int res;
  void *resRead, *resWrite;
  float latRead, latWrite;
  int v;

  // write initial value
  test18_coid.cid = getCidTable(18, 0); // pick a cachable coid
  test18_coid.oid = 0;
  TRANSACTION_VAR(t);
  t.start();
  v=0;
  t.write(test18_coid, (char*) &v, sizeof(int));
  res = t.tryCommit();
  assert(res==0);
  
  res = OSCreateThread(&threads[0], test18thread, (void*) 0); assert(res==0);
  res = OSCreateThread(&threads[1], test18thread, (void*) 1); assert(res==0);
  OSWaitThread(threads[0], &resRead);
  OSWaitThread(threads[1], &resWrite);
  latRead = *(float*) &resRead;
  latWrite = *(float*) &resWrite;
  printf("  Lat read: %f\n", (double) latRead);
  printf("  Lat write: %f\n", (double) latWrite);
  assert(latWrite > 500.00 && latRead < 0.05);
}

// test19: subtransactions
void test19(){
  int i, j, k, res;
  COid coid;
  Ptr<Valbuf> buf;
  
  coid.cid = 19;

  TRANSACTION_VAR(t);

  // test values
  // abort -----------------------
  coid.oid = 0;

  // start, startsub, put, abortsub
  for (k=0; k < 8; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k%1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    t.startSubtrans(1);
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k%2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.abortSubtrans(0);
    if (k%4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==15);
  }

  // start, put, startsub, put, abortsub
  for (k=0; k < 4; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0); // version with vget at beginning
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    i=14;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    t.startSubtrans(1);
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    t.abortSubtrans(0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==14);
  }


  // release ---------------------
  // start, startsub, put, releasesub
  for (k=0; k < 8; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    t.startSubtrans(1);
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.releaseSubtrans(0);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==16);
  }    

  // start, put, startsub, put, releasesub
  for (k=0; k < 8; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0); // version with vget at beginning
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    i=14;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    t.startSubtrans(1);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0); // version with vget at beginning
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    i=17;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    t.releaseSubtrans(0);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==17);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==17);
  }

  // abort with two levels -----------------------
  // start, startsub, put, startsub, put, abortsub, abortsub
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    t.startSubtrans(1);
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.startSubtrans(2);
    i=17;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==17);
    }
    t.abortSubtrans(1);
    if (k&8){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.abortSubtrans(0);
    if (k&16){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==15);
  }
  
  // start, startsub, put, startsub, put, abortsub0
  for (k=0; k < 16; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    t.startSubtrans(1);
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.startSubtrans(2);
    i=17;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==17);
    }
    t.abortSubtrans(0);
    if (k&8){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==15);
  }


  // start, put, startsub, put, startsub, put, abortsub, abortsub
  for (k=0; k < 256; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    i=14;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    
    t.startSubtrans(1);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&8){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.startSubtrans(2);
    if (k&16){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    i=18;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&32){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==18);
    }
    
    t.abortSubtrans(1);
    if (k&64){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.abortSubtrans(0);
    if (k&128){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==14);
  }

  // start, put, startsub, put, startsub, put, abortsub0
  for (k=0; k < 128; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    i=14;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    
    t.startSubtrans(1);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&8){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.startSubtrans(2);
    if (k&16){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    i=18;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&32){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==18);
    }
    
    t.abortSubtrans(0);
    if (k&64){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==14);
  }
  
  // release with two levels -----------------------
  // start, startsub, put, startsub, put, [releasesub], [releasesub]
  for (k=0; k < 512; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    t.startSubtrans(1);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    t.startSubtrans(2);
    if (k&8){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    i=17;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&16){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==17);
    }
    if (k&32) t.releaseSubtrans(1);
    if (k&64){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==17);
    }
    if (k&128) t.releaseSubtrans(0);
    if (k&256){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==17);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==17);
  }

  // start, put, startsub, put, startsub, put, [releasesub], [releasesub]
  for (k=0; k < 1024; ++k){
    // setup
    t.start();
    i=15;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==15);
    }
    i=14;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&2){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==14);
    }
    i=16;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&8){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    
    t.startSubtrans(2);
    if (k&16){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==16);
    }
    i=18;
    res = t.put(coid, (char*)&i, 4); assert(res==0);
    if (k&32){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==18);
    }
    if (k&64) t.releaseSubtrans(1);
    if (k&128){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==18);
    }
    if (k&256) t.releaseSubtrans(0);
    if (k&512){
      res = t.vget(coid, buf); assert(res==0);
      assert(buf->type == 0); assert(buf->len == 4);
      j = *(int*)buf->u.buf;
      assert(j==18);
    }
    res = t.tryCommit(); assert(res==0);
    // check
    t.start();
    res = t.vget(coid, buf); assert(res==0);
    assert(buf->type == 0); assert(buf->len == 4);
    j = *(int*)buf->u.buf;
    assert(j==18);
  }

    
  // test supervalues  
  SuperValue sv;
  ListCell c1,c2, c3, c4, c8, c10, c12;
  
  c1 = SetIntCell(1);
  c2 = SetIntCell(2);
  c3 = SetIntCell(3);
  c4 = SetIntCell(4);
  c8 = SetIntCell(8);
  c10 = SetIntCell(10);
  c12 = SetIntCell(12);
  coid.oid = 1;

  // check that listadd, listdelrange, attrset get aborted or released correctly
  
  // listadd #1
  static int vals0[] = { 0,1,2,3,4 };
  static int vals1[] = { 0,1,2,3,4,8,10 };
  static int vals2[] = { 0,1,2,3,4,8 };

  // start, add, startsub, add, abortsub
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.listAdd(coid, &c10, 0, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // start, startsub, add, startsub, add, abortsub
  for (k=0; k < 64; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.listAdd(coid, &c10, 0, 0); assert(res==0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    t.abortSubtrans(1);
    if (k&32){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // listadd #5b
  static int vals3[] = { 0,1,2,3,4,8,10,12 };
  // start, add, startsub, add, startsub, add, abortsub0
  for (k=0; k < 128; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.listAdd(coid, &c10, 0, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    res = t.listAdd(coid, &c12, 0, 0); assert(res==0);
    if (k&32){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals3, sizeof(vals3)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&64){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // listadd with release
  // start, add, startsub, add, release
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.listAdd(coid, &c10, 0, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    t.releaseSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // start, startsub, add, startsub, add, [release], [release]
  for (k=0; k < 512; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.listAdd(coid, &c10, 0, 0); assert(res==0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    if (k&32) t.releaseSubtrans(1);
    if (k&64){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    if (k&128) t.releaseSubtrans(0);
    if (k&256){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // start, add, startsub, add, startsub, add, [release], [release]
  for (k=0; k < 1024; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.listAdd(coid, &c10, 0, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals1, sizeof(vals1)/sizeof(int));
    }
    res = t.listAdd(coid, &c12, 0, 0); assert(res==0);
    if (k&32){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals3, sizeof(vals3)/sizeof(int));
    }
    if (k&64) t.releaseSubtrans(1);
    if (k&128){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals3, sizeof(vals3)/sizeof(int));
    }
    if (k&256) t.releaseSubtrans(0);
    if (k&512){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals3, sizeof(vals3)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals3, sizeof(vals3)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // --------------------------------------------------- listdelrange

  // listDelRange #1
  static int vals4[] = { 0,4 };
  static int vals5[] = { 0,3,4 };

  // start, del, startsub, del, abort
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c1, &c2, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c2, &c3, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // start, startsub, del, startsub, del, abort
  for (k=0; k < 64; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c1, &c2, 0); assert(res==0);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c2, &c3, 0); assert(res==0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    t.abortSubtrans(1);
    if (k&32){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  static int vals6[] = { 0 };
  // start, del, startsub, del, startsub, del, abort0
  for (k=0; k < 128; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c1, &c2, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c2, &c3, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c3, &c4, 0); assert(res==0);
    if (k&32){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals6, sizeof(vals6)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&64){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // listDelRange with release
  // start, del, startsub, del, release
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c1, &c2, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c2, &c3, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    t.releaseSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }
  
  // start, startsub, del, startsub, del, [release], [release]
  for (k=0; k < 512; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c1, &c2, 0); assert(res==0);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c2, &c3, 0); assert(res==0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    if (k&32) t.releaseSubtrans(1);
    if (k&64){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    if (k&128) t.releaseSubtrans(0);
    if (k&256){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // start, del, startsub, del, startsub, del, [release], [release]
  for (k=0; k < 1024; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c1, &c2, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals5, sizeof(vals5)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c2, &c3, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    t.startSubtrans(2);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals4, sizeof(vals4)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c3, &c4, 0); assert(res==0);
    if (k&32){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals6, sizeof(vals6)/sizeof(int));
    }
    if (k&64) t.releaseSubtrans(1);
    if (k&128){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals6, sizeof(vals6)/sizeof(int));
    }
    if (k&256) t.releaseSubtrans(0);
    if (k&512){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals6, sizeof(vals6)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals6, sizeof(vals6)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }
  
    
  // check that listadd and listdelrange work together in different levels
  static int vals7[] = { 0,1,2,3,12 };
  static int vals8[] = { 0,1,2,3,4,8,12 };

  // start, add, startsub, del, abortsub
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    res = t.listAdd(coid, &c12, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals8, sizeof(vals8)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals8, sizeof(vals8)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c4, &c10, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals7, sizeof(vals7)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals8, sizeof(vals8)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals8, sizeof(vals8)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // now with release
  // start, add, startsub, del, releasesub
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    res = t.listAdd(coid, &c12, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals8, sizeof(vals8)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals8, sizeof(vals8)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c4, &c10, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals7, sizeof(vals7)/sizeof(int));
    }
    t.releaseSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals7, sizeof(vals7)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals7, sizeof(vals7)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }
  
    
  // invert listadd and listdelrange above
  static int vals9[] = { 0,1,2,3 };
  static int vals10[] = { 0,1,2,3,8,12 };

  // start, del, startsub, add, abortsub
  for (k=0; k < 32; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals0, sizeof(vals0)/sizeof(int));
    }
    res = t.listDelRange(coid, 4, &c4, &c10, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    }
    t.startSubtrans(1);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    }
    
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    res = t.listAdd(coid, &c12, 0, 0); assert(res==0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals10, sizeof(vals10)/sizeof(int));
    }
    
    t.abortSubtrans(0);
    if (k&16){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }

  // check that writesupervalue works
  //   writesupervalue at 0
  //   listadd at 1
  //   abort 0
  //   check that supervalue is there
  for (k=0; k < 16; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 4);
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    }
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    }
    t.startSubtrans(1);
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    res = t.listAdd(coid, &c12, 0, 0); assert(res==0);
    if (k&4){
      res = t.vsuperget(coid, buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals10, sizeof(vals10)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }
  
  //   listadd at 0
  //   writesupervalue at 1
  //   abort 0
  //   check that listadd is there
  for (k=0; k < 16; ++k){
    // setup
    t.start();
    SetAttrs(&sv);
    SetIntCells(&sv, 5);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    res = t.tryCommit(); assert(res==0);
    // test
    t.start();
    if (k&1){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    }
    res = t.listAdd(coid, &c8, 0, 0); assert(res==0);
    if (k&2){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    t.startSubtrans(1);
    SetAttrs(&sv);
    SetIntCells(&sv, 4);
    res = t.writeSuperValue(coid, &sv); assert(res==0);
    if (k&4){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals9, sizeof(vals9)/sizeof(int));
    }
    t.abortSubtrans(0);
    if (k&8){
      res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
      CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    }
    res = t.tryCommit(); assert(res == 0);
    // check
    t.start();
    res = t.vsuperget(coid,buf, 0, 0); assert(res==0);
    CheckIntCellsList(buf->u.raw, vals2, sizeof(vals2)/sizeof(int));
    res = t.tryCommit(); assert(res == 0);
  }
}  

class HostConfig;
extern StorageServerState *S;

int main(void){
  srand((unsigned)time(0)); // seed PRNG
  UniqueId::init();
#ifndef LOCAL_TRANSACTION  
  tinitScheduler(0);
  SC = new StorageConfig(CONFIGFILENAME);
#else
  void initStorageServer(HostConfig *hc);
  initStorageServer(0);
  S->cLogInMemory.setSingleVersion(false); // some tests require multiple
                                           // versions
#endif  
  KI = createki();

#if DTREE_SPLIT_LOCATION == 2
  printf("These tests do not work with DTREE_SPLIT_LOCATION 2,"
         "set it to 1 in options.h\n");
  exit(1);
#endif

  printf("Test1\n");
  test1();
  printf("Test2\n");
  test2();
  printf("Test3\n");
  test3();
  printf("Test4\n");
  test4();
  printf("Test5\n");
  test5();
  printf("Test6\n");
  test6();
  printf("Test7\n");
  test7();
  printf("Test7b\n");
  test7b();
  printf("Test8\n");
  test8();
  printf("Test9\n");
  test9();
  printf("Test10\n");
  test10();
  printf("Test11\n");
  test11();
  printf("Test12\n");
  test12();
  printf("Test13\n");
  test13();
  printf("Test14\n");
  test14();
  printf("Test15\n");
  test15();
  printf("Test16\n");
  test16();
  printf("Test17\n");
  test17();
#ifndef LOCAL_TRANSACTION // skip test18 for local library
  printf("Test18\n");
  test18();
#else
  printf("Test18: skipped (caching irrelevant for local library)\n");
#endif
  printf("Test19\n");
  test19();
  printf("All tests done\n");
  return 0;
}
