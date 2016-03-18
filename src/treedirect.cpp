//
// treedirect.cpp
//
// Functions for a client to access the distributed B-tree directly
// without going through SQlite.
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
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>

#include "options.h"
#include "debug.h"
#include "os.h"
#include "tmalloc.h"
#include "treedirect.h"

// forward definitions
int DtMovetoaux(BtCursor *pCur, const void *pKey, i64 nKey, int bias,
                int *pRes, bool tryDirect);
int sqlite3BtreeCreateTableChooseTable(Btree *p, Pgno *piTable, int flags);

int DdInit(){
  return sqlite3_initialize();
  //InitYesql(); // not necessary to call this since sqlite3_open() already
                 // calls it
}

void DdUninit(){
  UninitYesql();
}

int DdInitConnection(const char *dbname, DdConnection *&conn){
  int res;
  conn = new DdConnection;
  conn->dbname = new char[strlen(dbname)+1];
  strcpy(conn->dbname, dbname);
  res=sqlite3_open(dbname, &conn->db); 
  if (res) 
    return res;
  res=sqlite3BtreeOpen(dbname, conn->db, &conn->pBtree, 0, 0); 
  if (res) 
    return res;
  return 0;
}

int DdCloseConnection(DdConnection *conn){
  sqlite3BtreeClose(conn->pBtree);
  return sqlite3_close(conn->db);
}

int DdStartTx(DdConnection *conn){
  int res;
  res=sqlite3BtreeBeginTrans(conn->pBtree, 1);
  if (res){ dprintf(1, "sqlite3BtreeBeginTrans fails: %d\n", res); return -1; }
  return 0;
}

int DdRollbackTx(DdConnection *conn){
  int res;
  res = sqlite3BtreeRollback(conn->pBtree);
  if (res){ dprintf(1, "sqlite3BtreeRollback fails: %d\n", res); return -1; }
  return 0;
}

int DdCommitTx(DdConnection *conn){
  int res;
  res = sqlite3BtreeCommit(conn->pBtree);
  if (res){ dprintf(1, "sqlite3BtreeCommit fails: %d\n", res); return -1; }
  return 0;
}

int DdOpenTable(DdConnection *conn, u64 iTable, DdTable *&table){
  table = new DdTable;
  table->conn = conn;
  table->pCur = 0;
  table->iTable = iTable;
  return 0;
}

int DdCreateTable(DdConnection *conn, u64 iTable, DdTable *&table){
  int res;
  DdStartTx(conn);
  res = sqlite3BtreeCreateTableChooseTable(conn->pBtree, &iTable, BTREE_INTKEY);
  if (res) return res;
  res = DdCommitTx(conn); if (res) return res;
  return DdOpenTable(conn, iTable, table);
}

int DdCloseCursor(DdTable *table){
  int res=0;
  if (table->pCur){
    res = sqlite3BtreeCloseCursor(table->pCur);
    free(table->pCur);
    table->pCur=0;
  }
  return res;
}

void DdCloseTable(DdTable *table){
  if (table->pCur) DdCloseCursor(table);
}

int DdInitCursor(DdTable *table){
  int res;
  if (table->pCur) DdCloseCursor(table);
  table->pCur = (BtCursor*) malloc(sqlite3BtreeCursorSize());
  assert(table->pCur);
  sqlite3BtreeCursorZero(table->pCur);
  res=sqlite3BtreeCursor(table->conn->pBtree,
                  table->iTable & ~0xffff800000000000LL, 1, 0, table->pCur);
  if (res){ dprintf(1, "sqlite3BtreeCursor fails: %d\n", res); return -1; }
  return 0;
}

int DdInsert(DdTable *table, i64 key, const char *value, int valuelen){
  int res;
  DdInitCursor(table);
  res = sqlite3BtreeInsert(table->pCur, 0, key, (void*) value, valuelen, 0, 0,
                           0);
  if (res){
    dprintf(1, "sqlite3BtreeInsert fails: %d\n", res);
    DdCloseCursor(table);
    return res;
  }
  DdCloseCursor(table);
  return 0;
}


int DdDelete(DdTable *table, i64 key){
  int res;
  int pres;

  DdInitCursor(table);
  res = DtMovetoaux(table->pCur, 0, key, 0, &pres, false);
  if (res){
    dprintf(1, "DtMovetoaux fails: %d pres %d\n", res, pres);
    DdCloseCursor(table);
    return res;
  }
  if (pres != 0){
    dprintf(1, "Did not find element %lld\n", (long long)key);
    DdCloseCursor(table);
    return res;
  }
  if (table->pCur->eState != CURSOR_VALID){
    dprintf(1, "BAD CURSOR %d\n", table->pCur->eState);
    DdCloseCursor(table);
    return SQLITE_IOERR;
  }
    //ptr = (char*)sqlite3BtreeKeyFetch(pCur, &amt);
  res = sqlite3BtreeDelete(table->pCur);
  if (res){
    dprintf(1, "sqlite3BtreeDelete fails: %d\n", res);
    DdCloseCursor(table);
    return res;
  }
  DdCloseCursor(table);
  return 0;
}

int DdLookup(DdTable *table, i64 key, char *buf, int buflen, int *valuelen,
             bool tryDirect){
  int pres;
  char *ptr;
  int res;
  int len;

  DdInitCursor(table);
  res = DtMovetoaux(table->pCur, 0, key, 0, &pres, tryDirect);
  if (res){
    dprintf(1, "DtMovetoaux fails: %d pres %d", res, pres);
    DdCloseCursor(table);
    return res;
  }
  if (pres != 0){
    dprintf(1, "Lookup did not find element; pres %d", pres);
    DdCloseCursor(table);
    *valuelen=0;
    return 0;
  }
  if (table->pCur->eState != CURSOR_VALID &&
      table->pCur->eState != CURSOR_DIRECT){
    dprintf(1, "Lookup did not find element: bad cursor");
    DdCloseCursor(table);
    *valuelen=0;
    return 0;
  }

  //ptr = (char*)sqlite3BtreeKeyFetch(pCur, &amt);
  ptr = (char*) sqlite3BtreeDataFetch(table->pCur, &len);
  len = len < buflen ? len : buflen;
  memcpy(buf, ptr, len);
  *valuelen = len;
  DdCloseCursor(table);
  return res;
}

// gets oid of leaf node storing a given key
Oid DdGetOid(DdTable *table, i64 key){
  int pres;
  int res;
  BtCursor *pCur;
  Oid retval;

  DdInitCursor(table);
  res = DtMovetoaux(table->pCur, 0, key, 0, &pres, false);
  if (res){
    dprintf(1, "DtMovetoaux fails: %d pres %d", res, pres);
    retval = 0;
  } else {
    pCur = table->pCur;
    retval = pCur->node[pCur->levelLeaf].NodeOid();
  }
  DdCloseCursor(table);
  return retval;
}

int DdUpdate(DdTable *table, i64 key, char *buf, int buflen,
             int (*callback)(char *buf, int len, void *arg), void *arg){
  int pres;
  char *ptr;
  int res;
  int len;

  DdInitCursor(table);
  res = DtMovetoaux(table->pCur, 0, key, 0, &pres, true); // Change last
                      // argument to false/true to disable/enable direct seeks
  if (res){
    dprintf(1, "DtMovetoaux fails: %d pres %d", res, pres);
    DdCloseCursor(table);
    return res;
  }
  if (pres != 0){
    dprintf(1, "Lookup did not find element; pres %d", pres);
    DdCloseCursor(table);
    return 0;
  }
  if (table->pCur->eState != CURSOR_VALID &&
      table->pCur->eState != CURSOR_DIRECT){
    dprintf(1, "Lookup did not find element: bad cursor");
    DdCloseCursor(table);
    return 0;
  }

  //ptr = (char*)sqlite3BtreeKeyFetch(pCur, &amt);
  ptr = (char*) sqlite3BtreeDataFetch(table->pCur, &len);
  len = len < buflen ? len : buflen;
  memcpy(buf, ptr, len);

  len = callback(buf, len, arg); // invoke callback to possibly modify buffer

  res = sqlite3BtreeInsert(table->pCur, 0, key, buf, len, 0, 0, 0);
  
  DdCloseCursor(table);
  return res;
}

#define BUFLEN 32768

int DdScan(DdTable *table, i64 key, int nelems,
  void (*callback)(i64 key, const char *data, int len, int n, bool eof,
                   void *callbackparm),
           void *callbackparm, bool fetchdata){
  int pres;
  char *ptr;
  int res;
  int len;
  char buf[BUFLEN];
  i64 k;
  int i=0;

  if (nelems <= 0) return 0;
  DdInitCursor(table);
  res = DtMovetoaux(table->pCur, 0, key, 0, &pres, false);
  if (res){
    dprintf(1, "DtMovetoaux fails: %d pres %d", res, pres);
    DdCloseCursor(table);
    return res;
  }
  if (table->pCur->eState != CURSOR_VALID){ // did not find anything
    dprintf(1, "DtMovetoaux did not find an element >= given one");
    if (callback)
      callback(0, 0, 0, -1, true, callbackparm);
    DdCloseCursor(table);
    return 0;
  }

  i=0;
  do {
    sqlite3BtreeKeySize(table->pCur, &k);
    if (fetchdata){
      ptr = (char*) sqlite3BtreeDataFetch(table->pCur, &len);
      len = len < BUFLEN ? len : BUFLEN;
      memcpy(buf, ptr, len);
    } else {
      len = 0; // do not fetch data
    }
    if (callback)
      callback(k, buf, len, i, false, callbackparm);
    sqlite3BtreeNext(table->pCur, &pres);
    ++i;
  } while (pres==0 && i < nelems);
  if (i < nelems){ // did not get all asked elements
    if (callback)
      callback(0, 0, 0, -1, true, callbackparm);
  }
  DdCloseCursor(table);
  return 0;
}
