//
// treedirect.h
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

#ifndef _DTREEDIRECT_H
#define _DTREEDIRECT_H

#define NODEFINE_SQLITE3_VERSION
#include "sqlite.h"
#include "dtreeaux.h"

#include "btreeInt.h"
#include "gaiatypes.h"
#include "util.h"
#include "datastruct.h"
#include "clientlib.h"
#include "clientdir.h"
#include "yesql-init.h"

struct DdConnection {
  char *dbname;
  sqlite3 *db;
  Btree *pBtree;
};

struct DdTable {
  DdConnection *conn;
  BtCursor *pCur;
  u64 iTable;
};

int DdInit(); // Initialize. Call once across all threads.
void DdUninit(); // Uninitialize. Call once across all threads.
int DdInitConnection(const char *dbname, DdConnection *&conn); // Initialize a
  // connection. Call once per thread. Returns err status (0=ok) and sets conn.
int DdCloseConnection(DdConnection *conn); // Close a connection.
int DdOpenTable(DdConnection *conn, u64 iTable, DdTable *&table); // Open a
  // table given connection and table number. Call once per thread. Returns
  // error status (0=ok) and sets table.
int DdCreateTable(DdConnection *conn, u64 iTable, DdTable *&table); // Create
  // and open a table given connection. Returns table number in iTable
  // and handle in table
void DdCloseTable(DdTable *table); // Close a table.
int DdStartTx(DdConnection *conn); // Start a transaction
int DdRollbackTx(DdConnection *conn); // Rollback a transaction
int DdCommitTx(DdConnection *conn); // Try to commit a transaction. Returns 0
  // if committed, -1 if aborted.
int DdInsert(DdTable *table, i64 key, const char *value, int valuelen);
  // Insert on a table. The key is key, value is value with length valuelen
Oid DdGetOid(DdTable *table, i64 key);
  // Get oid of leaf node storing a given key
int DdUpdate(DdTable *table, i64 key, char *buf, int buflen,
  int (*callback)(char *buf, int len, void *arg), void *arg);
  // Update key.
  // Read into buf with buflen, invokes callback to change buf, then write
  // back buf with length returned by callback
int DdDelete(DdTable *table, i64 key); // Delete key.
int DdLookup(DdTable *table, i64 key, char *buf, int buflen,
             int *valuelen, bool tryDirect=true);
  // Lookup key, writes value in *buf up to buflen characters, set *valuelen
  // to number of bytes written to buf. Returns error status (0=ok). Sets
  // *valuelen=0 if key not found.
int DdScan(DdTable *table, i64 key, int nelems,
  void (*callback)(i64 key, const char *data, int len, int n, bool eof,
  void *callbackparm), void *callbackparm, bool fetchdata=true); // Scan table
  // starting a first key <= given key for up to nelements.
  // For each found element, invoke callback with key, data, length of data,
  // a sequence number (starting with 0 up to elements-1), a flag indicating
  // whether there are no more elements (if this flag is set, data and len
  // will be set to 0), and a callback parameter given to DdScan().
  // Returns 0 if no error (reaching eof before nelems is not error),
  // non-zero otherwise.

#endif
