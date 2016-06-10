//
// clientlib-local.h
//
// Library for client to access a local emulation of a remote server.
// This is used to keep temporary tables created by the SQL processor.
// These tables are created and later destroyed. They are not shared and
// need not be persisted, so it does not make sense to keep them in
// the storage server.
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

#ifndef _CLIENTLIBLOCAL_H
#define _CLIENTLIBLOCAL_H

#include "debug.h"
#include "util.h"
#include "clientlib-common.h"
#include "gaiarpcaux.h"
#include "datastruct.h"
#include "supervalue.h"

#include <sys/uio.h>
#include <set>
#include <list>

// Transaction running locally at a client
class LocalTransaction
{
private:
  int State;          // 0=valid, -1=aborted, -2=aborted due to I/O error
  Timestamp StartTs;
  Tid Id;
  int readsTxCached;
  bool hasWrites;
  int currlevel;          // current subtransaction level

  TxCache txCache;

  int tryLocalRead(COid &coid, Ptr<Valbuf> &buf, int typ);
  int auxprepare(Timestamp &chosents);
  int auxcommit(int outcome, Timestamp committs);
  int auxsubtrans(int level, int action);

public:
  LocalTransaction();
  ~LocalTransaction();

  // the methods below are as in class Transaction
  int start();
  int startDeferredTs(void);
  int write(COid coid, char *buf, int len);
  int writev(COid coid, int nbufs, iovec *bufs);
  int put(COid coid, char *buf, int len){ return write(coid, buf, len); }
  int put2(COid coid, char *data1, int len1, char *data2, int len2);
  int put3(COid coid, char *data1, int len1, char *data2, int len2,
           char *data3, int len3);
  int vget(COid coid, Ptr<Valbuf> &buf);
  int vsuperget(COid coid, Ptr<Valbuf> &buf, ListCell *cell,
                Ptr<RcKeyInfo> prki);
  static void readFreeBuf(char *buf);
  static char *allocReadBuf(int len);
  int addset(COid coid, COid toadd);
  int remset(COid coid, COid toremove);
  int writeSuperValue(COid coid, SuperValue *svret);
#if DTREE_SPLIT_LOCATION != 1
  int listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki, int flags);
#else
  // when split occurs at client, listadd returns ncells and size
  int listAdd(COid coid, ListCell *cell, Ptr<RcKeyInfo> prki, int flags,
              int *ncells=0, int *size=0);
#endif
  int listDelRange(COid coid, u8 intervalType, ListCell *cell1,
                   ListCell *cell2, Ptr<RcKeyInfo> prki);
  int attrSet(COid coid, u32 attrid, u64 attrvalue);
  int startSubtrans(int level);
  int abortSubtrans(int level);
  int releaseSubtrans(int level);
  int tryCommit(Timestamp *retcommitts=0);
  int abort(void);
};

#endif
