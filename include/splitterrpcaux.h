//
// splitterrpcaux.h
//
// RPC definitions for client to access splitter functionality at server,
// as well as the RPC to obtain a rowid.
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

#ifndef _SPLITTERRPCAUX_H
#define _SPLITTERRPCAUX_H

#include <sys/uio.h>
#include "ipmisc.h"
#include "gaiatypes.h"

// ---------------------------------- SPLITNODE RPC ----------------------------------
// RPC to split a node

struct SplitnodeRPCParm {
  int getstatusonly; // 0=perform split, 1=do not split anything,
                     // just return splitter status
  COid coid;         // coid to split. This parameter is meaningful only if
                     // getstatusonly==0.
  int  wait;         // 0=return immediately, 1=wait for split to finish
                     // before returning
                     //  This parameter is meaningful only if getstatusonly==0
};

class SplitnodeRPCData : public Marshallable {
public:
  SplitnodeRPCParm *data;
  int freedata;
  SplitnodeRPCData()  { freedata = 0; }
  ~SplitnodeRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(SplitnodeRPCParm);
    return 1;
  }
  void demarshall(char *buf){
     data = (SplitnodeRPCParm*) buf;
  }
};

struct SplitterLoadStatus {
  SplitterLoadStatus(){ splitQueueSize=0; splitTimeAvg = splitTimeStddev = 0.0;
                        splitTimeRetryingMs=0; }
  int splitQueueSize;      // how many elements are queued to be split
  double splitTimeAvg;     // average time to split
  double splitTimeStddev;  // standard deviation time to split
  u64 splitTimeRetryingMs; // how many ms we have been retrying current split
                           // (0 if current split is done)
};

struct SplitnodeRPCResp {
  int status;
  COid coid;               // ciod of node for which a split request was made
  SplitterLoadStatus load; // current load of splitter
  int haspending;          // true if there are pending split requests
};

class SplitnodeRPCRespData : public Marshallable {
public:
  SplitnodeRPCResp *data;
  int freedata;
  SplitnodeRPCRespData(){ freedata = 0; }
  ~SplitnodeRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(SplitnodeRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (SplitnodeRPCResp*) buf; }
};

// ------------------------------- GETROWID RPC --------------------------------
// RPC to get a fresh rowid

struct GetRowidRPCParm {
  Cid cid;  // cid to get rowid of
  i64 hint; // hint of possible rowid
};

class GetRowidRPCData : public Marshallable {
public:
  GetRowidRPCParm *data;
  int freedata;
  GetRowidRPCData()  { freedata = 0; }
  ~GetRowidRPCData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(GetRowidRPCParm);
    return 1;
  }
  void demarshall(char *buf){
     data = (GetRowidRPCParm*) buf;
  }
};

struct GetRowidRPCResp {
  i64 rowid;
};

class GetRowidRPCRespData : public Marshallable {
public:
  GetRowidRPCResp *data;
  int freedata;
  GetRowidRPCRespData(){ freedata = 0; }
  ~GetRowidRPCRespData(){ if (freedata){ delete data; } }
  int marshall(iovec *bufs, int maxbufs){
    assert(maxbufs >= 1);
    bufs[0].iov_base = (char*) data;
    bufs[0].iov_len = sizeof(GetRowidRPCResp);
    return 1;
  }
  void demarshall(char *buf){ data = (GetRowidRPCResp*) buf; }
};

#endif
