//
// splitter-server.h
//
// Server RPCs and stubs for splitting and returning rowids
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

#ifndef _SPLITTER_SERVER_H
#define _SPLITTER_SERVER_H

#include "gaiarpcaux.h"
#include "splitterrpcaux.h"

#ifndef STORAGESERVER_SPLITTER
#define SPLITTER_PORT_OFFSET 10  // port of splitter is port of storageserver
                                 // plus SPLITTER_PORT_OFFSET
#else
#define SPLITTER_PORT_OFFSET 0  // port of splitter is same port of
                                // storageserver
#endif

#define SPLITTER_STAT_MOVING_AVE_WINDOW 30 // window size for moving average of split time

void InitServerSplitter();
void UninitServerSplitter();

int ss_nullRpcStub(RPCTaskInfo *rti);
int ss_shutdownRpcStub(RPCTaskInfo *rti);
int ss_splitnodeRpcStub(RPCTaskInfo *rti);
int ss_getrowidRpcStub(RPCTaskInfo *rti);

Marshallable *ss_nullRpc(NullRPCData *d);
Marshallable *ss_shutdownRpc(ShutdownRPCData *d);
Marshallable *ss_splitnodeRpc(SplitnodeRPCData *d);
Marshallable *ss_getrowidRpc(GetRowidRPCData *d);

#endif
