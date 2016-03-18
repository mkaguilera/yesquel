//
// storageserver-rpc.h
//
// Stubs for RPCs at the storage server
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

#ifndef _STORAGESERVER_RPC_H
#define _STORAGESERVER_RPC_H

#include "tcpdatagram.h"
#include "grpctcp.h"
#include "task.h"


// stubs
int nullRpcStub(RPCTaskInfo *rti);
int getstatusRpcStub(RPCTaskInfo *rti);
int writeRpcStub(RPCTaskInfo *rti);
int readRpcStub(RPCTaskInfo *rti);
int fullwriteRpcStub(RPCTaskInfo *rti);
int fullreadRpcStub(RPCTaskInfo *rti);
int listaddRpcStub(RPCTaskInfo *rti);
int listdelrangeRpcStub(RPCTaskInfo *rti);
int attrsetRpcStub(RPCTaskInfo *rti);
int prepareRpcStub(RPCTaskInfo *rti);
int commitRpcStub(RPCTaskInfo *rti);
int subtransRpcStub(RPCTaskInfo *rti);
int shutdownRpcStub(RPCTaskInfo *rti);
int startsplitterRpcStub(RPCTaskInfo *rti);
int flushfileRpcStub(RPCTaskInfo *rti);
int loadfileRpcStub(RPCTaskInfo *rti);
#endif
