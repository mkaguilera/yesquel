//
// storageserver.h
//
// This is an include file for invoking the functions of the storage server
// directly.
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

#ifndef _STORAGESERVER_H
#define _STORAGESERVER_H

#include "gaiarpcaux.h"
#include "newconfig.h"

// must call before invoking any of the functions below
void initStorageServer(HostConfig *hc);

// remote procedures
Marshallable *nullRpc(NullRPCData *d);
Marshallable *getstatusRpc(GetStatusRPCData *d);
Marshallable *writeRpc(WriteRPCData *d);
Marshallable *readRpc(ReadRPCData *d, void *handle, bool &defer);
Marshallable *fullwriteRpc(FullWriteRPCData *d);
Marshallable *fullreadRpc(FullReadRPCData *d, void *handle, bool &defer);
Marshallable *listaddRpc(ListAddRPCData *d, void *&state);
Marshallable *listdelrangeRpc(ListDelRangeRPCData *d);
Marshallable *attrsetRpc(AttrSetRPCData *d);
Marshallable *prepareRpc(PrepareRPCData *d, void *&state, void *rpctasknotify);
Marshallable *commitRpc(CommitRPCData *d);
Marshallable *subtransRpc(SubtransRPCData *d);
Marshallable *shutdownRpc(ShutdownRPCData *d);
Marshallable *startsplitterRpc(StartSplitterRPCData *d);
Marshallable *flushfileRpc(FlushFileRPCData *d);
Marshallable *loadfileRpc(LoadFileRPCData *d);

// Auxilliary function to be used by server implementation
// Wake up a task that was deferred, by sending a wake-up message to it
void serverAuxWakeDeferred(void *handle);

// a function that schedules an exit to occur after a while (2 seconds)
void scheduleExit();


#endif
