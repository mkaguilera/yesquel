//
// splitter-client.h
//
// Functions to be used by client-based splitter. Also includes
// functions for clients to obtain new rowid's.
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

#ifndef _SPLITTER_CLIENT_H
#define _SPLITTER_CLIENT_H

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <signal.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <float.h>

#include <map>
#include <list>
#include <set>
#include <signal.h>

#include "options.h"
#include "os.h"

#include "gaiatypes.h"
#include "newconfig.h"

#include "tcpdatagram.h"
#include "grpctcp.h"
#include "splitterrpcaux.h"

#define SS_NULL_RPCNO 0
#define SS_SHUTDOWN_RPCNO 1
#ifndef STORAGESERVER_SPLITTER
#define SS_GETROWID_RPCNO 2
#else
#define SS_GETROWID_RPCNO 16
#endif

i64 GetRowidFromServer(Cid cid, i64 hint); // get a fresh rowid for a given cid

struct SplitterStats {
  unsigned dest:1;              // 1 means stats refer to this thread, 0 for
                                // another thread. This is relevant when
                                // splitter reports a new status, so that the
                                // receiving thread knows whose coid was
                                // recently split
  unsigned splitTimeRetryingMs:31; // how many ms we have been retrying current
                                   // split (0 if current split is done,
                                   // <0 if this is to be ignored)
  u32 splitQueueSize;      // how many elements are queued to be split
  float splitTimeAvg;     // average time to split
  float splitTimeStddev;  // standard deviation time to split
  SplitterStats(){ splitQueueSize=0; splitTimeRetryingMs=0;
                   splitTimeAvg=0.0; splitTimeStddev=0.0; }
};

#define SPLITTER_THROTTLE_NMETRICS 3
class SplitterThrottle {
private:
  //RWLock lock; // lock protecting the fields below
  SplitterStats Load; // this variable is used for debugging only
  unsigned Nelements, Nbytes; // this variable is used for debugging only
  int Delays[SPLITTER_THROTTLE_NMETRICS];
  u64 Expirations[SPLITTER_THROTTLE_NMETRICS];

  // computes delay and expiration due to various metrics
  static int computeDelayFromQueue(SplitterStats &load, u64& expiration);
  static int computeDelayFromTimeRetrying(SplitterStats &load, u64& expiration);
  static int computeDelayFromNodesize(unsigned nelements, unsigned nbytes,
                                      u64& expiration);
  
public:
  SplitterThrottle();
  void ReportLoad(SplitterStats &newload);
  void ReportNodeSize(unsigned nelements, unsigned nbytes);
  int getCurrentDelay(void);
};

SplitterThrottle *ExtractThrottleFromServerSplitterState(void *sss);

#endif
