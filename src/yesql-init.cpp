//
// yesquel-init.cpp
//
// Initialization functions for Yesquel
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

#include "options.h"

#include "clientdir.h"
#include "kvinterface.h"
#include "splitter-client.h"
#include "storageserver.h"
#include "debug.h"
#include "task.h"

StorageConfig *SC=0;

extern StorageConfig *InitGaia(void);
extern void UninitGaia(StorageConfig *SC);

extern void YesqlInitGlobals();

static void InitOnce(void){
#ifndef NOGAIA
  SC = InitGaia();  // initialize gaia (including reading config file)
#else
  SC = 0;
  tinitScheduler(0);
#endif

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
// initialize debug log
  DebugInit(false); // true=use log file, false=use stdout
  SetDebugLevel(0);
  //DebugInit(false); // true=use log file, false=use stdout
  //SetDebugLevel(2);
#endif

  KVInterfaceInit(); // initialize kvinterface
  initStorageServer(0); // initialize local key-value system
  YesqlInitGlobals(); // initialize globals for yesql
}

void InitYesql(void){
  static std::once_flag yesqlinit_flag;
  std::call_once(yesqlinit_flag, [](){ InitOnce(); });
  initThreadContext("yesqlclient", 0);
}

void UninitYesql(void){
  UninitGaia(SC);
}
