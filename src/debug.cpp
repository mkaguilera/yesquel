//
// debug.cpp
//
// Macros for reporting debug information
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
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdarg.h>
#include <fcntl.h>
#include <math.h>
#include <map>
#include <list>

#include "debug.h"

int DebugLevel = 0;
void SetDebugLevel(int dl){ DebugLevel = dl; }

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
int DebugHandle=0;

void DebugInit(bool uselogfile, char *logfile){
  if (uselogfile){
    if (!logfile){
      logfile = getenv(GAIADEBUG_ENV_VAR);
      if (!logfile) logfile = (char*) GAIADEBUG_DEFAULT_FILE;
    }

    DebugHandle = creat(logfile, 0755);
  } else DebugHandle = 0;
}

void DebugUninit(){
  if (DebugHandle) close(DebugHandle);
}

void DebugPrintf(int level, char *format, ...){
  char msg[1024];
  va_list vl;
  va_start(vl, format); 

  vsnprintf(msg, sizeof(msg)-2, format, vl);
  msg[sizeof(msg)-2] = 0;
  if (DebugHandle)
    strcat(msg, "\r\n");
  else strcat(msg, "\n");
  if (DebugHandle)
    write(DebugHandle, msg, (int)strlen(msg));
  else 
    fputs(msg, stdout);
  va_end(vl);
}
#endif
