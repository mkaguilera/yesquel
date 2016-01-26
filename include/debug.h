//
// debug.h
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

#ifndef _DEBUG_H
#define _DEBUG_H

#include "options.h"

extern int DebugLevel;
void SetDebugLevel(int dl); // sets the debugging level

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
void DebugInit(bool uselogfile, char *logfile=0);
void DebugUninit();

extern void DebugPrintf(int level, char *format, ...);
#define dprintf(level,format,...) if (level <= DebugLevel){ DebugPrintf(level, (char*)("%llx " format), (long long) Time::now(), ##__VA_ARGS__); }
#define dshortprintf(level,format,...) if (level <= DebugLevel){ DebugPrintf(level, (char*)format, ##__VA_ARGS__); }
#define dputchar(level,c) if (level <= DebugLevel){ putchar(c); fflush(stdout); }
#else
#define dprintf(level,format,...)
#define dshortprintf(level,format,...)
#define dputchar(level,c)
#endif

// dshowchar is used to briefly report what is happening
//#define dshowchar(c) { putchar(c); fflush(stdout); }
#define dshowchar(c)

#endif
