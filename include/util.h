//
// util.h
//
// General utility classes
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


#ifndef _UTIL_H
#define _UTIL_H

#include <time.h>
#include "prng.h"
#include "os.h"
#include "inttypes.h"

// Pretty printer for binary data
void DumpData(char *ptr, int i, int firstoff=0);

// prints a buffer in a short format
void DumpDataShort(char *ptr, int n);

#ifdef GETOPT
// command-line argument processing
extern int optind;  // current argv being processed
extern int opterr;  // whether to print error messages
extern int optopt;    // returned last processed option character
extern char *optarg;  // returned option argument
int getopt(int argc, char **argv, const char *options);
#endif

class Time {
public:
  static void init(void){}

  // gets clock resolution in nanoseconds
  static u64 getres(void){
    struct timespec ts;
    int res = clock_getres(CLOCK_REALTIME, &ts); assert(res==0);
    return (u64) ts.tv_sec * 1000000000L + (u64) ts.tv_nsec;
  }
  
  // return time in microseconds
  static u64 nowus(void){
    struct timespec ts;
    int res = clock_gettime(CLOCK_REALTIME, &ts); assert(res==0);
    return (u64) ts.tv_sec * 1000000 + (u64) ts.tv_nsec / 1000;
  }

  // return time in milliseconds
  static u64 now(void){
    return nowus()/1000;
  }
};

#endif
