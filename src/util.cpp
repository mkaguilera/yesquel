//
// util.cpp
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


#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <map>
#include <list>

#include "tmalloc.h"
#include "debug.h"
#include "util.h"

static void print16(unsigned char *ptr, int n)
{ int i;
  if (n > 16) n = 16;
  for (i = 0; i < n; ++i){
    printf("%02x", *ptr++);
    if (i % 8 == 7)  putchar('-');
    else putchar(' ');
  }
  ptr -= n;
  for (;i<16;i++) printf("   ");
  printf("  ");
  for (i = 0; i < n; i++)
    if (isprint((u8)*ptr)) putchar(*ptr++);
    else { ++ptr; putchar('.'); }
  putchar('\n'); }

// Pretty printer for binary data
void DumpData(char *ptr, int i, int firstoff){
  while (i>0){
    printf("%04x:  ", firstoff);
    print16((unsigned char*)ptr, i);
    ptr += 16;
    i -= 16;
    firstoff += 16;
  }
}

void DumpDataShort(char *ptr, int n){
  if (!ptr) return;
  for (int i=0; i < n; ++i){
    if (isprint((u8)*ptr)) putchar(*ptr++);
    else if (!*ptr){ ++ptr; putchar('0'); }
    else { ++ptr; putchar('.'); }
  }
}

#ifdef GETOPT
// Parse command-line options
int optind=1;  // current argv being processed
int opterr=1;  // whether to print error messages
int optopt;    // returned last processed option character
char *optarg;  // returned option argument
int getopt(int argc, char **argv, const char *options){
  static char *contopt=0; // point from where to continue processing options
                          // in the same argv
  const char *ptr;

  if (!contopt){
    if (optind >= argc || *argv[optind] != '-') return -1;
    if (argv[optind][1]=='-'){ ++optind; return -1; }
  }
  optopt = contopt ? *contopt : argv[optind][1];
  ptr=strchr(options,optopt);
  if (!ptr){
    if (opterr) fprintf(stderr, "Unrecognized option %c\n", optopt);
    // advance
    if (contopt && contopt[1]) ++contopt;
    else if (!contopt && argv[optind][2]) contopt=&argv[optind][2];
    else { contopt=0; ++optind; }
    return '?';
  }
  if (ptr[1]==':'){ // expecting argument
    if (contopt && contopt[1]){
      optarg=contopt+1;
      contopt=0;
      ++optind;
      return optopt;
    }
    contopt=0;
    if (++optind == argc){
      optarg=0; 
      if (opterr) fprintf(stderr, "Missing argument for option %c\n", optopt);
      return ':';
    }
    optarg=argv[optind++];
  }
  else {
    // no argument
    if (contopt && contopt[1]) ++contopt;
    else if (!contopt && argv[optind][2]) contopt=&argv[optind][2];
    else { contopt=0; ++optind; }
  }
  return optopt;
}
#endif

