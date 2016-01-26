//
// valbuf.cpp
//
// In-memory structure to keep a value of a key-value pair. The value
// can be a raw buffer or a supervalue (which has further structure).
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
#include <malloc.h>
#include <signal.h>
#include <list>
#include <map>
#include <set>

#include "tmalloc.h"
#include "supervalue.h"
#include "clientlib.h"

static SuperValue tmpdummySV;

Valbuf::Valbuf(){ refcount=0; type=1; u.raw = &tmpdummySV; }

Valbuf::Valbuf(const Valbuf& c){
  memcpy(this, &c, sizeof(Valbuf));
  refcount = 0;
  switch(c.type){
  case 0:
    u.buf = Transaction::allocReadBuf(c.len);
    memcpy(u.buf, c.u.buf, c.len);
    break;
  case 1:
    u.raw = new SuperValue(*c.u.raw);
    break;
  }
}

Valbuf::Valbuf(const SuperValue& sv, COid c, bool im, Timestamp *ts){
  refcount=0;
  type = 1;
  coid = c;
  immutable = im;
  if (ts){ commitTs = *ts; readTs = *ts; }
  else { commitTs.setLowest(); readTs.setLowest(); }
  len = 0;
  u.raw = new SuperValue(sv);
}

Valbuf::~Valbuf(){
  switch(type){
  case 0: if (u.buf) Transaction::readFreeBuf(u.buf); 
    break;
  case 1: if (u.raw != &tmpdummySV) delete u.raw; 
    break;
  }
}
