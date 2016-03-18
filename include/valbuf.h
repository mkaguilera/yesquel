//
// valbuf.h
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

#ifndef _VALBUF_H
#define _VALBUF_H

#include <string.h>
#include "inttypes.h"
#include "datastruct.h"
#include "gaiatypes.h"

class SuperValue;

class Valbuf {
private:
  friend class Ptr<Valbuf>;
  Align4 int refcount;
public:
  int type;           // 0=value, 1=supervalue
  COid coid;
  bool immutable;
  Timestamp commitTs; // when value/supervalue was written; can be invalid for
                      // nodes in writeset of a transaction
  Timestamp readTs;   // when value/supervalue was read (readTs >= commitTs);
                      // can be invalid for nodes in writeset of a transaction
                      // There is a guarantee that the node was not written
                      // after commitTs and before readTs.
  int len;
  union {
    char *buf;        // if type=0. Must be allocated with
                      // Transaction::allocReadBuf() since it will be freed with
                      // Transaction::readFreeBuf()
    SuperValue *raw;  // if type=1
  } u;
  Valbuf();
  Valbuf(const Valbuf& c);
  Valbuf(const SuperValue& sv, COid c, bool im, Timestamp *ts);
  ~Valbuf();
};

#endif
