//
// gaiatypes.h
//
// Types used in storage server and in the client library to access
// storage server.
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

#ifndef _GAIATYPES_H
#define _GAIATYPES_H

#include <assert.h>
#include "inttypes.h"
#include "os.h"

typedef u64 Cid; // container id type
typedef u64 Oid; // object id type

// container and object id
struct COid {
  Cid cid;
  Oid oid;
  static unsigned hash(const COid &c) {
    return (unsigned) ((u32)c.cid ^ (u32)(c.cid>>32) ^
                       (u32)c.oid ^ (u32)(c.oid>>32));
  }
  static int cmp(const COid &l, const COid &r) {
    if (l.cid < r.cid) return -1;
    if (l.cid > r.cid) return +1;
    // l.cid == r.cid
    if (l.oid < r.oid) return -1;
    if (l.oid > r.oid) return +1;
    return 0;
  }
  void setInvalid(){ cid=oid=0xffffffffffffffffULL; }
  bool isInvalid(){ return cid+1==0 && oid+1==0; }
};

struct Interval {
  int off;
  int len;
};


// returns a unique identifier for the current process
// consists of IP concatenated with PID
class UniqueId
{
private:
  static Tlocal u64 myid;
public:
  static void init(u32 myip=0);
  static u64 getUniqueId(void){
    if (!myid) init();
    return myid;
  }
};

// 128-bit Tid
class Tid {
private:
  static u32 count;
public:
  u64 d1, d2;
  void setNew(void);

  static int cmp(const Tid &l, const Tid &r){
    if (l.d1 < r.d1) return -1;
    if (l.d1 > r.d1) return +1;
    // l.d1 == r.d1
    if (l.d2 < r.d2) return -1;
    if (l.d2 > r.d2) return +1;
    return 0;
  }

  static unsigned hash(const Tid &l){
    return (unsigned)((u32)l.d1 ^ (u32)(l.d1>>32) ^
                      (u32)l.d2 ^ (u32)(l.d2>>32));
  }
};

// 128-bit timestamp
// Format for timestamps:
// [magic] [localclock] [count] [uniqueid]
// 16 bits   48 bits    16 bits  48 bits
class Timestamp { 
private:
  static Tlocal u32 count;
  static Tlocal i64 advance;
  static Tlocal u64 lastus;
  static Tlocal u32 countoverflow; // for debugging purposes. Number of times
                                   // count has overflown
  u64 d[2];
public:
  u64 getd1(void){ return d[0]; }
  u64 getd2(void){ return d[1]; }
  static u64 getadvance(){ return advance; } // for debugging purposes
  static u32 getcountoverflow(){ return countoverflow; } // for debugging
                                                         // purposes

  // set timestamp to a new fresh timestamp
  void setNew();

  // sets a timestamp in the past by ms milliseconds, if ms >= 0.
  // If ms < 0 then sets a timestamp in the future
  void setOld(i64 ms);

  // returns how old is the timestamp in ms. A negative value
  // means that the timestamp is in the future.
  int age(void);
  int ageus(void); // same as age() but in us

  // set timestamp to one of the lowest possible timestamp
  // (not necessarily "the" lowest, because we want to keep timestamps unique)
  // The illegal timestamp is actually the lowest timestamp
  void setLowest(void);
  void setHighest(void);

  // sets timestamp as an illegal timestamp. It is also the real lowest
  // timestamp.
  void setIllegal(void){ d[0] = d[1] = 0; }
  bool isIllegal(void){ return (d[0]==0 && d[1]==0); }
  static int cmp(const Timestamp &l, const Timestamp &r){
    if (l.d[0] < r.d[0]) return -1;
    if (l.d[0] > r.d[0]) return +1;
    // l.d[0] == r.d[0]
    if (l.d[1] < r.d[1]) return -1;
    if (l.d[1] > r.d[1]) return +1;
    return 0;
  }

  // adds time to the timstamp. The time is given in ms, and it can be negative
  void addMs(i64 ms){ d[0] = d[0] + 1000*ms; }

  // gets a timestamp a little bit bigger while maintaining the same unique
  // id part
  void addEpsilon();

  // check if ts is bigger than time now, and if so set advance appropriately
  static void catchup(Timestamp &ts);
};


#endif
