//
// gaiatypes.cpp
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

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <map>
#include <list>

#include "tmalloc.h"
#include "debug.h"

#include "gaiatypes.h"
#include "ipmisc.h"
#include "util.h"


// ------------------------------------ Tid  ---------------------------------
Align4 u32 Tid::count=0;

void Tid::setNew(void){
  u32 c = AtomicInc32(&count);
  d1 = UniqueId::getUniqueId(); // first 64 bits: unique id for this
                                // process (IP+PID)
  d2 = ( ((u64) (Time::now() / 1000) & 0xffffffffffffffff) << 32)
                                 // next 32 bits: seconds from clock
         | c;                    // final 32 bits: counter
}

// -------------------------------- Timestamp --------------------------------

Tlocal i64 Timestamp::advance = 0;// offset by which to correct local clock
Tlocal u32 Timestamp::count = 0;  // count of last returned timestamp
Tlocal u64 Timestamp::lastus = 0; // ms of last returned timestamp
Tlocal u32 Timestamp::countoverflow = 0; // for debugging purposes

#define B48LL   0x0000ffffffffffffLL // lowest 48 bits
#define B16     0x0000ffff           // lowest 16 bits
#define TSMAGIC 0xbeec000000000000LL // magic string at beginning of timestamp,
                                     // to help debugging

void Timestamp::setNew(void){
  u64 us = (Time::nowus()+advance) & B48LL;
  if (us != lastus){ // reset count if time advanced
    count = 0;
    lastus = us;
  } else {
    ++count; // otherwise increment count
    if (count & B16<<16){ // count has overflown
      ++countoverflow;  // for debugging purposes
      ++us;
      count = 0;
      lastus = 0;
    }
  }
  d[0] = TSMAGIC | us;
  d[1] = (u64)count<<48 | (UniqueId::getUniqueId() & B48LL);
}

void Timestamp::setOld(i64 ms){
  setNew();
  d[0] -= 1000*ms;
}

// set timestamp to one of the lowest possible timestamps
// (not necessarily "the" lowest, because we want to keep timestamps unique)
void Timestamp::setLowest(void){
  d[0] = TSMAGIC;
  d[1] = UniqueId::getUniqueId() & B48LL;
}

// set timestamp to one of the largest possible timestamps
// (not necessarily "the" lowest, because we want to keep timestamps unique)
void Timestamp::setHighest(void){
  d[0] = TSMAGIC | B48LL;
  d[1] = (u64)B16<<48 | (UniqueId::getUniqueId() & B48LL);
}

int Timestamp::age(void){
  i64 t = (Time::nowus()+advance) & B48LL;
  return (int)(t - (d[0] & B48LL))/1000;
}

int Timestamp::ageus(void){
  i64 t = (Time::nowus()+advance) & B48LL;
  return (int)(t - (d[0] & B48LL));
}

void Timestamp::addEpsilon(){
  u32 c = (d[1] >> 48)+1; // bump count in timestamp
  if (c & (B16<<16)){ // count has overflown
    printf("**!**CLOCK counter overflow\n");
    ++countoverflow; // for debugging purposes
    c = 0;
    ++d[0];   // increment us
  }
  d[1] = (u64)c<<48 | (UniqueId::getUniqueId()&B48LL);
}

// check if ts is bigger than time now, and if so set advance appropriately
void Timestamp::catchup(Timestamp &ts){
  u64 tsus = (ts.d[0] & B48LL);
  u64 tnow = Time::nowus() & B48LL;
  i64 deltats = tsus - tnow;
  if (deltats >= advance){
    if (deltats > advance){
      assert(tsus > tnow+advance);
      advance = deltats;
    }
    count = (ts.d[1]>>48)+1; // ensures our next timestamp is higher than ts
    if (count & (B16<<16)){ // count has overflown
      ++countoverflow;
      count = 0;
      ++advance;
      lastus = tsus+1;
    } else lastus = tsus;
  }
}

// -------------------------------- UniqueId -----------------------------------

Tlocal u64 UniqueId::myid = 0;

//static
void UniqueId::init(u32 myip){
  if (myid) return;  // already initialized
  if (!myip) myip = IPMisc::getMyIP();
  u64 mytid = gettid();
  if ((mytid & ~0xffff)){
    printf("Error: thread id %lld has more than 16 bits. This will cause "
           "problems with unique ids, which reserve only 16 bits for the "
           "thread id. If two threads get the same unique id, data will get "
           "corrupted. Exiting.\n", (long long) mytid);
    exit(1);
  }
  myid = ((u64) myip)<<16 | (mytid & B16);
}



