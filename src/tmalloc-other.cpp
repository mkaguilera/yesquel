//
// tmalloc-other.cpp
//
// Other stuff not yet integrated into tmalloc.cpp
// The FixedAllocatorPinned uses a StackArray template,
// which in turns uses malloc, so it was not included
// in tmalloc for now.
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


#include "tmalloc-other.h"

void FixedAllocatorPinned::grow(){
  int i;
  void *ptr;
  NAvailable += IncGrow;
  ptr = allocRegion();
  for (i=0; i < IncGrow; ++i){
    Items.push(ptr);
    ptr = (void*)((char*) ptr + Size);
  }
}

// allocate a pinned memory region of size Size
void *FixedAllocatorPinned::allocRegion(){
  int res;
  void *ptr;
  ptr = VirtualAlloc(0, Size * IncGrow, MEM_COMMIT | MEM_RESERVE,
                     PAGE_READWRITE);
  assert(ptr);
  res = VirtualLock(ptr, Size * IncGrow);
  if (res==0){
    res = GetLastError();
    assert(0);
    exit(2);
  }
  return ptr;
}

// free a memory region allocated with allocRaw
void FixedAllocatorPinned::freeRegion(void *ptr){
  VirtualUnlock(ptr, Size * IncGrow);
  VirtualFree(ptr, Size * IncGrow, MEM_RELEASE);
}

// size: size of allocation units; should be page size
// startpool: initial number of allocation units
// incgrow: incremental number of units to grow when no more units available
// tag: information added to each allocated block. Can be obtained by calling
// getTag on the allocated block
FixedAllocatorPinned::FixedAllocatorPinned(int size, int incgrow, u32 tag) :
  Items(incgrow, 2.0),
  Regions(30, 2.0)
{
  Size = size;
  IncGrow = incgrow;
  NAllocated = 0;
  NAvailable = 0;
  Tag = tag;
  grow();
}

// destructor does not free pool of allocated chunks
FixedAllocatorPinned::~FixedAllocatorPinned(){
  void *ptr;
  while (!Regions.empty()){
    ptr = Regions.pop();
    freeRegion(ptr);
  }
}

// allocate new buffer. buffer will have fixed size. 
PinnedPtr FixedAllocatorPinned::alloc(){
  if (Items.empty()) grow();
  assert(!Items.empty());
  PinnedPtr retval(Items.pop(), Tag, Size);
  return retval;
}

// free buffer
void FixedAllocatorPinned::free(PinnedPtr pinptr){
  Items.push(pinptr.getbuf());
}
