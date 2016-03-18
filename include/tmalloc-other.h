//
// tmalloc-other.h
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

class PinnedPtr {
private:
  void *base;
  u32 tag;
  u32 size;
public:
  PinnedPtr(void *b, u32 t, u32 s){ base = b; tag = t; size = s; }
  void *getbuf(){ return base; }
  u32 gettag(){ return tag; }
  u32 getsize(){ return size; }
};

// fixed allocator for pinned fixed-length blocks of memory
class FixedAllocatorPinned {
public:
private:
  int Size;       // user requested size
  int IncGrow;    // incremental number of units to grow when no more units
                  // available
  int NAllocated; // number of units allocated
  int NAvailable;
  u32 Tag;        // tag to be added at each allocated block
  StackArray<void*> Items; // stack of allocated items
  StackArray<void*> Regions; // stack of allocated regions

  void grow();
  void *allocRegion(); // allocate a pinned region of size Size  
  void freeRegion(void *ptr); // free a region allocated with allocRaw

public:
  // size: size of allocation units; should be page size
  // incgrow: initial and incremental number of units to grow when no more
  //          units available
  // tag: information added to each allocated block. Can be obtained by calling
  //      getTag on the allocated block
  FixedAllocatorPinned(int size, int incgrow, u32 tag=0);
  ~FixedAllocatorPinned(); // destructor does not free pool of allocated chunks
  PinnedPtr alloc(); // allocate new buffer. buffer will have fixed size. 
  void free(PinnedPtr pinptr); // free buffer
  static size_t getSize(PinnedPtr pinptr){
    return pinptr.getsize();
  }
  static u32 getTag(PinnedPtr pinptr){
    return pinptr.gettag();
  }
};
