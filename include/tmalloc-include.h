//
// tmalloc-include.h
//
// Include file used internally by tmalloc.cpp
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

#ifndef _TMALLOC_INCLUDE_H
#define _TMALLOC_INCLUDE_H
#define _NO_TMALLOC

#include "os.h"
#include "inttypes.h"
#include "datastruct.h"
#include <assert.h>

#ifdef malloc
#undef malloc
#endif
#ifdef free
#undef free
#endif

//#define BYPASS_THREADALLOCATOR   // if defined, use malloc() instead of
                                      // thread allocator
#define VARALLOC_FIRSTPOOL           5 // log2 of size of elements in first pool
#define VARALLOC_NPOOLS             26 // number of pools. Each successive
                                       // pool has elements with twice the
                                       // size of the previous pool
#define _TM_SUPERBUFFER_SIZE        64 // how many buffers get batched in a
                                       // superbuffer before sending to owner
                                       // for GC
#define _TM_DESTMAP_HASHTABLE_SIZE 512 // for each thread, hash table mapping
                                       // destination threads to their partially
                                       // filled superbuffer
#ifndef NDEBUG
//#define _TM_FILLBUFFERS              // if defined, fill allocated and
//                                     // freed buffers with special marker
#endif
#define _TM_FILLALLOC             0xca // special marker used to fill allocated
                                       // buffers if _TM_FILLBUFFERS is set
#define _TM_FILLFREE              0xcf // special marker used to fill freed
                                       // buffers if _TM_FILLBUFFERS is set
#define PADBEFOREMAGIC "ALLC"          // fixed at 4 bytes (see PadBefore)
#define PADAFTERMAGIC "ENDAENDA"       // fixed at 8 bytes (see PadAfter)


void *_tmalloc(size_t size);
void _tfree(void *buf);

// fixed-size memory allocator
class FixedAllocator {
public:
  struct PadBefore {  // block of padding before each buffer
    PadBefore *next; // linked list of free blocks
    u64 allocated; // -1LL if block is free, otherwise requested size
    u64 tag;       //
    u32 status;   // used by _tmalloc and _tfree. 0=free, 1=waiting, 2=allocated
    char magic[4]; // magic string to test against overwriting
  };
  struct PadAfter {
    char magic[8]; // magic string to test against overwriting
  };

private:
  int Size;       // user requested size
  int Realsize;   // size including padding before and after
  int IncGrow;    // incremental number of units to grow when no more units
                  // available
  int NAllocated; // number of units allocated
  u64 Tag;        // tag to be added at each allocated block

  PadBefore *FreeUnitsHead;  // dummy head of linked list of free units
  PadBefore *FreeUnitsTail;  // dummy tail of linked list of free units
  RWLock FreeUnits_lock;

  void grow(int inc); // must be called with FreeUnits_lock held
  void addPadding(void *tmp); // Add padding information

  void checkPadding(void *tmp, bool alloc); // Check padding information.
              //   buf: pointer to buffer
              //   alloc: whether allocated field should be != -1
              //   newalloc: new value to set allocated field
public:
  // size: size of allocation units
  // startpool: initial number of allocation units
  // incgrow: incremental number of units to grow when no more units available
  // tag: information added to each allocated block. Can be obtained by calling
  // getTag on the allocated block
  FixedAllocator(int size, int startpool, int incgrow, u64 tag=0);
  ~FixedAllocator(); // destructor does not free pool of allocated chunks
  static size_t getSize(void *buf); // returns size of allocated block
                                    // (requested size in myalloc)
  int getNAllocated(void){ return NAllocated; }
  void grow(void){ grow(IncGrow); }
  void *alloc(u64 reqsize=-1LL); // allocate new buffer. buffer will have fixed
                                 // size. reqsize is written into header for
                                 // bookkeeping purposes only (not used).
  void free(void *buf); // free buffer
  static u64 getTag(void *ptr);
  static void setStatus(void *buf, u32 status);
  static u32 getStatus(void *buf);
};

// fixed-size memory allocator without locking
class FixedAllocatorNolock {
public:
  struct PadBefore {  // block of padding before each buffer
    PadBefore *next; // linked list of free blocks
    u64 allocated; // 0 if block is free, otherwise requested size
    u64 tag;       //
    u32 status;
    char magic[4]; // magic string to test against overwriting
  };
  struct PadAfter {
    char magic[8]; // magic string to test against overwriting
  };

private:
  int Size;       // user requested size
  int Realsize;   // size including padding before and after
  int IncGrow;    // incremental number of units to grow when no more units
                  // available
  int NAllocated; // number of units allocated
  u64 Tag;        // tag to be added at each allocated block
  void *(*PageAllocFunc)(size_t);
  unsigned PageSize;

  PadBefore *FreeUnitsHead;  // dummy head of linked list of free units
  PadBefore *FreeUnitsTail;  // dummy tail of linked list of free units

  void grow(int inc);
  void addPadding(void *tmp); // Add padding information
  void checkPadding(void *tmp, bool alloc); // Check padding information.
             //   buf: pointer to buffer
             //   alloc: whether buffer is supposed to be allocated
 
public:
  // size: size of allocation units
  // startpool: initial number of allocation units
  // incgrow: incremental number of units to grow when no more units available
  // tag: information added to each allocated block. Can be obtained by calling
  //      getTag on the allocated block
  // pagesize: if non-zero, internally allocates memory in increments of
  //           pagesize. Will adjust incgrow to round up to the next pagesize.
  //           If zero, allocates memory according to incgrow parameter alone.
  //           In this case, note that size*incgrow is not exactly the amount
  //           of internally allocated memory since the allocator may add
  //           padding between the buffers returned to the user in alloc()
  // pageallocfunc: if set, function to be called to internally allocate pages
  FixedAllocatorNolock(int size, int startpool, int incgrow, u64 tag=0,
                       unsigned pagesize=0, void *(*pageallocfunc)(size_t)=0);
  ~FixedAllocatorNolock(); // destructor does not free pool of allocated chunks
  static size_t getSize(void *buf); // returns size of allocated block
                                    // (requested size in myalloc)
  static void setSize(void *buf, size_t newsize); // sets size of allocated
                                                  // block (must be <= Size)
  int getNAllocated(void){ return NAllocated; }
  void grow(void){ grow(IncGrow); }
  void *alloc(u64 reqsize=-1LL); // allocate new buffer. buffer will have
                                 // fixed size. 
                                 // reqsize is written into header for
                                 // bookkeeping purposes only (not used).
  void free(void *buf); // free buffer
  static u64 getTag(void *ptr);
  void checkBuf(void *tmp, bool alloc){
    checkPadding((void*) ((char*)tmp-sizeof(PadBefore)), alloc);
  }
  static void setStatus(void *buf, u32 status);
  static u32 getStatus(void *buf);
};


class VariableAllocatorNolock {
private:
  FixedAllocatorNolock *FixedPools;
  static int ceillog2(size_t n);
  void *(*PageAllocFunc)(size_t);
  unsigned PageSize;
public:
  // tag: tag that will be placed before every allocated block.
  //      Used to differentiate blocks allocated by different allocators.
  //      User can obtain the tag by calling getTag on the block.
  // pagesize: if non-zero, internally allocates memory in increments of
  //           pagesize. Will adjust incgrow to round up to the next pagesize.
  //           If zero, allocates memory according to incgrow parameter alone.
  //           In this case, note that size*incgrow is not exactly the amount
  //           of internally allocated memory since the allocator may add
  //           padding between the buffers returned to the user in alloc()
  // allocfunc: if set, function to be called to internally allocate pages
  VariableAllocatorNolock(u64 tag=0, unsigned pagesize=0,
                          void *(*pageallocfunc)(size_t)=0);
  ~VariableAllocatorNolock();
  void *alloc(size_t size);
  void free(void *ptr);
  static size_t getSize(void *buf){
    // returns size of allocated block (requested size in myalloc)
    return FixedAllocatorNolock::getSize(buf);
  }
  static void setSize(void *buf, size_t newsize){
    // returns size of allocated block; should be <= initially allocated size
    FixedAllocatorNolock::setSize(buf, newsize);
  }

  static u64 getTag(void *ptr);
  void checkBuf(void *buf, bool alloc); // Checks padding info. Alloc is
                                   // whether buffer is supposed to be allocated
  static void setStatus(void *buf, u32 status);
  static u32 getStatus(void *buf);
};


// Fixed allocator with multiple pools, to improve multi-thread performance.
// There are many pools. To allocate, pick a pool at random to allocate from.
// To free, pick a pool at random to return the buffer. This avoids lock
// contention if multiple threads are trying to allocate and deallocate
// simultaneously.
class FixedAllocatorMultipool {
private:
  int NPools;
  FixedAllocator *Pools;
  Align4 int NextPool;

public:
  FixedAllocatorMultipool(int size, int startpool, int incgrow, int npools);
  ~FixedAllocatorMultipool();
  void *alloc(void);   // allocate new buffer
  void free(void *buf);   // free buffer
};


// link list of super buffers
struct _TMLinkListNode {
  _TMLinkListNode *next;
  int nbufs;      // number of buffers in the super buffer
  void *bufs[1];  // buffers

  static _TMLinkListNode *_TMnewnode(int n); // allocate a new superbuffer
                                             // with space for n buffers
  static void _TMfreenode(_TMLinkListNode *node); // free a superbuffer
};

struct DestMapItem { // item in map from destthread to superbuffer
  void *destthread;             // destination thread
  _TMLinkListNode *superbuffer; // superbuffer being filled
  int pos;                      // next available position in superbuffer

  DestMapItem *next, *prev, *snext, *sprev;
  void *GetKey(){ return destthread; }
  static unsigned HashKey(void *k){ return (unsigned)(uint64_t) k; }
  static int CompareKey(void *l, void *r){
    if ((unsigned)(uint64_t)l < (unsigned)(uint64_t)r) return -1;
    else if ((unsigned)(uint64_t)l > (unsigned)(uint64_t)r) return +1;
    else return 0;
  }
  DestMapItem(void *dest);
  DestMapItem(){ destthread = 0; superbuffer = 0; pos = 0; }
};

class _TMOrigAllocator {
public:
  static void *alloc(size_t size){ return ::malloc(size); }
  static void free(void *ptr){ ::free(ptr); }
};


// thread-specific data
struct _TMThreadInfo {
  VariableAllocatorNolock allocator;
  _TMLinkListNode *headLinkList;   // head of list of superbuffers to be freed
  HashTable<void*,DestMapItem,_TMOrigAllocator> destMap;  // maps destination
                             // threads to their partially-filled superbuffers
  // add a node to the linklist
  void addNode(_TMLinkListNode *node);
  _TMThreadInfo();
};


#endif
