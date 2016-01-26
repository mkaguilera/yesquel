//
// tmalloc.h
//
// Thread-local memory allocator.
// To use, include this file and link tmalloc.cpp to your application.
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

/*
   Each thread keeps its own pool of memory. To allocate a buffer,
   a thread gets memory from its pool.  If the same thread who
   allocated the buffer later frees the buffer, the buffer is returned
   to the local pool. If, however, a different thread frees the
   buffer, the buffer should not be returned to the thread's local
   pool, otherwise memory from one thread's pool starts to move to
   another thread's pool. In some applications where some threads
   allocate and produce data while others consume a free data, this
   would be a problem as memory would continually flow from one thread
   to another, eventually exhausting the pool of the first thread.
  
   To address this problem, if a thread frees a buffer allocated by
   another thread, it will send back the buffer to the other thread so
   that the other thread can return the buffer to its local pool. To
   send back buffers efficiently, a thread accumulates a bunch of
   buffers into a superbuffer of buffers, and once the superbuffer is
   large enough, it sends the entire superbuffer. The reason for doing
   this is to avoid too much thread synchronization.
  
   When a thread allocates memory, it checks to see if any
   superbuffers are being returned to it. If so, it returns all the
   buffers in the superbuffers to the local pool.
  
   To send superbuffers between threads, there is a concurrent linked
   list per thread.  The list stores the superbuffers to be received
   by the thread. Another thread adds a superbuffer to the link list
   of the first thread by creating a new node pointing to the current
   hand, and then doing a compare-and-swap on the current head to
   change it to the new head. If the compare-and-swap fails, someone
   else managed to modify the head, so the process is repeated. The
   thread will consume the superbuffers in the link list, skipping the
   first entry. Doing so allows the thread to consume the superbuffers
   without changing the head pointer, and hence without contending
   with threads trying to add new elements. The side effect of this is
   that the superbuffer at the head will be consumed only after a new
   superbuffer is added.
  
   The local pool is itself a set of subpools, where each subpool
   keeps buffers of a fixed size. The sizes of the subpools grow
   exponentially. A new buffer is allocated from the subpool holding
   the smallest buffers that will fit the requested size.
*/

void *_tmalloc(size_t size);
void _tfree(void *buf);
void *_trealloc(void *ptr, size_t size);
size_t _tgetsize(void *buf);
#define malloc _tmalloc
#define free _tfree
#define realloc _trealloc
