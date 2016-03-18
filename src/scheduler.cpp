//
// scheduler.cpp
//
// A class for scheduling events to be executed in the future.
// Event handlers should return quickly, otherwise other events will be delayed.
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
#include "scheduler.h"
#include "inttypes.h"

bool operator < (const Event& x, const Event& y){
  return x.when > y.when;
  // (when.dwHighDateTime > y.when.dwHighDateTime) ||
  // (when.dwHighDateTime == y.when.dwHighDateTime &&
  // when.dwLowDateTime > y.when.dwLowDateTime);
}

OSTHREAD_FUNC EventScheduler::executorThread(void *parm){
  EventScheduler *es = (EventScheduler *) parm;
  Event copyTopEvent;
  EventData *ed;
  int handlerres;

  while (!es->ForceStop){
    es->Events_lock.lock();
    u64 now = Time::now();

    // invoke all expired events, removing them from queue
    while (!es->Events.empty() &&
           (copyTopEvent = es->Events.top()).when <= now){
      do {
        ed = copyTopEvent.ed;
        es->Events.pop();
        es->Events_lock.unlock();

        handlerres = ed->handler(ed->data);  

        es->Events_lock.lock();
        if (ed->type==1 && handlerres==0){ // reschedule event
          copyTopEvent.when = Time::now() + ed->msFromNow;
          es->Events.push(copyTopEvent); // put back event with new time
        } else delete ed; // free space taken by event data
      } while (!es->Events.empty() &&
               (copyTopEvent = es->Events.top()).when <= now);

      now = Time::now();
    }

    // determine sleep time, and go to sleep
    u64 wakeup = es->Events.empty() ? INFINITE : copyTopEvent.when - now;
    es->Events_lock.unlock();
    //es->SemCondVar.unlockSleepLock(&es->Events_lock, (int) wakeup);
    es->Events_semaphore.wait((int)wakeup);
  }
  return 0;
}

// schedule an event to be called
void EventScheduler::AddEvent(EventHandler handler, void *data, int type,
                              int msFromNow){
  Event e;
  EventData *ed;

  assert(type >= 0 && type <= 1);

  ed = new EventData;

  e.when = Time::now() + msFromNow;
  e.ed = ed;
  ed->handler = handler;
  ed->type = type;
  ed->msFromNow = msFromNow;
  ed->data = data;

  Events_lock.lock();
  Events.push(e);
  if (Events.top().when == e.when)
    // if this event is at the top (smallest when time), wake up
    // thread otherwise it will sleep until the next event
    Events_semaphore.signal();
  Events_lock.unlock();
}

void EventScheduler::launch(void){
  int res;
  if (!Launched){
    Launched = true;
    res = OSCreateThread(&SchedulerThread, executorThread, (void*) this);
    assert(res==0);
  }
}

void EventScheduler::stop(void){
  ForceStop = true;
  if (Launched){
  //SemCondVar.wakeAll();
    Events_semaphore.signal();
    pthread_join(SchedulerThread, 0);
    Launched = false;
  }
}
