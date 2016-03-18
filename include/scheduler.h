//
// scheduler.h
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

#ifndef _SCHEDULER_H
#define _SCHEDULER_H

#include "util.h"
#include <queue>

using namespace std;

typedef int (*EventHandler)(void *);

struct EventData {
  EventHandler handler; // handler
  int type;             // 0=once, 1=periodic
  int msFromNow;        // parameter passed by user of when to schedule
  void *data;           // data to pass to handler
};

class Event {
  public:
  u64 when;        // time of next call to handler
  EventData *ed;   // separating rest to make class small since it gets
                   // copied a lot
  Event(){ ed=0; }
};

bool operator < (const Event& x, const Event& y);

class EventScheduler {
private:
  priority_queue<Event> Events;
  RWLock Events_lock;
  Semaphore Events_semaphore;
  //CondVar SemCondVar;
  bool Launched;
  bool ForceStop;
  OSThread_t SchedulerThread;
  char *AppName;

  // the function that calls the events
  static OSTHREAD_FUNC executorThread(void *parm);

public:
  EventScheduler(char *appname){
    Launched = false;
    ForceStop = false;
    AppName = appname;
  }
  ~EventScheduler(){ stop(); }

  // Schedule an event to be called.
  // If type=0 then it is called once. Event handler return value is ignored.
  // If type=1 then it is called periodically as long as event handler
  //                returns 0.
  void AddEvent(EventHandler handler, void *data, int type, int msFromNow);

  // start executing events
  void launch(void);

  // stop scheduler
  void stop(void);
};

#endif /* _SCHEDULER_H */
 
