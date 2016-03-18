//
// warning.cpp
//
// A class for outputing warnings to stdout.
// It batches together warnings with the same number, so that
//   if multiple such warnings occur in a short period, only
//   the first is reported, followed by a count of how many
//   repetitions there were.
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


#ifndef NDEBUG

#include "util.h"
#include "warning.h"
#include "task.h"

bool Warning::hasinit = false;
PastWarnings Warning::WarningList[MAX_WARNINGS];
int Warning::MaxWarning=0;

void Warning::ImmediateFuncWarning(TaskMsgData &msgdata, TaskScheduler *ts,
                                   int srcthread){
  TaskMsgDataWarning *tmdw = (TaskMsgDataWarning*) &msgdata;
  PastWarnings *w;
  u64 now;
  bool show;

  if (!hasinit) return;

  assert(tmdw->number < MAX_WARNINGS);
  now = Time::now();
  w = WarningList+tmdw->number;
  if (MaxWarning < tmdw->number) MaxWarning = tmdw->number;
  show = w->n == 0 && now > w->first + COALESCE_INTERVAL_MS;
  if (!show) ++w->n; 
  else {
    w->first = now;
    w->n = 0;
  }
  if (show){
    fprintf(WARNING_FILE, "Warning %d: %s\n", tmdw->number, tmdw->msg);
    //fflush(WARNING_FILE);
  }
  free(tmdw->msg);
}

void Warning::init(){
  int i;
  TaskScheduler *ts;

  if (!hasinit){
    MaxWarning=0;
    for (i=0; i < MAX_WARNINGS; ++i){
      WarningList[i].n=0;
      WarningList[i].first=0;
    }
    hasinit = true;
    ts = tgetTaskScheduler();
    // assign immediate function
    ts->assignImmediateFunc(IMMEDIATEFUNC_WARNING, ImmediateFuncWarning);
    // add event to periodically flush repeated warnings
    TaskEventScheduler::AddEvent(tgetThreadNo(), flushWarnings, 0, 1,
                                 WARNING_FLUSHER_PERIOD);
    // set tclass for warnings
    gContext.setNThreads(TCLASS_WARNING, 1);
    gContext.setThread(TCLASS_WARNING, 0, tgetThreadNo());
  }
}

int Warning::flushWarnings(void *parm){
  int i;
  PastWarnings *w;
  u64 now;

  if (!hasinit) return 0;
  now = Time::now();
  for (i=0; i <= (int)MaxWarning; ++i){
    w = WarningList+i;
    if (w->n > 0 && now > w->first + COALESCE_INTERVAL_MS){
      fprintf(WARNING_FILE, "Warning %d repeated %d more times\n", i, w->n);
      //fflush(WARNING_FILE);
      w->n = 0;
      w->first = 0;
    }
  }
  return 0;
}

void Warning::log(int level, unsigned number, const char *format, ...){
  TaskMsg msg;
  TaskMsgDataWarning *tmdw = (TaskMsgDataWarning *) &msg.data;
  va_list vl;
  va_start(vl, format);

  assert(sizeof(TaskMsgDataWarning) <= TASKSCHEDULER_TASKMSGDATA_SIZE);
  if (!hasinit) return;

  tmdw->level = level;
  tmdw->number = number;
  tmdw->msg = (char*) malloc(MAX_WARNING_LENGTH);
  vsnprintf(tmdw->msg, MAX_WARNING_LENGTH, format, vl);
  tmdw->msg[MAX_WARNING_LENGTH-1] = 0;
  msg.dest = TASKID_CREATE(gContext.getThread(TCLASS_WARNING, 0),
                           IMMEDIATEFUNC_WARNING);
  msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;

  tgetTaskScheduler()->sendMessage(msg);
  va_end(vl);  
}

#endif
