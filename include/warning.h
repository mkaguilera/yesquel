//
// warning.h
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

#ifndef _WARNING_H
#define _WARNING_H

#include <stdarg.h>
#include "util.h"
#include "task.h"

#ifndef NDEBUG
#define WARNING_INIT(...) Warning::init(__VA_ARGS__)
#define WARNING_LOG(level, n, format, ...) Warning::log(level, n, format, __VA_ARGS__)
#else
#define WARNING_INIT(...)
#define WARNING_LOG(level, n, format, ...)
#endif


#define MAX_WARNINGS 100  // maximum number of different warnings
#define MAX_WARNING_LENGTH 128 // max # of chars in a warning
#define COALESCE_INTERVAL_MS 5000  // coalesce warnings within this period in ms
#define WARNING_FLUSHER_PERIOD 5000 // how often to flush warnings
#define WARNING_FILE stdout

// Warnings specific to Yesql/Gaia
#define WARNING_RECEIVE_FRAGMENTED_RPC 1  // receiving RPC was too big and
                                          // therefore was fragmented
#define WARNING_SEND_FRAGMENTED_RPC    2  // sending RPC was too big and
                                          // therefore was fragmented


struct TaskMsgDataWarning {
  char *msg;
  int level;
  int number;
};


#ifndef NDEBUG
struct PastWarnings {
  int n;    // number of coalesced repetitions of this warning
  u64 first; // time of first coalesced repetition of this warning
};


class Warning {
private:
  static bool hasinit;
  static PastWarnings WarningList[];
  static int MaxWarning; // max warning number ever used

  static void ImmediateFuncWarning(TaskMsgData &msgdata, TaskScheduler *ts,
                                   int srcthread);

  // this method is called periodically to flush outstanding coalesced warnings
  static int flushWarnings(void *parm);

public:
  // warnings will be disabled until init() is called.
  // init() should be called in the thread that will be handling the warnings.
  static void init();

  // Log a message. Currently, level is not used, but it is intended
  // to represent a severity. In the future, we may add functionality
  // to filter messages below a chosen severity. Number is an identifier
  // for the warning; it is used to coalesce several warnings with the
  // same identifier issued within COALESCE_INTERVAL_MS milliseconds.
  //  Format and ... are as in printf.
  static void log(int level, unsigned number, const char *format, ...);
};

#endif
#endif
