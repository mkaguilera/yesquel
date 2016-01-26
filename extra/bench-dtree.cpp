/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  All rights reserved. 

  Written by Joshua B. Leners

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
#include <float.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <malloc.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bench-runner.h"
#include "bench-log.h"
#include "bench-config.h"
#include "bench-dtree-client.h"

void run_client(const char* conf_str, const Config* cfg, bool init_db){
  ClientPtr clp;
  int ret;
  // SYSTEM-SPECIFIC
  if ((ret = CreateDtreeClient(conf_str, &clp, init_db)) != 0){
    LOG("issue creating client! %d\n", ret);
  } else {
    Workload workload = getWorkloadFromString(cfg->get<std::string>("workload", ""));
    RunWorkload(clp, workload, cfg);
  }
  delete clp;
  return;
}

void do_experiment(int nthreads, const char* cfg_str, const Config* cfg, bool load){
  std::vector<std::thread> threads;
  for (auto i = 0; i < nthreads; ++i){
    threads.push_back(std::thread(std::bind(run_client, cfg_str, cfg, load && i == 0)));
  }
  for (auto& i : threads){
    i.join();
  }
}

void SetDebugLevel(int nl);

extern int handleOpts(int argc, char *argv[]);
extern int optind;
extern int optClientno;
extern char *usage;

int main(int argc, char* argv[])
{
  argc -= handleOpts(argc, argv);
  if (argc < 2){
    fprintf(stderr, usage, argv[0]);
    exit(1);
  }
  optClientno = atoi(argv[optind+1]);

  SetDebugLevel(0);
  ConfigParser cp(argv[optind]);
  cp.Parse();
  bool load_complete = false;
  for (auto& section : cp._section_names){
    if (section.find("Workload") != std::string::npos){
      std::string workload = section;

      Config cfg(&cp, workload);
      // SYSTEM-SPECIFIC
      std::string system_conf = cfg.get<std::string>("yesql", "");

      std::string dir = cfg.get<std::string>("logdir", ".");
      LOG("Got %s as logdir", dir.c_str());
      if (dir.back() != '/')
        dir.append("/");
      dir.append("client-");
      dir.append(std::to_string(optClientno));
      dir.append("-");
      dir.append(workload);
      dir.append(".txt");
      LOG("Setting log as %s\n", dir.c_str());
      SetLog(dir.c_str());
      LOG("About to initialize\n");
      if (!load_complete){
        sqlite3_initialize();
        SetDebugLevel(2);
      } else {
        SetDebugLevel(2);
      }
      int nthreads = cfg.get<int>("threads", 1);
      bool do_load = false;
      if (!load_complete)
        do_load = cfg.get<bool>("load", true);
      LOG("About to do a yesql experiment with %d threads\n", nthreads);
      do_experiment(nthreads, system_conf.c_str(), &cfg, do_load);
      load_complete = true;
    }
  }
  return 0;
}
