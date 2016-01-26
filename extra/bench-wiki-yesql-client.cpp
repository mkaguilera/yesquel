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
#include <stdio.h>
#include <stdarg.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bench-wiki-yesql-client.h"

#define INCLUDE_WIKI_QUERIES 1
#include "bench-sql.h"
#include "bench-log.h"

typedef std::unordered_map<std::string, std::set<std::string> > DatMap;

DatMap WikiYesqlClient::_category_map;
DatMap WikiYesqlClient::_link_map;
DatMap WikiYesqlClient::_stub_map;
DatMap WikiYesqlClient::_image_map;

std::mutex WikiYesqlClient::_category_load_lock;
std::vector<std::string> WikiYesqlClient::_page_titles;

// Since access to a std::map via [] is non-const, we use this helpful
// wrapper instead.
static const std::set<std::string>
get_ts(const std::string& key, const DatMap& map){
  auto it = map.find(key);
  if (it != map.cend()){
    return it->second;
  }
 std::set<std::string> ret;
 return ret;
}

const std::set<std::string> 
WikiYesqlClient::threadSafeGetCategories(const std::string& page_title){
  return get_ts(page_title, _category_map);
}

const std::set<std::string> 
WikiYesqlClient::threadSafeGetImages(const std::string& page_title){
  return get_ts(page_title, _image_map);
}

const std::set<std::string> 
WikiYesqlClient::threadSafeGetStubs(const std::string& page_title){
  return get_ts(page_title, _stub_map);
}

const std::set<std::string> 
WikiYesqlClient::threadSafeGetLinks(const std::string& page_title){
  return get_ts(page_title, _link_map);
}

WikiYesqlClient::WikiYesqlClient(const char *database, const char *confdir, bool create) : _dbname(database), _confdir(confdir), _create(create), _dbhandle(nullptr){}
WikiYesqlClient::WikiYesqlClient(const std::string& database, const std::string& confdir, bool create) : _dbname(database), _confdir(confdir), _create(create), _dbhandle(nullptr){}

void WikiYesqlClient::load_data(){
  auto loader_helper = [] (DatMap* dat_map, const char* filename, bool load_titles){
    int fd = open(filename, O_RDONLY);
    if (fd == -1){LOG("%s not found\n", filename); exit(1);}

    // Helper function
    auto read_wrapper = [fd] (int len, void* buf){
      int tmp; // useful for debugging
      if((tmp = ::read(fd, buf, len)) != len){
        LOG("Failed to load %s!\n");
        exit(1);
      }
    };
    int total_len, title_len, cat_len, n_cats;
    read_wrapper(sizeof(total_len), &total_len);
    LOG("read total_len: %d\n", total_len);
    char stack[2048];
    char* buf = stack;

    dat_map->reserve(total_len);
    for (auto i = 0; i < total_len; ++i){
      // read the page title
      read_wrapper(sizeof(title_len), &title_len);
      read_wrapper(title_len, buf);
      const std::string title(buf, title_len);
      if (load_titles) _page_titles.push_back(title);

      // read categories for that title
      read_wrapper(sizeof(n_cats), &n_cats);
      for (auto j = 0; j < n_cats; ++j){
        read_wrapper(sizeof(cat_len), &cat_len);
        if (cat_len > (int)sizeof(stack)){
          if (buf == stack){
            buf = (char *) malloc(cat_len);
          } else {
            buf = (char *) realloc(buf, cat_len);
          }
        }
        read_wrapper(cat_len, buf);
        const std::string category(buf, cat_len);
        (*dat_map)[title].insert(category);
      }
    }
    if (buf != stack) free(buf);
    close(fd);
    LOG("Done with %s\n", filename);
  };

  std::vector<std::thread> threads;

  std::string confpath = _confdir;
  if (!confpath.empty() && confpath.back() != '/') confpath += "/";
  
  std::string catfilename = confpath + "categories.dat";
  std::string linksfilename = confpath + "links.dat";
  std::string stubsfilename = confpath + "stubs.dat";
  std::string imagesfilename = confpath + "images.dat";
  
  threads.push_back(std::thread(std::bind(loader_helper, &_category_map, catfilename.c_str(), true)));
  threads.push_back(std::thread(std::bind(loader_helper, &_link_map, linksfilename.c_str(), false)));
  threads.push_back(std::thread(std::bind(loader_helper, &_stub_map, stubsfilename.c_str(), false)));
  threads.push_back(std::thread(std::bind(loader_helper, &_image_map, imagesfilename.c_str(), false)));

  for (auto& thread : threads){
    thread.join();
  }
  return;
}

int WikiYesqlClient::Init(){
  int ret = 0;
  int retries = 0;
  do {
    ret = sqlite3_open(_dbname.c_str(), &_dbhandle);
    if (ret != SQLITE_OK){
      ++retries;
      usleep(1000000);
    }
  } while (ret != SQLITE_OK && retries < 10);
  
  char* err_str = nullptr;
  if (ret == SQLITE_OK){
    if (_create){
      LOG("Opened db %s, about to create a table:\n%s\n", _dbname.c_str(), WIKI_SYNCTABLE_STATEMENT);
      retry1:
      ret = sqlite3_exec(_dbhandle, WIKI_SYNCTABLE_STATEMENT, nullptr, nullptr, &err_str);
      if (ret == SQLITE_BUSY){ usleep(1000); goto retry1; }
      if (ret != SQLITE_OK){
        LOG("SQL error %d: %s\n", ret, err_str);
        sqlite3_free(err_str);
        sqlite3_close(_dbhandle);
      }
    }
  } else {
    LOG("Can't open database %s (%d): %s\n", _dbname.c_str(),
         ret, sqlite3_errmsg(_dbhandle));
    sqlite3_close(_dbhandle);
  }
  // Only one thread should initialize the categories
  {
    std::lock_guard<std::mutex> lock(_category_load_lock);
    if (_category_map.size() == 0){
      //sqlite3_exec(_dbhandle, "DELETE FROM synctable WHERE 1", nullptr, nullptr, &err_str);
      load_data();
    }
  }
  return ret;
}

WikiYesqlClient::~WikiYesqlClient (){
  if (_dbhandle) sqlite3_close(_dbhandle);
}

int CreateWikiYesqlClient(const char* conf_str, const char* confdir, BenchmarkClient** clp, bool create){
  *clp = new WikiYesqlClient(conf_str, confdir, create);
  return (*clp)->Init();
}

inline int WikiYesqlClient::execute_sql(sqlite3_stmt* stmt, const char* caller){
  int rc = -1;
  if (stmt){
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE) rc = 0;
  }
  return rc;
}

struct YesqlStatementHelper {
  YesqlStatementHelper(sqlite3_stmt** stmt, sqlite3* dbhandle, const char*sql, WikiYesqlClient* clp) :
                       _sql(sql), _clp(clp){
    _stmt = _clp->getStatement(sql);
    const char* tail;
    int rc;
    if (!_stmt){
      if ((rc = sqlite3_prepare_v2(dbhandle, sql, strlen(sql) + 1, &_stmt, &tail)) != SQLITE_OK){
        LOG("error preparing: %d\n", rc);
        LOG("more details? %s\n", sqlite3_errmsg(dbhandle));
      } else {
        _clp->setStatement(sql, _stmt);
      }
    }
    *stmt = _stmt;
  }

  ~YesqlStatementHelper(){
    if (_stmt){
      if (sqlite3_reset(_stmt)){
        sqlite3_finalize(_stmt);
        _clp->setStatement(_sql, nullptr);
      }
    }
  }

  // This is a shallow copy because I know the lifetime of this statement is limited to
  // a single function call.
  const char*  _sql;
  sqlite3_stmt* _stmt;
  WikiYesqlClient* _clp;
};

int read_results(sqlite3_stmt* stmt, int nCols, const std::set<int>& fetch_cols, std::vector<std::string>& cols, int &retcols, int &retrows){
  int rc;
  retcols = 0;
  retrows = 0;
  do {
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW){
      ++retrows;
      for (auto i = 0; i < nCols; ++i){
        ++retcols;
        const char* ret = (const char*) sqlite3_column_text(stmt, i);
        if (fetch_cols.find(i) != fetch_cols.cend()){
          cols.push_back(ret);
        }
      }
    }
  } while (rc == SQLITE_ROW);
  if (rc == SQLITE_DONE) rc = 0;
  return rc;
}


void logger(void* _ctx, const char* msg){
  LOG("msg from SQLITE: %s\n", msg);
}
 
int WikiYesqlClient::do_query(const char* query_str, int nCols, const std::vector<std::string>& params,
         const std::set<int>& fetch_cols, std::vector<std::string>& cols){
  int rc;
  int retrows, retcols;
  //char* err_str;
  sqlite3_stmt* stmt = nullptr;
  YesqlStatementHelper sh(&stmt, _dbhandle, query_str, this);
  if (!stmt) return -1;
  int idx = 1;
  for (auto it = params.cbegin(); it != params.cend(); ++it){
    if (SQLITE_OK != (rc = sqlite3_bind_text(stmt, idx++, it->c_str(),
                                             (int) it->size(), SQLITE_STATIC))){
      LOG("Errror binding parameter: %d\n", rc);
      LOG("Query: %s\n", query_str);
      LOG("Parameter: %s\n", it->c_str());
      return -1;
    }
  }
  if (nCols) rc = read_results(stmt, nCols, fetch_cols, cols, retrows, retcols);
  else {
    retrows = retcols = 0;
    rc = execute_sql(stmt, query_str);
  }
  if (rc){
    LOG("Error %d fetching '%s' with nCols %d: retrows %d retcols %d pthread %d errmsg %s\n", rc, query_str, nCols, retrows, retcols, (int) pthread_self(), sqlite3_errmsg(_dbhandle));
    return -1;
  }

  return 0;
}

// Arguments: const char* s (query string)
//            size_t n (number of fields returend by query)
// Note: this macro depends on locally defined functions
//       query_wrapper
#define QUERY(s,n) if (query_wrapper(s, n)) return -1;
#define REPEAT_QUERY(s,n) while(!query_wrapper(s,n));
         
#define INIT_QUERY_LOCALS \
  std::set<int> fetch_cols; \
  std::vector<std::string> params; \
  std::vector<std::string> ret_cols; \
  auto reset_locals = [&fetch_cols, &params, &ret_cols] (){ \
    fetch_cols.clear(); params.clear(); ret_cols.clear(); \
  }; \
  auto query_wrapper = [this, &fetch_cols, &params, &ret_cols] (const char* qstr, int nCols){ \
    return do_query(qstr, nCols, params, fetch_cols, ret_cols); \
  }; \

int WikiYesqlClient::browser_cache_read(int seed){
  // Use the seed to construct some random values
  const std::string title = _page_titles[seed % _page_titles.size()];

  // get the categories (normally we'd get this by checking the cached object)
  const std::set<std::string> categories = threadSafeGetCategories(title);
  std::stringstream categories_bind_ss;
  for (auto i = 0; i < (int) categories.size() - 1; ++i){
    categories_bind_ss << "?,";
  }
  categories_bind_ss << "?";
  const std::string get_categories_sql = get_category_links + categories_bind_ss.str() + "))";

  // Generate a fake IPV6 addr
  char IPV6_ADDR[128];
  int addr_len = snprintf(IPV6_ADDR, 128, "%3d:%3d:0:0:0:%3d:0:%3d",
                           seed & UCHAR_MAX, (seed >> 8) & UCHAR_MAX,
                           (seed >> 16) & UCHAR_MAX, (seed >> 24) & UCHAR_MAX);
  const std::string ipaddr(IPV6_ADDR, addr_len);

  INIT_QUERY_LOCALS;

  // ACTUAL QUERIES FOLLOW

  // Get the page_id
  fetch_cols.insert(0);
  params.push_back(title);
  QUERY(get_page_cols, 11);
  // Not a real page
  if (ret_cols.size() == 0) return -1;
  const std::string pageid(ret_cols[0]);
  reset_locals();

  params.push_back(pageid);
  QUERY(get_page_restrictions, 7);
  reset_locals();

  for (auto& i : categories) params.push_back(i);
  QUERY(get_categories_sql.c_str(), 7);
  reset_locals();

/*
  params.push_back(ipaddr);
  params.push_back(ipaddr);
  params.push_back(title);
  QUERY(get_user_info, 6);
  reset_locals();

  params.push_back(ipaddr);
  QUERY(get_user_talk, 1);
  reset_locals();

  QUERY(check_static_version, 3);
  reset_locals();

  QUERY(check_noscript_version, 3);
  reset_locals();
  */

//  QUERY(check_message_resources, 3);
//  reset_locals();

  params.push_back(pageid);
  QUERY(update_page_stats, 0);
  reset_locals();

  // QUERY(update_site_stats, 0);
  // reset_locals();

  // QUERY(job_mgmt, 6);
  // reset_locals();
  return 0;
}

int WikiYesqlClient::memcache_read(int seed){
  // Use the seed to construct some random values
  const std::string title = _page_titles[seed % _page_titles.size()];

  // get the categories (normally we'd get this by checking the cached object)
  std::stringstream categories_bind_ss;
  const std::set<std::string> categories = threadSafeGetCategories(title);
  for (auto i = 0; i < (int)categories.size() - 1; ++i){
    categories_bind_ss << "?,";
  }
  categories_bind_ss << "?";
  const std::string get_categories_sql = get_category_links + categories_bind_ss.str() + "))";

  // Generate a fake IPV6 addr
  char IPV6_ADDR[128];
  int addr_len = snprintf(IPV6_ADDR, 128, "%3d:%3d:0:0:0:%3d:0:%3d",
                          seed & UCHAR_MAX, (seed >> 8) & UCHAR_MAX,
                         (seed >> 16) & UCHAR_MAX, (seed >> 24) & UCHAR_MAX);
  const std::string ipaddr(IPV6_ADDR, addr_len);

  INIT_QUERY_LOCALS;
  return 0;
}

int WikiYesqlClient::database_read(int seed){
  // Use the seed to construct some random values
  const std::string title = _page_titles[seed % _page_titles.size()];
  const std::set<std::string> links = threadSafeGetLinks(title);
  const std::set<std::string> stubs = threadSafeGetStubs(title);
  const std::set<std::string> images = threadSafeGetImages(title);

  // get the categories (normally we'd get this by checking the cached object)
  std::stringstream categories_bind_ss;
  const std::set<std::string> categories = threadSafeGetCategories(title);
  for (auto i = 0; i < (int)categories.size() - 1; ++i){
    categories_bind_ss << "?,";
  }
  categories_bind_ss << "?";
  const std::string get_categories_sql = get_category_links + categories_bind_ss.str() + "))";

  // Generate a fake IPV6 addr
  char IPV6_ADDR[128];
  int addr_len = snprintf(IPV6_ADDR, 128, "%3d:%3d:0:0:0:%3d:0:%3d",
                          seed & UCHAR_MAX, (seed >> 8) & UCHAR_MAX,
                         (seed >> 16) & UCHAR_MAX, (seed >> 24) & UCHAR_MAX);
  const std::string ipaddr(IPV6_ADDR, addr_len);

  INIT_QUERY_LOCALS;

  // Get the page
  fetch_cols.insert(0);
  fetch_cols.insert(9);
  params.push_back(title);
  QUERY(get_page_cols, 11);
  if (ret_cols.size() < 2) return -1;
  const std::string pageid(ret_cols[0]);
  const std::string revid(ret_cols[1]);
  reset_locals();

  params.push_back(pageid);
  QUERY(get_page_restrictions, 11);
  reset_locals();

  params.push_back(pageid);
  params.push_back(revid);
  QUERY(fetch_from_conds, 19);
  reset_locals();

  params.push_back(revid);
  QUERY(load_text, 2);
  reset_locals();

  // Build stub links
  for (auto& stub : stubs){
    reset_locals();
    fetch_cols.insert(0);
    params.push_back(stub);
    QUERY(load_stub_text, 19);
    if (ret_cols.size() == 0) continue;
    const std::string stub_rev(ret_cols[0]);
    reset_locals();

    params.push_back(stub);
    QUERY(add_link_obj, 4);
    reset_locals();
    
    params.push_back(stub_rev);
    QUERY(load_text, 2);
    reset_locals();
  }
  if (stubs.size() > 0){
    reset_locals();
    const std::string stub("Stub-template");
    params.push_back(stub);
    QUERY(load_stub_text, 19);
    if (ret_cols.size() > 0){
      const std::string stub_rev(ret_cols[0]);
      reset_locals();

      params.push_back(stub);
      QUERY(add_link_obj, 4);
      reset_locals();
    
      params.push_back(stub_rev);
      QUERY(load_text, 2);
      reset_locals();
    }
  }
  reset_locals();

  // Check the interwiki links
  QUERY(iw_if, 6); reset_locals();
  QUERY(iw_ifeq, 6); reset_locals();
  QUERY(iw_iferror, 6); reset_locals();
  QUERY(iw_switch, 6); reset_locals();

  for (auto& image : images){
    params.push_back(image);
    QUERY(get_img_pg, 1);
    reset_locals();
  }

  fetch_cols.insert(0);
  QUERY(get_bad_img_list, 19);
  if (fetch_cols.size() == 0) return -1;
  const std::string rev_bad_img(ret_cols[0]);
  reset_locals();

  QUERY(iw_wikipedia, 6); reset_locals();

  for (auto& image : images){
    params.push_back(image);
    QUERY(get_img, 13);
    reset_locals();
  }

  if (stubs.size() && links.size()){
    char get_links_query[4096];
    std::stringstream links_bind_ss;
    for (auto i = 0; i < (int)links.size() - 1; ++i){
      links_bind_ss << "?,";
    } links_bind_ss << "?";
 
    std::stringstream stubs_bind_ss;
    for (auto i = 0; i < (int)stubs.size() - 1; ++i){
      stubs_bind_ss << "?,";
    } stubs_bind_ss << "?";

    snprintf(get_links_query, 4096, get_links, links_bind_ss.str().c_str(),
      stubs_bind_ss.str().c_str());
    
    for (auto& link : links) params.push_back(link);
    for (auto& stub : stubs) params.push_back(stub);

    QUERY(get_links_query, 6);
    reset_locals();
  }

  for (auto& i : categories) params.push_back(i);
  QUERY(get_categories_sql.c_str(), 7);
  reset_locals();
  
  params.push_back(ipaddr);
  params.push_back(ipaddr);
  params.push_back(title);
  QUERY(preload_existence, 6);
  reset_locals();

  params.push_back(ipaddr);
  QUERY(check_newtalk, 1);
  reset_locals();

  return 0;
}

int WikiYesqlClient::insert(const TableId& table, const Key& key, const ValueMap& values){
  static const char* read_insert = "INSERT INTO synctable VALUES (?, ?)";
  if (table != std::string("synctable")) return -1;

  // This will fail ungracefully if value map doesn't contain FIELD1
  const std::string value = (values.find("FIELD1")->second);
  INIT_QUERY_LOCALS;

  params.push_back(key);
  params.push_back(value);
  QUERY(read_insert, 0);
  reset_locals();

  return 0;
}

int WikiYesqlClient::read(const TableId& table, const Key& key, const FieldList& fields,
      ValueMap& result){
  static const char* read_sync = "SELECT FIELD1 FROM synctable WHERE " KEYNAME " = ?";
  if (table != std::string("synctable")) return -1;  

  INIT_QUERY_LOCALS;

  params.push_back(key);
  fetch_cols.insert(0);
  QUERY(read_sync, 1);
  if (ret_cols.size() == 0) return -1;
  result[fields[0]] = ret_cols[0];
  reset_locals();

  return 0;
}
