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
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

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

#include <my_global.h>
#include <mysql.h>

#include "bench-wiki-mysql-client.h"
#include "bench-log.h"

#define INCLUDE_WIKI_QUERIES 1
#include "bench-sql.h"

// Arguments: const char* s (query string)
//            size_t n (number of fields returend by query)
// Note: this macro depends on locally defined functions
//       query_wrapper
#define QUERY(s,n) if (query_wrapper(s, n)) return -1;

int CreateWikiMysqlClient(const char* conf_str, const char* confdir, BenchmarkClient** clp, bool leader){
  *clp = new WikiMysqlClient(conf_str, confdir, leader);
  return (*clp)->Init();
}

// Stolen from mysql-client.cpp. TODO: copypasta is bad, put this 
// in some common file.
inline void bind_string(const std::string& to_bind, MYSQL_BIND* bind){
  memset(bind, 0, sizeof(*bind));
  bind->buffer_type = MYSQL_TYPE_BLOB;
  bind->buffer = const_cast<void *>((const void *) to_bind.c_str());
  bind->buffer_length = (unsigned long) to_bind.length();
  bind->length_value = (unsigned long) to_bind.length();
}

inline void  bind_int(long long iarg, MYSQL_BIND* bind){
  std::stringstream iarg_ss;
  iarg_ss << iarg;
  bind_string(iarg_ss.str(), bind);
}

typedef std::unordered_map<std::string, std::set<std::string> > DatMap;

DatMap WikiMysqlClient::_category_map;
DatMap WikiMysqlClient::_link_map;
DatMap WikiMysqlClient::_stub_map;
DatMap WikiMysqlClient::_image_map;

std::mutex WikiMysqlClient::_category_load_lock;
std::vector<std::string> WikiMysqlClient::_page_titles;


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
WikiMysqlClient::threadSafeGetCategories(const std::string& page_title){
  return get_ts(page_title, _category_map);
}

const std::set<std::string> 
WikiMysqlClient::threadSafeGetImages(const std::string& page_title){
  return get_ts(page_title, _image_map);
}

const std::set<std::string> 
WikiMysqlClient::threadSafeGetStubs(const std::string& page_title){
  return get_ts(page_title, _stub_map);
}

const std::set<std::string> 
WikiMysqlClient::threadSafeGetLinks(const std::string& page_title){
  return get_ts(page_title, _link_map);
}

WikiMysqlClient::WikiMysqlClient(const char* database, const char* confdir, bool leader=false) : _dbname(database), _confdir(confdir), _dbconn(nullptr), _leader(leader){}
WikiMysqlClient::WikiMysqlClient(const std::string& database, const std::string& confdir, bool leader=false) : _dbname(database), _confdir(confdir), _dbconn(nullptr), _leader(leader){}

void WikiMysqlClient::load_data(){
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

int WikiMysqlClient::Init(){
  int ret = -1;
  try {
    size_t pos = _dbname.find(":");
    assert(pos > 0);
    std::string dbname = _dbname.substr(pos + 1);
    std::string host = _dbname.substr(0, pos);
    LOG("_dbname %s host %s dbname %s\n", _dbname.c_str(), host.c_str(), dbname.c_str());
    
    _dbconn = mysql_init(nullptr);
    if (mysql_real_connect(_dbconn, host.c_str(), DBUSER, DBPASSWD, 
                           dbname.c_str(), 0, nullptr, 0)){
      ret = 0;
    } else {
      LOG( "Couldnt connect to database (%s): Error: %s\n",
              _dbname.c_str(), mysql_error(_dbconn));
    }
  } catch (const std::out_of_range){
    LOG( "Connection string %s is invalid (no colon)\n\
         format is hostname:dbname\n", _dbname.c_str());
  }
  { // Only one thread should initialize the categories
    std::lock_guard<std::mutex> lock(_category_load_lock);
    if (_category_map.size() == 0){
      if (_leader)  {
		  int ret = mysql_query(_dbconn, WIKI_SYNCTABLE_STATEMENT);
          if (ret) LOG("Error creating synctable: %s\n", mysql_error(_dbconn));
		  mysql_query(_dbconn, "DELETE FROM synctable WHERE 1");
	  }
      load_data();
    }
  }
  return ret;
}

WikiMysqlClient::~WikiMysqlClient (){
  if (_dbconn) mysql_close(_dbconn);
};

inline int WikiMysqlClient::execute_sql(MYSQL_STMT* stmt, const char* caller){
  int rc = -1;
  if (stmt){
    rc = mysql_stmt_execute(stmt);
    if (rc != 0){
      LOG("Error executing %s: %s\n", caller, mysql_stmt_error(stmt));
    }
  }
  return rc;
}

struct MysqlStatementHelper {
  MysqlStatementHelper(MYSQL_STMT** stmt, MYSQL* dbconn, const char* query_str, WikiMysqlClient* clp){
    _stmt = clp->getStatement(query_str);
    if (!_stmt){
      LOG("Initializing statement for: %s\n", query_str);
      _stmt = mysql_stmt_init(dbconn);
      int rc = mysql_stmt_prepare(_stmt, query_str, (unsigned long) strlen(query_str));
      if (rc){
        LOG("Error preparing %s %d\n", query_str, rc); 
        LOG("extra info: %s\n", mysql_stmt_error(_stmt));
        mysql_stmt_close(_stmt);
        _stmt = nullptr;
      } else {
        clp->setStatement(query_str, _stmt);
      }
    } 
    *stmt = _stmt;
  }

  ~MysqlStatementHelper(){
    if(_stmt) mysql_stmt_free_result(_stmt);
    if(_stmt) mysql_stmt_reset(_stmt);
  }
  MYSQL_STMT* _stmt;
};

static void init_result_bind(MYSQL_BIND* bind, 
                             unsigned long* item_lengths,
                             size_t sz){
  // this is a hack to figure out if we need to allocate large
  // space for the pages.
  size_t alloc_sz = (sz == 2) ? 10 * 4096 : 256;
  for (auto i = 0; i < (int) sz; ++i){
    memset(&bind[i], 0, sizeof(MYSQL_BIND));
    bind[i].buffer_type = MYSQL_TYPE_BLOB;
    item_lengths[i] = 0;
    bind[i].length = &item_lengths[i];
    bind[i].buffer_length = alloc_sz;
    bind[i].buffer = malloc(alloc_sz);
  }
}

static int fetch_result(MYSQL_STMT* stmt, int nCols, const std::set<int>& fetch_cols, 
             std::vector<std::string>& cols){
  int rc = -1;
  //long long pgid = 0;
  int frc;
  unsigned long* item_lens = new unsigned long[nCols];
  MYSQL_BIND* bind = new MYSQL_BIND[nCols];

  init_result_bind(bind, item_lens, nCols);
  if (0 == mysql_stmt_bind_result(stmt,bind)){
    frc = mysql_stmt_fetch(stmt);
    rc = 0;
    // Successfully got the data
    if (frc == 0 || frc == MYSQL_DATA_TRUNCATED){
      for (auto i = 0; i < nCols; ++i){
        if (mysql_stmt_fetch_column(stmt, &bind[i], i, 0)){
          LOG("Error fetching column %d, reason: %s\n",
              i, mysql_stmt_error(stmt));
        } else if (fetch_cols.find(i) != fetch_cols.cend()){
          cols.emplace_back((char*)bind[i].buffer, item_lens[i]);
        } else if (bind[i].error_value || (bind[i].error && *bind[i].error)){
          if (bind[i].length){
            LOG("truncated page: %d\n", *bind[i].length);
           } else {
             LOG("other error...\n");
             rc = -1;
          }
        }
      } 
    } else if (frc != MYSQL_NO_DATA){
      LOG("Unknown error %d from mysql_stmt_fetch()\n", frc);
      rc = -1;
    }
  }

  for (auto i = 0; i < nCols; ++i) free(bind[i].buffer);
  delete [] bind; delete [] item_lens;
  return rc;
}

class MysqlBindHelper {
private:
  size_t      _idx;
  MYSQL_BIND* _bind, *_curr_bind;
public:
  MysqlBindHelper(size_t size) :
    _idx(0), _bind(size ? new MYSQL_BIND[size] : nullptr), _curr_bind(_bind){ }

  ~MysqlBindHelper(){if (_bind) delete [] _bind;}

  MYSQL_BIND* get_first(){return _bind;}
  MYSQL_BIND* get_next(){return _curr_bind++;}
};

// Does everything necessary to perform the query in query_str
// parameters:
//    query_str  -- the SQL to query
//    nCols      -- number of columns returned by this query
//    params     -- parameters to bind to ? in this query (must be in order)
//    fetch_cols -- which columns to return (note: all columns are *read*,
//                  this is just the list to return)
//    cols       -- result array
// RETURN VALUE: 0 on success, -1 on error.
int WikiMysqlClient::do_query(const char* query_str, int nCols, const std::vector<std::string>& params,
         const std::set<int>& fetch_cols, std::vector<std::string>& cols){
  MYSQL_STMT* stmt;
  int rc = 0;
  MysqlBindHelper bh(params.size());
  MysqlStatementHelper h(&stmt, _dbconn, query_str, this);
  for (const auto& i : params){
    bind_string(i, bh.get_next());
  }
  if (params.size()) rc = mysql_stmt_bind_param(stmt, bh.get_first());
  if (rc){LOG("Error binding %s %d\n", query_str, rc); return -1;}
  if (execute_sql(stmt, query_str)) return -1;
  if (nCols) rc = fetch_result(stmt, nCols, fetch_cols, cols);
  if (rc){LOG("Error fetching %s\n", query_str); return -1;}
  return 0;
}
         
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

int WikiMysqlClient::browser_cache_read(int seed){
  // Use the seed to construct some random values
  const std::string title = _page_titles[seed % _page_titles.size()];

  // get the categories (normally we'd get this by checking the cached object)
  const std::set<std::string> categories = threadSafeGetCategories(title);
  std::stringstream categories_bind_ss;
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

//  QUERY(update_site_stats, 0);
//  reset_locals();

//  QUERY(job_mgmt, 6);
//  reset_locals();
  return 0;
}

int WikiMysqlClient::memcache_read(int seed){
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

int WikiMysqlClient::database_read(int seed){
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

// Utilities for reading/writing synctable... Unfortunately more copypasta from mysql-client.cpp
MYSQL_STMT *WikiMysqlClient::construct_sql_insert(const TableId& table, const Key& key, const ValueMap& values){
  MYSQL_STMT* ret = mysql_stmt_init(_dbconn);
  bool prepared = false;
  std::string sql;
  sql.append("INSERT INTO ");
  sql.append(table);
  sql.append(" (");
  sql.append(KEYNAME);
  sql.append(",");
  for (auto& it: values){
    sql.append(it.first);
    sql.append(",");
  }
  sql.back() = ')';
  sql.append(" VALUES (");
  sql.append("?,");
  for (auto& it : values){
    sql.append("?,");
  }
  sql.back() = ')';
  sql.append(" ON DUPLICATE KEY UPDATE ");
  for (auto& it: values){
    sql.append(it.first);
    sql.append("=?,");
  }
  sql.back() = ' ';

  int rc = mysql_stmt_prepare(ret, sql.c_str(), (unsigned long) sql.length());
  if (rc == 0){
    int count = 0;
    prepared = true;
    MYSQL_BIND *bind = new MYSQL_BIND [2 * values.size() + 1];
    bind_string(key, &bind[count++]);
    // First time bind values for the insert
    for (auto& it : values){
      bind_string(it.second, &bind[count++]);
    }
    // Second time bind values for the update
    for (auto& it : values){
      bind_string(it.second, &bind[count++]);
    }
    rc = mysql_stmt_bind_param(ret, bind);
    delete [] bind;
  } 
  if (rc != 0){
    LOG( "insert %s failed: %s\n", (prepared) ? "binding" : "preparing",
            mysql_stmt_error(ret));
    mysql_stmt_close(ret);
    ret = nullptr;
  }
  return ret;
}

int WikiMysqlClient::insert(const TableId& table, const Key& key, const ValueMap& values){
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
  /*
  MYSQL_STMT* stmt = construct_sql_insert(table, key, values);
  int ret = execute_sql(stmt, __FUNCTION__);
  if (stmt) mysql_stmt_close(stmt);
  return ret;
  */
}

int WikiMysqlClient::read(const TableId& table, const Key& key, const FieldList& fields,
      ValueMap& result){
  static const char* read_sync = "SELECT FIELD1 FROM synctable WHERE " KEYNAME " = ?";
  if (table != std::string("synctable")) return -1;  

  INIT_QUERY_LOCALS;

  params.push_back(key);
  fetch_cols.insert(0);
  QUERY(read_sync, 1);
  LOG("%d fields read\n", ret_cols.size());
  if (ret_cols.size() == 0) return -1;
  LOG("got %s when I read\n", ret_cols[0].c_str());
  result[fields[0]] = ret_cols[0];
  reset_locals();

  return 0;
}
