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

#pragma once

// CLIENT-CACHED QUERIES (Non-nosql)
// One argument TITLE
const char* get_page_cols = " SELECT page_id,page_namespace,page_title,page_restrictions,page_counter,page_is_redirect,page_is_new,page_random,page_touched,page_latest,page_len  FROM page WHERE page_namespace='0' AND page_title=? LIMIT 1";
// ONE ARGUMENT PAGE_ID
const char* get_page_restrictions = "SELECT * FROM page_restrictions  WHERE pr_page=?";
// APPEND TO BIND N CATEGORIES
const char* get_category_links = "SELECT page_id,page_namespace,page_title,page_len,page_is_redirect,page_latest,pp_value  FROM page LEFT JOIN page_props ON (pp_propname = 'hiddencat' AND (pp_page = page_id))  WHERE (page_namespace = '14' AND page_title IN (";
// Three arguments, IPV6_ADDR, IPV6_ADDR, TITLE
const char* get_user_info = "SELECT page_id,page_namespace,page_title,page_len,page_is_redirect,page_latest  FROM page  WHERE (page_namespace = '2' AND page_title = ?) OR (page_namespace = '3' AND page_title = ?) OR (page_namespace = '1' AND page_title = ?)"; 
// ONE ARGUMENT IPV6_ADDR
const char* get_user_talk = "SELECT   user_ip  FROM user_newtalk  WHERE user_ip = ? LIMIT 1";  
// NO ARGUMENTS
const char* check_static_version = "SELECT   page_namespace,page_title,page_touched  FROM page  WHERE (page_namespace = '8' AND page_title IN ('Common.js','Common.css','Vector.js','Vector.css','Print.css'))";
const char* check_noscript_version = "SELECT   page_namespace,page_title,page_touched  FROM page  WHERE (page_namespace = '8' AND page_title = 'Noscript.css')";
const char* check_message_resources = "SELECT   mr_blob,mr_resource,mr_timestamp  FROM msg_resource  WHERE mr_resource IN ('user.options','user.tokens')  AND mr_lang = 'en'";
// ONE ARGUMENT PAGE_ID
const char* update_page_stats = "UPDATE   page SET page_counter = page_counter + 1 WHERE page_id =?";
// NO ARGUMENTS
const char* update_site_stats = "UPDATE   site_stats SET ss_total_views=ss_total_views+1";
const char* job_mgmt = "SELECT   *  FROM job  WHERE (job_id >= 0)  ORDER BY job_id LIMIT 1";

// 19 Cols returned ARGUMENTS: pageid, latest
const char* fetch_from_conds = "SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,user_name  FROM revision INNER JOIN page ON ((page_id = rev_page)) LEFT JOIN user ON ((rev_user != 0) AND (user_id = rev_user))  WHERE page_id = ? AND rev_id = ?  LIMIT 1";

// 2 Cols returned, ARGUMENTS: latest
const char* load_text = "SELECT old_text,old_flags  FROM text WHERE old_id = ? LIMIT 1";

// 19 Cols returned, ARGUMENTS: stub-title
// should-capture rev_id
const char* load_stub_text = "SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,user_name  FROM revision INNER JOIN page ON ((page_id = rev_page)) LEFT JOIN user ON ((rev_user != 0) AND (user_id = rev_user))  WHERE page_namespace = '10' AND page_title = ? AND (rev_id=page_latest)  LIMIT 1";

const char* add_link_obj = "SELECT page_id,page_len,page_is_redirect,page_latest  FROM page  WHERE page_namespace = '10' AND page_title = ?  LIMIT 1";

// 6 cols returned, no arguments 
const char* iw_if = "SELECT iw_prefix,iw_url,iw_api,iw_wikiid,iw_local,iw_trans  FROM interwiki  WHERE iw_prefix = '#if'";
const char* iw_ifeq = "SELECT iw_prefix,iw_url,iw_api,iw_wikiid,iw_local,iw_trans  FROM interwiki  WHERE iw_prefix = '#ifeq'";
const char* iw_iferror = "SELECT iw_prefix,iw_url,iw_api,iw_wikiid,iw_local,iw_trans  FROM interwiki  WHERE iw_prefix = '#iferror'";
const char* iw_switch = "SELECT iw_prefix,iw_url,iw_api,iw_wikiid,iw_local,iw_trans  FROM interwiki  WHERE iw_prefix = '#switch'";

// 1 col returned, ARGUMENT: image name
const char* get_img_pg = "SELECT page_id FROM page  WHERE page_namespace = '6' AND page_title = ? LIMIT 1";  

// 19 cols returned, no arguments.
// should capture rev_id for next
const char* get_bad_img_list = "SELECT rev_id,rev_page,rev_text_id,rev_timestamp,rev_comment,rev_user_text,rev_user,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,user_name  FROM `revision` INNER JOIN `page` ON ((page_id = rev_page)) LEFT JOIN `user` ON ((rev_user != 0) AND (user_id = rev_user))  WHERE page_namespace = '8' AND page_title = 'Bad_image_list' AND (rev_id=page_latest)  LIMIT 1"; 
// load text here

const char* iw_wikipedia = "SELECT iw_prefix,iw_url,iw_api,iw_wikiid,iw_local,iw_trans  FROM interwiki  WHERE iw_prefix = 'wikipedia'";

// 13 cols returned, ARGUMENT: image name
const char* get_img = "SELECT img_size,img_width,img_height,img_bits,img_media_type,img_major_mime,img_minor_mime,img_metadata,img_timestamp,img_sha1,img_user,img_user_text,img_description  FROM image  WHERE img_name = ?  LIMIT 1"; 

// 6 cols returend, ARGUMENTS: links <multi>, stubs <multi>
const char* get_links = "SELECT page_id,page_namespace,page_title,page_is_redirect,page_len,page_latest  FROM page WHERE (page_namespace = '0' AND page_title IN (%s) ) OR (page_namespace = '14' AND page_title in (%s))";

// 6 cols returned, ARGUMENTS: ip, ip, title
const char* preload_existence = "SELECT page_id,page_namespace,page_title,page_len,page_is_redirect,page_latest  FROM page  WHERE (page_namespace = '2' AND page_title = ?) OR (page_namespace = '3' AND page_title = ?) OR (page_namespace = '1' AND page_title = ?)";

// 1 col returned, ARGUMENT: ip
const char* check_newtalk = "SELECT user_ip  FROM user_newtalk  WHERE user_ip = ?  LIMIT 1";
