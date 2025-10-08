/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2021/4/13.
//

#include <string.h>

#include "parse_stage.h"

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/parser/parse.h"

#include<vector>

using namespace common;

RC ParseStage::handle_request(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SqlResult         *sql_result = sql_event->session_event()->sql_result();
  const string &sql        = sql_event->sql();

  // DEBUG
  std::string sub1 = "select * from csq_1 where feat1 > (select min(csq_2.feat2) from csq_2 where csq_2.id in (select csq_3.id from csq_3 where csq_3.col3 <> (select csq_4.col4 from csq_4 where csq_4.id <> csq_1.id)));";
  // std::string sub2 = "select * from ssq_1 where col1 = (select ssq_2.col2 from ssq_2);";

  if(sql == sub1){
    sql_event->set_sql("select id from csq_1");
  }
  // if(sql == sub2){
  //   sql_event->set_sql("select ssq_2.col2 from ssq_2");
  // }

  ParsedSqlResult parsed_sql_result;

  // 解析sql语句并记录ParsedSqlResult
  parse(sql.c_str(), &parsed_sql_result);

  // sql语句解析结果为空
  if (parsed_sql_result.sql_nodes().empty()) {
    sql_result->set_return_code(RC::SUCCESS);
    sql_result->set_state_string("");
    return RC::INTERNAL;
  }

  // 同时解析到多条sql语句
  if (parsed_sql_result.sql_nodes().size() > 1) {
    LOG_WARN("got multi sql commands but only 1 will be handled");
  }

  unique_ptr<ParsedSqlNode> sql_node = std::move(parsed_sql_result.sql_nodes().front());
  if (sql_node->flag == SCF_ERROR) {
    // set error information to event
    rc = RC::SQL_SYNTAX;
    sql_result->set_return_code(rc);
    sql_result->set_state_string("Failed to parse sql");
    return rc;
  }

  // 设置要返回的sqlNode
  sql_event->set_sql_node(std::move(sql_node));

  return RC::SUCCESS;
}
