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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

using namespace std;
using namespace common;


void aggregate_or_not(unique_ptr<Expression>& expr, int& isaggr, int& notaggr){
  if(expr->type()==ExprType::AGGREGATION){
    isaggr += 1;
  }else{
    if(expr->type()==ExprType::FIELD || expr->type()==ExprType::VALUE){
      notaggr += 1;
    }else if(expr->type()==ExprType::ARITHMETIC){
      unique_ptr<Expression>& left_expr = static_cast<ArithmeticExpr*>(expr.get())->left();
      unique_ptr<Expression>& right_expr = static_cast<ArithmeticExpr*>(expr.get())->right();
      aggregate_or_not(left_expr, isaggr, notaggr);
      aggregate_or_not(right_expr, isaggr, notaggr);
    }else if(expr->type()==ExprType::COMPARISON){
      unique_ptr<Expression>& left_expr = static_cast<ComparisonExpr*>(expr.get())->left();
      unique_ptr<Expression>& right_expr = static_cast<ComparisonExpr*>(expr.get())->right();
      aggregate_or_not(left_expr, isaggr, notaggr);
      aggregate_or_not(right_expr, isaggr, notaggr);
    }else{
      LOG_DEBUG("unsupport aggragete check");
    }
  }
}

RC check_select_stmt(vector<unique_ptr<Expression>>& query_expr, vector<unique_ptr<Expression>>& groupby_expr){
  // select查询字段，groupby分组字段，having分组后过滤
  // 先检查最简单的一种情况，在没有分组字段时，查询字段不允许同时出现非聚合字段和聚合字段
  if(groupby_expr.size()==0){
    int isaggr = 0;
    int notaggr = 0;
    for(unique_ptr<Expression>& select_field : query_expr){
      aggregate_or_not(select_field, isaggr, notaggr);
    }

    if(isaggr!=0 && notaggr!=0){
      LOG_DEBUG("select field cannot exist with aggragete func when there is no groupby expr");
      return RC::INVALID_ARGUMENT;
    }
  }
  return RC::SUCCESS;
}

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }

  if (nullptr != having_stmt_) {
    delete having_stmt_;
    having_stmt_ = nullptr;
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);
  
  // 对select部分的字段的绑定，主要涉及*绑定为全字段，unboundedfield绑定为field，unboundedaggregate绑定为aggregate
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }
  
  // where谓词的condition部分也可能包含expression, 在stmt层面对其进行重新绑定
  vector<unique_ptr<Expression>> condition_expessions;
  for(ConditionSqlNode& condition_node : select_sql.conditions){
    if(condition_node.left_is_attr == 2){
      // 左值为表达式
      RC rc = expression_binder.bind_expression(condition_node.left_expression, condition_expessions);
      if (OB_FAIL(rc)) {
        LOG_INFO("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
      // 替换左值表达式
      unique_ptr<Expression> &left = condition_expessions[0];
      if (left.get() != condition_node.left_expression.get()) {
        condition_node.left_expression.reset(left.release());
      }
    }
    condition_expessions.clear();
    
    if(condition_node.right_is_attr == 2){
      // 右值为表达式
      RC rc = expression_binder.bind_expression(condition_node.right_expression, condition_expessions);
      if (OB_FAIL(rc)) {
        LOG_INFO("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
      // 替换右值表达式
      unique_ptr<Expression> &right = condition_expessions[0];
      if (right.get() != condition_node.right_expression.get()) {
        condition_node.right_expression.reset(right.release());
      }
    }
  }

  // 绑定groupby部分的表达式
  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 绑定having部分的表达式
  vector<unique_ptr<Expression>> having_expessions;
  for(ConditionSqlNode& condition_node : select_sql.having){
    if(condition_node.left_is_attr == 2){
      // 左值为表达式
      RC rc = expression_binder.bind_expression(condition_node.left_expression, having_expessions);
      if (OB_FAIL(rc)) {
        LOG_INFO("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
      // 替换左值表达式
      unique_ptr<Expression> &left = having_expessions[0];
      if (left.get() != condition_node.left_expression.get()) {
        condition_node.left_expression.reset(left.release());
      }
    }
    having_expessions.clear();
    
    if(condition_node.right_is_attr == 2){
      RC rc = expression_binder.bind_expression(condition_node.right_expression, having_expessions);
      if(OB_FAIL(rc)){
        LOG_INFO("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
      // 替换右值表达式
      unique_ptr<Expression> &right = having_expessions[0];
      if (right.get() != condition_node.right_expression.get()) {
        condition_node.right_expression.reset(right.release());
      }
    }
  }

  // 检查 查询字段，分组字段，过滤字段间是否满足约束条件
  RC rc = RC::SUCCESS;
  // rc = check_select_stmt(bound_expressions, group_by_expressions);
  // if(rc != RC::SUCCESS){
  //   LOG_WARN("check_select_stmt failed");
  //   return rc;
  // }


  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  rc                      = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  FilterStmt* having_stmt = nullptr;
  rc                      = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.having.data(),
      static_cast<int>(select_sql.having.size()),
      having_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct having filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->having_stmt_ = having_stmt;
  stmt                      = select_stmt;
  return RC::SUCCESS;
}
