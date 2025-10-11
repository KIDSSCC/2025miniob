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


using namespace std;
using namespace common;


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

void manual_destruction(SelectStmt* stmt){
  if(stmt != nullptr){
    delete stmt;
    stmt = nullptr;
  }
}

void manual_destruction(FilterStmt* stmt){
  if(stmt != nullptr){
    delete stmt;
    stmt = nullptr;
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt, BinderContext* parent_bind_context, int* max_table_index)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;
  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;

  // 将join部分涉及的所有table，获取其table指针绑定到relation_node上。这一步绑定的指针将直接用于逻辑计划生成阶段，生成tableget和join算子
  function<RC(unique_ptr<RelationNode>&)> bind_table_ptr = [&](unique_ptr<RelationNode>&relation_node) -> RC{
    if(!relation_node->is_join){
      relation_node->table_ptr = db->find_table(relation_node->table_name.c_str());
      if(relation_node->table_alias != ""){
        // 声明了一个表的别名
        bool alias_repeat = binder_context.add_table_alias(relation_node->table_name, relation_node->table_alias);
        if(alias_repeat){
          LOG_WARN("Table alias %s is already used", relation_node->table_alias.c_str());
          return RC::INTERNAL;
        }
      }
    }else{
      if (relation_node->left) {
        rc = bind_table_ptr(relation_node->left);
        if(rc != RC::SUCCESS){
          return rc;
        }
      }
      if (relation_node->right) {
        rc = bind_table_ptr(relation_node->right);
        if(rc != RC::SUCCESS){
          return rc;
        }
      }
    }
    return RC::SUCCESS;
  };

  
  vector<string> table_names;
  rc = bind_table_ptr(select_sql.relations);
  if(rc != RC::SUCCESS){
    LOG_WARN("Failed to bind table ptr");
    return rc;
  }
  select_sql.relations->get_all_tables(table_names);

  for (size_t i = 0; i < table_names.size(); i++) {
    const char *table_name = table_names[i].c_str();
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

  // BinderContext only_oneself_context(binder_context);
  // binder_context中以separate作为分界线，前一部分为子查询相关的表，后一部分为父查询相关的表
  binder_context.set_separate(binder_context.query_tables().size());
  if(parent_bind_context){
    // 继承来自父查询的表指针
    for(size_t i=0;i<parent_bind_context->query_tables().size();i++){
      Table* parent_table = parent_bind_context->query_tables()[i];
      binder_context.add_table(parent_table);
      table_map.insert({parent_table->name(), parent_table});
    }
    // 继承来自父查询的表别名
    for(size_t i=0;i<parent_bind_context->table_aliases().size();i++){
      binder_context.add_table_alias(parent_bind_context->table_aliases()[i].second, parent_bind_context->table_aliases()[i].first);
    }
    // 继承来自父查询的字段别名
    for(size_t i=0;i<parent_bind_context->field_aliases().size();i++){
      binder_context.add_field_alias(parent_bind_context->field_aliases()[i].second, parent_bind_context->field_aliases()[i].first);
    }
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);
  int max_table = -1;
  
  // 对select部分的字段的绑定，主要涉及*绑定为全字段，unboundedfield绑定为field，unboundedaggregate绑定为aggregate
  expression_binder.allow_alias_declare = true;
  expression_binder.allow_alias_identify = false;
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    rc = expression_binder.bind_expression(expression, bound_expressions, max_table);
    if (OB_FAIL(rc)) {
      LOG_WARN("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }
  expression_binder.allow_alias_declare = false;
  expression_binder.allow_alias_identify = false;

  function<RC(ConditionSqlNode&, vector<unique_ptr<Expression>>&)> bind_condition_node = [&](ConditionSqlNode& condition_node, vector<unique_ptr<Expression>>& expressions) -> RC{
    function<RC(int, unique_ptr<Expression>&)> check_expr =[&] (int is_attr, unique_ptr<Expression>& expr_node) -> RC{
      if(is_attr == 2 && expr_node->type() == ExprType::SELECT_T){
        // 对子查询表达式的特殊绑定: 子查询表达式生成一个单独的语句，保存在expr中
        Stmt *sub_select_stmt = nullptr;
        Expression* expr = expr_node.get();
        rc = SelectStmt::create(db, static_cast<SelectPackExpr*>(expr)->get_node(), sub_select_stmt, &binder_context, &max_table);
        if(rc != RC::SUCCESS){
          LOG_WARN("Failed to create sub select node");
          return rc;
        }

        std::unique_ptr<SelectStmt, void(*)(SelectStmt*)> raw(static_cast<SelectStmt*>(sub_select_stmt), manual_destruction);
        bool is_scalar = false;
        if(raw->query_expressions().size() == 1){
          if(raw->query_expressions()[0]->type() == ExprType::AGGREGATION){
            // 子查询的查询字段仅有一个聚合字段时，标记该子查询为标量子查询
            is_scalar = true;
          }
        }
        static_cast<SelectPackExpr*>(expr)->select_expr_->value_type_ = raw->get_type();
        static_cast<SelectPackExpr*>(expr)->select_expr_->select_stmt_ = std::move(raw);
        static_cast<SelectPackExpr*>(expr)->select_expr_->is_scalar_ = is_scalar;

      } else if(is_attr == 2){
        // 左值为表达式
        rc = expression_binder.bind_expression(expr_node, expressions, max_table);
        if (OB_FAIL(rc)) {
          LOG_WARN("bind expression failed. rc=%s", strrc(rc));
          return rc;
        }
        // 替换左值表达式
        unique_ptr<Expression> &left = expressions[0];
        if (left.get() != expr_node.get()) {
          expr_node.reset(left.release());
        }
      }
      expressions.clear();
      return rc;
    };

    rc = check_expr(condition_node.left_is_attr, condition_node.left_expressions);
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to check_expr");
      return rc;
    }

    rc = check_expr(condition_node.right_is_attr, condition_node.right_expressions);
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to check_expr");
      return rc;
    }

    return rc;
  };
  
  // where谓词的condition部分也可能包含expression, 在stmt层面对其进行重新绑定
  vector<unique_ptr<Expression>> condition_expessions;
  for(ConditionSqlNode& condition_node : select_sql.conditions){
    rc = bind_condition_node(condition_node, condition_expessions);
    if(rc != RC::SUCCESS){
      LOG_WARN("Cannot bind condition_node");
      return rc;
    }
  }

  // 绑定groupby部分的表达式, 暂时认为groupby部分不会出现子查询
  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    rc = expression_binder.bind_expression(expression, group_by_expressions, max_table);
    if (OB_FAIL(rc)) {
      LOG_WARN("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 绑定having部分的表达式
  vector<unique_ptr<Expression>> having_expessions;
  for(ConditionSqlNode& condition_node : select_sql.having){
    rc = bind_condition_node(condition_node, having_expessions);
    if(rc != RC::SUCCESS){
      LOG_WARN("Cannot bind condition_node");
      return rc;
    }
  }

  // 绑定order by部分的表达式, 暂时认为orderby部分不会出现子查询
  vector<unique_ptr<Expression>> orderby_expessions;
  expression_binder.allow_alias_declare = false;
  expression_binder.allow_alias_identify = true;
  LOG_INFO("prepare to bind order by expressions");
  for(pair<Order, unique_ptr<Expression>>& order_field : select_sql.order_by){
    rc = expression_binder.bind_expression(order_field.second, orderby_expessions, max_table);
    if (OB_FAIL(rc)) {
      LOG_WARN("bind order by expression failed. rc=%s", strrc(rc));
      return rc;
    }
    unique_ptr<Expression> &bounded_expr = orderby_expessions[0];
    if(bounded_expr.get()!=order_field.second.get()){
      order_field.second.reset(bounded_expr.release());
    }
    orderby_expessions.clear();
  }
  expression_binder.allow_alias_declare = false;
  expression_binder.allow_alias_identify = false;

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // 对join部分的条件谓词，生成过滤语句
  function<RC(unique_ptr<RelationNode>&)> generate_filter = [&](unique_ptr<RelationNode>& relation_node) -> RC{
    if(relation_node->is_join){
      // on条件部分的表达式也需要进行绑定
      vector<unique_ptr<Expression>> on_condition_expessions;
      for(ConditionSqlNode& condition_node : relation_node->join_conditions){
        rc = bind_condition_node(condition_node, condition_expessions);
        if(rc != RC::SUCCESS){
          LOG_WARN("Cannot bind condition_node");
          return rc;
        }
      }

      FilterStmt *filter_stmt_ = nullptr;
      rc = FilterStmt::create(db, default_table, &table_map, 
        relation_node->join_conditions.data(), static_cast<int>(relation_node->join_conditions.size()), filter_stmt_);
      
      if(rc!=RC::SUCCESS){
        LOG_WARN("cannot construct filter stmt about inner join");
        return rc;
      }
      std::unique_ptr<FilterStmt, void(*)(FilterStmt*)> raw(filter_stmt_, &manual_destruction);
      relation_node->filter_stmt = std::move(raw);

      // 递归遍历左右节点
      if(relation_node->left){
        rc = generate_filter(relation_node->left);
        if(rc!=RC::SUCCESS){
          LOG_WARN("cannot construct filter stmt about inner join, left node");
          return rc;
        }
      }

      if(relation_node->right){
        rc = generate_filter(relation_node->right);
        if(rc!=RC::SUCCESS){
          LOG_WARN("cannot construct filter stmt about inner join, right node");
          return rc;
        }
      }
    }
    return rc;
  };

  rc = generate_filter(select_sql.relations);
  if(rc!=RC::SUCCESS){
    LOG_WARN("cannot generate filter for inner join");
    return rc;
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

  SelectStmt *select_stmt = new SelectStmt();

  // 先将having字段中涉及的表达式拷贝一份，用于在逻辑计划生成的时候，绑定聚合字段。
  function<RC(unique_ptr<Expression>&)> collector = [&](unique_ptr<Expression> &expr) -> RC {
    if (expr->type() == ExprType::AGGREGATION) {
      select_stmt->having_expressions_.emplace_back(move(expr->copy()));
      select_stmt->having_expressions_.back()->set_name(expr->name());
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, collector);
    return rc;
  };


  for(size_t i=0;i<select_sql.having.size();i++){
    ConditionSqlNode& curr_condition_node = select_sql.having[i];
    if(curr_condition_node.left_is_attr == 2){
      collector(curr_condition_node.left_expressions);
    }

    if(curr_condition_node.right_is_attr == 2){
      collector(curr_condition_node.right_expressions);
    }
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
  for(size_t i=0;i<select_sql.order_by.size();i++){
    select_stmt->order_by_.emplace_back(select_sql.order_by[i].first, std::move(select_sql.order_by[i].second));
  }
  bool is_relevant = false;
  if(max_table > 0 && max_table >= binder_context.separate()){
    // max_table大于separate，说明当前查询引用到了父查询乃至更早查询的表，标记当前select为相关查询
    is_relevant = true;
  }

  if(max_table_index != nullptr){
    // 将max_table传递给更上层，例如A->B->C, 其中C跨层引用了A，此处C将max_table传给B，B判断出自己也应该是relevant
    *max_table_index = max_table - binder_context.separate();
  }
  select_stmt->tables_.swap(tables);
  select_stmt->relations_ = move(select_sql.relations);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->having_stmt_ = having_stmt;
  select_stmt->is_relevant_ = is_relevant;
  stmt                      = select_stmt;
  return rc;
}

AttrType SelectStmt::get_type(){
  if(query_expressions_.empty()){
    return AttrType::UNDEFINED;
  }else{
    unique_ptr<Expression>& first_field = query_expressions_[0];
    return first_field->value_type();
  }
}
