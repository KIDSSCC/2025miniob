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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, vector<unique_ptr<Expression>>& values, vector<int> value_amount, FilterStmt *filter_stmt)
    : table_(table), value_amount_(value_amount), filter_stmt_(filter_stmt)
{
  values_ = std::move(values);
}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, UpdateSqlNode &update, Stmt *&stmt)
{
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 确定更新的字段在表中的位置
  vector<int> field_indexs;
  const vector<FieldMeta>* field_matas = table->table_meta().field_metas();
  
  for(size_t i=0;i<update.attribute_names.size();i++){
    int field_index = -1;
    for(int j=0;j<(int)((*field_matas).size());j++) {
      auto field_meta = (*field_matas)[j];
      if (0 == strcmp(field_meta.name(), update.attribute_names[i].c_str())) {
        field_index = j;
        break;
      }
    }
    if(field_index < 0) {
      LOG_ERROR("no such field. table=%s, field=%s", table_name, update.attribute_names[i].c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    field_indexs.push_back(field_index);
  }

  BinderContext binder_context;
  binder_context.add_table(table);
  binder_context.set_separate(binder_context.query_tables().size());
  ExpressionBinder expression_binder(binder_context);

  RC rc = RC::SUCCESS;
  int unused_max_table;
  // 更新后的新值由单一的value调整为表达式vector，需要单独进行绑定
  for(size_t i=0;i<update.values.size();i++){
    unique_ptr<Expression>& curr_value = update.values[i];
    if(curr_value->type() == ExprType::SELECT_T){
      // 生成子查询语句
      Stmt *sub_select_stmt = nullptr;
      Expression* expr = curr_value.get();
      rc = SelectStmt::create(db, static_cast<SelectPackExpr*>(expr)->get_node(), sub_select_stmt, &binder_context, &unused_max_table);
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
    }else{
      // 绑定一般表达式
      vector<unique_ptr<Expression>> expressions;
      rc = expression_binder.bind_expression(curr_value, expressions, unused_max_table);
      if (OB_FAIL(rc)) {
        LOG_WARN("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
      // 替换左值表达式
      unique_ptr<Expression> &left = expressions[0];
      if (left.get() != curr_value.get()) {
        curr_value.reset(left.release());
      }
      expressions.clear();
    }
  }

  int unused_relevant = -1;
  // condition字段部分目前全部设置为了表达式，filterstmt中无法对表达式执行绑定，因此需要将绑定过程提到上层stmt中
  function<RC(ConditionSqlNode&, vector<unique_ptr<Expression>>&)> bind_condition_node = [&](ConditionSqlNode& condition_node, vector<unique_ptr<Expression>>& expressions) -> RC{
    function<RC(int, unique_ptr<Expression>&)> check_expr =[&] (int is_attr, unique_ptr<Expression>& expr_node) -> RC{
      RC rc = RC::SUCCESS;
      // update中暂时不考虑子查询的情况，仅对一般表达式进行绑定
      if(is_attr == 2){
        // 左值为表达式
        rc = expression_binder.bind_expression(expr_node, expressions, unused_relevant);
        if (OB_FAIL(rc)) {
          LOG_WARN("bind expression failed. rc=%s", strrc(rc));
          return rc;
        }
        // 替换左值表达式if
        unique_ptr<Expression> &left = expressions[0];
        if (left.get() != expr_node.get()) {
          expr_node.reset(left.release());
        }
      }else{
        LOG_WARN("condition node is not expr");
        return RC::INTERNAL;
      }
      expressions.clear();
      return rc;
    };

    RC rc = RC::SUCCESS;
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
  for(ConditionSqlNode& condition_node : update.conditions){
    
    RC rc = bind_condition_node(condition_node, condition_expessions);
    if(rc != RC::SUCCESS){
      LOG_WARN("Cannot bind condition_node");
      return rc;
    }
  }

  // 创建过滤语句
  unordered_map<string, Table *> table_map;
  table_map.insert(pair<string, Table *>(string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  rc          = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }


  stmt = new UpdateStmt(table, update.values, field_indexs, filter_stmt);
  static_cast<UpdateStmt*>(stmt)->db_ = db;
  return rc;
}
