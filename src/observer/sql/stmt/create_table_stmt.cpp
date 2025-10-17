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
// Created by Wangyunlai on 2023/6/13.
//

#include "common/log/log.h"
#include "common/types.h"
#include "sql/stmt/create_table_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "event/sql_debug.h"

RC CreateTableStmt::create(Db *db, CreateTableSqlNode &create_table, Stmt *&stmt)
{
  StorageFormat storage_format = get_storage_format(create_table.storage_format.c_str());
  if (storage_format == StorageFormat::UNKNOWN_FORMAT) {
    return RC::INVALID_ARGUMENT;
  }

  if(create_table.create_type == 0){
    // 普通创建模式，直接创建语句即可
    vector<string> empty_src;
    stmt = new CreateTableStmt(create_table.relation_name, empty_src, create_table.attr_infos, create_table.primary_keys, storage_format, create_table.create_type);
    sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
    return RC::SUCCESS;
  }
  // create_table_select, 目前拥有的是还未形成selectstmt的SelectPackExpr
  RC rc = RC::SUCCESS;
  unique_ptr<Expression>& sub_select = create_table.sub_select;

  Stmt *sub_select_stmt = nullptr;
  Expression* expr = sub_select.get();
  rc = SelectStmt::create(db, static_cast<SelectPackExpr*>(expr)->get_node(), sub_select_stmt);
  if(rc != RC::SUCCESS){
    LOG_WARN("Failed to create sub select node");
    return rc;
  }
  std::unique_ptr<SelectStmt, void(*)(SelectStmt*)> raw(static_cast<SelectStmt*>(sub_select_stmt), manual_destruction);
  static_cast<SelectPackExpr*>(expr)->select_expr_->value_type_ = raw->get_type();
  static_cast<SelectPackExpr*>(expr)->select_expr_->select_stmt_ = std::move(raw);

  // 构建select子查询语句后，需要从中解析出查询字段的名称和类型，构造成vector<AttrInfoSqlNode>用于创建表的结构
  vector<unique_ptr<Expression>>& exprs = static_cast<SelectPackExpr*>(expr)->select_expr_->select_stmt_->query_expressions();
  vector<AttrInfoSqlNode> attrs;
  vector<string> src_fields; 
  for(size_t i=0;i<exprs.size();i++){
    AttrInfoSqlNode attr;
    attr.name = exprs[i]->name();
    attr.type = exprs[i]->value_type();
    attr.length = exprs[i]->value_length();
    attr.allow_null = true; // 默认允许null
    attrs.push_back(attr);

    // 尝试记录字段的来源，用于视图插入更新
    if(exprs[i]->type() == ExprType::FIELD){
      FieldExpr* field_expr = static_cast<FieldExpr*>(exprs[i].get());
      src_fields.push_back(string(field_expr->table_name()) + "." + string(field_expr->field_name()));
    }else{
      // 非FIELD字段，大概率是聚合字段或其他计算表达式，此时该视图不可更新
      src_fields.push_back("");
    }
  }

  // create table可能预先定义了表的结构，需要检查一下子查询返回的结构和声明的结构能不能对得上
  if(!create_table.attr_infos.empty()){
    if(create_table.attr_infos.size() != attrs.size()){
      LOG_WARN("Attribute number not match. create table define %lu, but select return %lu", create_table.attr_infos.size(), attrs.size());
      return RC::SCHEMA_FIELD_MISSING;
    }

    // 对于create_table，其创建字段的同时指定了类型，所以需要同时进行类型检查。对于create view，其在创建的过程中未指定类型，从下层推断的字段中获取类型和长度
    if(create_table.create_type == 1){
      for(size_t i=0;i<attrs.size();i++){
        if(create_table.attr_infos[i].type != attrs[i].type){
          LOG_WARN("Attribute type not match for attr %s. create table define %d, but select return %d", attrs[i].name.c_str(), (int)create_table.attr_infos[i].type, (int)attrs[i].type);
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      }
    }else{
      for(size_t i=0;i<attrs.size();i++){
        create_table.attr_infos[i].type = attrs[i].type;
        create_table.attr_infos[i].length = attrs[i].length;
      }
    }
  }else{
    // 没有预定义表结构，直接使用子查询的结果作为表结构
    create_table.attr_infos = std::move(attrs);
  }


  stmt = new CreateTableStmt(create_table.relation_name, src_fields, create_table.attr_infos, create_table.primary_keys, storage_format, create_table.create_type);
  static_cast<CreateTableStmt*>(stmt)->sub_select = std::move(sub_select);
  static_cast<CreateTableStmt*>(stmt)->db_ = db;
  sql_debug("create table select statement: table name %s", create_table.relation_name.c_str());
  return rc;
}

StorageFormat CreateTableStmt::get_storage_format(const char *format_str) {
  StorageFormat format = StorageFormat::UNKNOWN_FORMAT;
  if (strlen(format_str) == 0) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "ROW")) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "PAX")) {
    format = StorageFormat::PAX_FORMAT;
  } else {
    format = StorageFormat::UNKNOWN_FORMAT;
  }
  return format;
}
