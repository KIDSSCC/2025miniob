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
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/insert_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/db/db.h"

#include <numeric>

using namespace std;

InsertPhysicalOperator::InsertPhysicalOperator(Table *table, vector<Value> &&values)
    : table_(table), values_(std::move(values))
{}

RC InsertPhysicalOperator::open(Trx *trx)
{
  if(table_->is_view()){
    return insert_view(trx);
  }else{
    return insert_table(trx);
  }
}

RC InsertPhysicalOperator::next() { return RC::RECORD_EOF; }

RC InsertPhysicalOperator::close() { return RC::SUCCESS; }

RC InsertPhysicalOperator::insert_table(Trx* trx){
  Record record;
  RC     rc = table_->make_record(static_cast<int>(values_.size()), values_.data(), record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to make record. rc=%s", strrc(rc));
    return rc;
  }
  rc = trx->insert_record(table_, record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
  }
  return rc;
}

RC InsertPhysicalOperator::insert_view(Trx* trx){
  RC rc = RC::SUCCESS;

  // 向视图的插入，当前存入的values_可能对应不同的表，也可能仅对应的源表的部分列。
  vector<string>& src_fields = table_->src_fields();
  vector<string> all_field_in_view;
  Table* related_table = nullptr;
  Db* db = table_->db();


  // 视图不可更新的一种情况，映射字段中存在聚合函数或表达式计算
  for(size_t i=0;i<src_fields.size();i++){
    string src_field = src_fields[i];
    if(src_field == ""){
      // 不可更新视图
      LOG_WARN("The view is not updatable");
      return RC::INTERNAL;
    }
  }

  // attr_infos为空时，即向视图默认的所有列添加内容。
  // attr_infos不为空时，即仅向视图中部分列添加内容。这些列需来自同一张表
  vector<int> related_field;
  if(attr_infos_.empty()){
    for(size_t i=0;i<src_fields.size();i++){
      related_field.push_back(i);
    }
  }else{
    const vector<FieldMeta> * all_field = table_->table_meta().field_metas();
    int start_idx = table_->table_meta().sys_field_num();
    for(size_t i=0;i<attr_infos_.size();i++){
      string& insert_field_name = attr_infos_[i].name;
      for(size_t j=0;j<all_field->size();j++){
        if(all_field->at(j).name() == insert_field_name){
          related_field.push_back(j - start_idx);
          break;
        }
      }
    }

    if(related_field.size() != attr_infos_.size()){
      LOG_WARN("Failed to parse view field");
      return RC::INTERNAL;
    }
  }

  for(size_t i=0;i<related_field.size();i++){
    string src_field = src_fields[related_field[i]];
    if(src_field == ""){
      // 不可更新视图
      LOG_WARN("The view is not updatable");
      return RC::INTERNAL;
    }

    // 解析关联表中的字段
    size_t dot_pos = src_field.find('.');
    string table_name = "";
    string field_name = "";
    if (dot_pos != std::string::npos){
      table_name = src_field.substr(0, dot_pos);
      field_name = src_field.substr(dot_pos + 1);
    }else{
      LOG_WARN("invalid src_field in view");
      return RC::INTERNAL;
    }

    all_field_in_view.push_back(field_name);

    Table* src_table = db->find_table(table_name.c_str());
    if(related_table == nullptr){
      related_table = src_table;
    }else{
      if(related_table->name() != table_name){
        // 目前仅支持单表更新
        LOG_WARN("The view is not updatable");
        return RC::INTERNAL;
      }
    }
  }

  // 从related_table中获取字段元数据，为期构造新的values
  const vector<FieldMeta>* all_field = related_table->table_meta().field_metas();
  vector<Value> new_values;
  for(size_t i = related_table->table_meta().sys_field_num(); i<all_field->size();i++){
    const FieldMeta& field = all_field->at(i);
    string field_name_in_src_table = field.name();
    
    size_t j = 0;
    for(j=0;j<all_field_in_view.size();j++){
      if(field_name_in_src_table == all_field_in_view[j]){
        // 源表的字段在当前视图中
        new_values.push_back(std::move(values_[j]));
        break;
      }
    }

    if(j == all_field_in_view.size()){
      // 源表的字段不在当前视图中，插入空值
      Value dafault_val;
      dafault_val.set_null(true);
      dafault_val.set_type(field.type());
      new_values.push_back(std::move(dafault_val));
    }
  }

  Record record;
  rc = related_table->make_record(static_cast<int>(new_values.size()), new_values.data(), record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to make record. rc=%s", strrc(rc));
    return rc;
  }
  rc = trx->insert_record(related_table, record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
  }

  
  return rc;
}
