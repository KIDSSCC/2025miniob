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
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/db/db.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{

  if(table_->is_view()){
    return update_view(trx);
  }else{
    return update_table(trx);
  }
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::update_table(Trx *trx){
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return rc;
  }

  // 调整之后的update，子节点中的最后一个，是tableget或predicate，排在前面的依次是新值中的子查询
  // update语句和delete语句一定会带有一个过滤节点。如果where字段为空，则过滤语句也为空，此时将不对记录进行过滤，返回表中全部的记录
  unique_ptr<PhysicalOperator> &child = children_.back();

  rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(std::move(record));
  }

  child->close();

  vector<Value> new_values;
  rc = generate_new_value(new_values);
  if(rc != RC::SUCCESS){
    LOG_WARN("Failed to generate new value, rc %s", strrc(rc));
    return rc;
  }

  // 先收集记录再进行更新
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如VacuousTrx
  for (Record &record : records_) {
    // 根据旧的record创建新的record
    Record new_record;
    rc = table_->make_record_from_record(record, new_record, field_index_, new_values);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record from record: %s", strrc(rc));
      return rc;
    }


    rc = trx_->update_record(table_, record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}


RC UpdatePhysicalOperator::update_view(Trx *trx){
  RC rc = RC::SUCCESS;

  // 更新视图，需要先通过更新字段情况分析出本次更新所涉及的表。目前只考虑视图关联一张表的情况。多表join后面再说
  vector<string>& src_fields = table_->src_fields();

  Table* related_table = nullptr;
  vector<int> src_field_index_;
  for(size_t i=0;i<field_index_.size();i++){
    int view_field_idx = field_index_[i] - table_->table_meta().sys_field_num();
    string src_field = src_fields[view_field_idx];

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

    // 检查是否是单表更新
    Table* src_table = db_->find_table(table_name.c_str());
    if(related_table == nullptr){
      related_table = src_table;
    }else{
      if(related_table->name() != table_name){
        // 目前仅支持单表更新
        LOG_WARN("The view is not updatable");
        return RC::INTERNAL;
      }
    }

    const vector<FieldMeta>* src_table_fields = src_table->table_meta().field_metas();
    for(size_t j=0;j<src_table_fields->size();j++){
      if(src_table_fields->at(j).name() == field_name){
        src_field_index_.push_back(j);
        break;
      }
    }
  }

  // 先收集所有的待更新的record
  if (children_.empty()) {
    return rc;
  }
  unique_ptr<PhysicalOperator> &child = children_.back();
  rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  trx_ = trx;

  while(OB_SUCC(rc = child->next())){
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    // 在成功拿到tuple的时候，底层也一定转到一个合适的row上了，尝试抓取原始record
    Tuple* curr_tuple = nullptr;
    rc = child->get_row_tuple(related_table, curr_tuple);
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to get row tuple, rc: %s", strrc(rc));
      return rc;
    }
    RowTuple *row_tuple = static_cast<RowTuple *>(curr_tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(std::move(record));
  }
  child->close();

  vector<Value> new_values;
  rc = generate_new_value(new_values);
  if(rc != RC::SUCCESS){
    LOG_WARN("Failed to generate new value, rc %s", strrc(rc));
    return rc;
  }

  LOG_INFO("records_ size: %d", records_.size());

  for(Record &record : records_){
    Record new_record;
    rc = related_table->make_record_from_record(record, new_record, field_index_, new_values);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record from record: %s", strrc(rc));
      return rc;
    }

    rc = trx_->update_record(related_table, record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }

  return rc;
}

RC UpdatePhysicalOperator::generate_new_value(vector<Value>& new_values)
{
  RC rc = RC::SUCCESS;
  if(!records_.empty()){
    // 只有record不为空的情况下再去获取更新值
    for(size_t i=0;i<new_expr_.size();i++){
      unique_ptr<Expression>& expr = new_expr_[i];
      if(expr->type() == ExprType::SELECT_T){
        // 子查询,需要先通过子节点拿到tuple，将tuple解析成值列表
        int node_pos = expr->pos();
        unique_ptr<PhysicalOperator>& sub_oper = children_[node_pos];
        rc = sub_oper->open(trx_);
        if(rc != RC::SUCCESS){
          LOG_WARN("Failed to execute open for child oper");
          return rc;
        }

        rc = sub_oper->next();
        if(rc != RC::SUCCESS){
          LOG_WARN("Failed to execute next for child oper");
          return rc;
        }

        Tuple* sub_tuple = sub_oper->current_tuple();
        if (nullptr == sub_tuple) {
          LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
          return RC::INTERNAL;
        }

        vector<Value> valuelist;
        expr->get_valuelist(*sub_tuple, valuelist);

        if(valuelist.size() > 1){
          LOG_WARN("The number of results returned by the subquery is not 1");
          sub_oper->close();
          return RC::INTERNAL;
        }

        if(valuelist.empty()){
          // valuelist为空集，此时目标字段更新为NULL
          Value null_value;
          null_value.set_type(AttrType::UNDEFINED);
          null_value.set_null();
          new_values.emplace_back(null_value);
        }else{
          // 返回的valuelist中仅有一个元素，是合理的情况
          new_values.emplace_back(std::move(valuelist[0]));
        }
        sub_oper->close();

      }else{
        // 一般表达式, 通常情况下不会和某一字段产生联系，先尝试使用try_get_value获取其值
        Value curr_value;
        expr->try_get_value(curr_value);
        new_values.emplace_back(std::move(curr_value));
      }
    }
  }

  return rc;
}