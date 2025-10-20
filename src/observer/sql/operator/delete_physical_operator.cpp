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

#include "sql/operator/delete_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/db/db.h"

RC DeletePhysicalOperator::open(Trx *trx)
{
  if(table_->is_view()){
    return delete_from_view(trx);
  }else{
    return delete_from_table(trx);
  }
}

RC DeletePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC DeletePhysicalOperator::close()
{
  return RC::SUCCESS;
}

RC DeletePhysicalOperator::delete_from_table(Trx *trx){
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
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

  // 先收集记录再删除
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如VacuousTrx
  for (Record &record : records_) {
    rc = trx_->delete_record(table_, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC DeletePhysicalOperator::delete_from_view(Trx *trx){
  RC rc = RC::SUCCESS;
  // 删除视图，情况与update类似但有一定不同。多表join下delete一定failure。单表下，普通字段，表达式字段，聚合字段都可以
  vector<string>& src_fields = table_->src_fields();
  Table* related_table = nullptr;
  Db* db_ = table_->db();

  for(size_t i=0;i<src_fields.size();i++){
    string src_field = src_fields[i];

    // 先忽略此处的表达式字段和聚合字段
    if(src_field == ""){
      continue;
    }

    size_t dot_pos = src_field.find('.');
    string table_name = "";
    if (dot_pos != std::string::npos){
      table_name = src_field.substr(0, dot_pos);
    }else{
      LOG_WARN("invalid src_field in view");
      return RC::INTERNAL;
    }

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
  }

  // for循环正常结束下，证明视图中不包含多表连接(有一种特殊情况，是多表字段的表达式计算，暂时被忽略了)
  // 先收集所有的待删除的record
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

  for (Record &record : records_) {
    rc = trx_->delete_record(related_table, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}
