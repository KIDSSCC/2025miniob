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

#include "sql/operator/create_table_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/db/db.h"

RC CreateTablePhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  // 先创建好表的结构，再逐一遍历底层算子插入数据
  rc = db_->create_table(table_name_.c_str(), attr_infos_, primary_keys_, storage_format_);

  if(children_.empty()){
    return RC::INTERNAL;
  }

  PhysicalOperator *child = children_[0].get();
  rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while(OB_SUCC(rc = child->next())){
    Tuple *child_tuple = child->current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }

    vector<Value> values;
    int cell_num = child_tuple->cell_num();
    for(int i=0;i<cell_num;i++){
      Value value;
      rc = child_tuple->cell_at(i, value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get cell at %d. rc=%s", i, strrc(rc));
        return rc;
      }
      values.push_back(value);
    }

    Table *table = db_->find_table(table_name_.c_str());
    if (nullptr == table) {
      LOG_WARN("no such table. table name=%s", table_name_.c_str());
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    Record record;
    rc = table->make_record(static_cast<int>(values.size()), values.data(), record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record. rc=%s", strrc(rc));
      return rc;
    }
    rc = trx->insert_record(table, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
      return rc;
    }
  }

  if (rc != RC::RECORD_EOF  && rc != RC::SUCCESS) {
    LOG_WARN("failed to get next tuple from child operator. rc=%s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC CreateTablePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC CreateTablePhysicalOperator::close()
{
  if(!children_.empty()){
    return children_[0]->close();
  }
  return RC::SUCCESS;
}
