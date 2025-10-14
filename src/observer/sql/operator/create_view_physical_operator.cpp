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

#include "sql/operator/create_view_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/db/db.h"

RC CreateViewPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  // 先创建好表的结构，再逐一遍历底层算子插入数据
  rc = db_->create_table(table_name_.c_str(), attr_infos_, primary_keys_, storage_format_);
  LOG_INFO("create table %s", table_name_.c_str());
  for(size_t i=0;i<attr_infos_.size();i++){
    const AttrInfoSqlNode &attr = attr_infos_[i];
    LOG_INFO("attr type is %d, name is %s", attr.type, attr.name.c_str());
  }

  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create view table %s. rc=%s", table_name_.c_str(), strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC CreateViewPhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC CreateViewPhysicalOperator::close()
{
  return RC::SUCCESS;
}
