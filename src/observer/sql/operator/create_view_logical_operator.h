/* Copyright (c) OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/12/26.
//

#pragma once

#include "sql/operator/logical_operator.h"

/**
 * @brief 逻辑算子，用于执行create_view语句
 * @ingroup LogicalOperator
 */
class CreateViewLogicalOperator : public LogicalOperator
{
public:
  CreateViewLogicalOperator() {}
  virtual ~CreateViewLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::CREATE_VIEW; }
  OpType              get_op_type() const override { return OpType::LOGICALCREATE_VIEW; }
  

public:
  Db            *db_ = nullptr;
  string table_name_;
  vector<AttrInfoSqlNode> attr_infos_;
  vector<string>          primary_keys_;
  StorageFormat           storage_format_;

  // 子查询已经转换为了逻辑节点，可能不需要保存原始表达式了
  unique_ptr<Expression> sub_select;
};
