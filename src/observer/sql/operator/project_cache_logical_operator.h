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
// Created by WangYunlai on 2022/12/08.
//

#pragma once

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"

/**
 * @brief project cache 表示一类特殊的投影运算，将所有结果缓存后形成一个完整的tuple传递至上层，用于子查询
 * @ingroup LogicalOperator
 * @details 从表中获取数据后，可能需要过滤，投影，连接等等。
 */
class ProjectCacheLogicalOperator : public LogicalOperator
{
public:
  ProjectCacheLogicalOperator(vector<unique_ptr<Expression>> &&expressions, bool is_relevant);
  virtual ~ProjectCacheLogicalOperator() = default;

  LogicalOperatorType         type() const override { return LogicalOperatorType::PROJECTION_CACHE; }
  OpType                      get_op_type() const override { return OpType::LOGICALPROJECTION_CACHE; }
  unique_ptr<LogicalProperty> find_log_prop(const vector<LogicalProperty *> &log_props) override;

  vector<unique_ptr<Expression>>       &expressions() { return expressions_; }
  const vector<unique_ptr<Expression>> &expressions() const { return expressions_; }

  bool is_relevant() const { return is_relevant_; }
public:
  bool is_relevant_;
};
