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
// Created by Wangyunlai on 2024/5/31.
//

#pragma once

#include "common/lang/vector.h"
#include "sql/expr/tuple.h"
#include "common/value.h"
#include "common/sys/rc.h"

// ExpressionTuple记录了若干表达式（ExprPointerType一般会是Expression*)
// 其中的每一个cell是一个表达式的值，表达式的值在计算时需要结合child_tuple_完成

template <typename ExprPointerType>
class ExpressionTuple : public Tuple
{
public:
  ExpressionTuple(const vector<ExprPointerType> &expressions) : expressions_(expressions) {}
  virtual ~ExpressionTuple() = default;

  TupleType type() const override { return TupleType::EXPRESSION; }

  void set_tuple(const Tuple *tuple) { child_tuple_ = tuple; }

  const Tuple *get_tuple() const { return child_tuple_; }

  int cell_num() const override { return static_cast<int>(expressions_.size()); }

  RC cell_at(int index, Value &cell) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }

    const ExprPointerType &expression = expressions_[index];
    return get_value(expression, cell);
  }

  RC spec_at(int index, TupleCellSpec &spec) const override
  {
    if (index < 0 || index >= cell_num()) {
      return RC::INVALID_ARGUMENT;
    }

    // ExpressionTuple在构造投影算子时，记录的TupleCellSpec信息只声明了别名，忽略了表名和字段名
    const ExprPointerType &expression = expressions_[index];
    spec                              = TupleCellSpec(expression->name());
    return RC::SUCCESS;
  }

  RC find_cell(const TupleCellSpec &spec, Value &cell) const override
  {
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = child_tuple_->find_cell(spec, cell);
      if (OB_SUCC(rc)) {
        return rc;
      }
    }

    rc = RC::NOTFOUND;
    for (const ExprPointerType &expression : expressions_) {
      if (0 == strcmp(spec.alias(), expression->name())) {
        rc = get_value(expression, cell);
        break;
      }
    }

    return rc;
  }

private:
  RC get_value(const ExprPointerType &expression, Value &value) const
  {
    LOG_INFO("get value for expression %s, type is %d", expression->name(), expression->type());
    RC rc = RC::SUCCESS;
    if (child_tuple_ != nullptr) {
      rc = expression->get_value(*child_tuple_, value);
    } else {
      rc = expression->try_get_value(value);
    }
    return rc;
  }

private:
  const vector<ExprPointerType> &expressions_;
  const Tuple                   *child_tuple_ = nullptr;
};
