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

#pragma once

#include "common/lang/unordered_map.h"
#include "common/lang/vector.h"
#include "sql/expr/expression.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class Db;
class Table;
class FieldMeta;

// 用于比较的对象，可能是表中的某一字段，也可能是某一常量值
struct FilterObj
{
  int  is_attr;
  Field field;
  Value value;
  unique_ptr<Expression> expr;

  void init_attr(const Field &field)
  {
    is_attr     = 1;
    this->field = field;
  }

  void init_value(const Value &value)
  {
    is_attr     = 0;
    this->value = value;
  }

  void init_expr(unique_ptr<Expression> expr){
    is_attr     = 2;
    this->expr  = move(expr);
  }

};

// 最小的比较单元，包含左值，比较，右值
class FilterUnit
{
public:
  FilterUnit() = default;
  ~FilterUnit() {}

  void set_comp(CompOp comp) { comp_ = comp; }

  CompOp comp() const { return comp_; }

  void set_left(FilterObj &obj) { left_ = std::move(obj); }
  void set_right(FilterObj &obj) { right_ = std::move(obj); }
  void set_conjunction(int type) { conjunction_forward = type; }

  const FilterObj &left() const { return left_; }
  const FilterObj &right() const { return right_; }
  const int conjunction_type() const { return conjunction_forward; }

private:
  CompOp    comp_ = NO_OP;
  FilterObj left_;
  FilterObj right_;
  int conjunction_forward = -1;
};

/**
 * @brief Filter/谓词/过滤语句
 * @ingroup Statement
 */
class FilterStmt
{
public:
  FilterStmt() = default;
  virtual ~FilterStmt();

public:
  const vector<FilterUnit *> &filter_units() const { return filter_units_; }

public:
  static RC create(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
      const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt);

  static RC create_filter_unit(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
      const ConditionSqlNode &condition, FilterUnit *&filter_unit);

private:
  vector<FilterUnit *> filter_units_;  // 默认当前都是AND关系
};
