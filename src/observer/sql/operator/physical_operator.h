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
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/expr/tuple.h"
#include "sql/operator/operator_node.h"

class Record;
class TupleCellSpec;
class Trx;

/**
 * @brief 物理算子
 * @defgroup PhysicalOperator
 * @details 物理算子描述执行计划将如何执行，比如从表中怎么获取数据，如何做投影，怎么做连接等
 */

/**
 * @brief 物理算子类型
 * @ingroup PhysicalOperator
 */
enum class PhysicalOperatorType
{
  PIPELINE_CACHE,
  TABLE_SCAN,
  TABLE_SCAN_VEC,
  INDEX_SCAN,
  NESTED_LOOP_JOIN,
  HASH_JOIN,
  EXPLAIN,
  PREDICATE,
  PREDICATE_VEC,
  PROJECT,
  PROJECT_CACHE,
  PROJECT_VEC,
  ORDER_BY,
  CALC,
  STRING_LIST,
  DELETE,
  UPDATE,
  INSERT,
  SCALAR_GROUP_BY,
  HASH_GROUP_BY,
  GROUP_BY_VEC,
  AGGREGATE_VEC,
  EXPR_VEC,
  CREATE_TABLE,
  CREATE_VIEW,
  VIEW_TRANSLATE,
  NOTHING
};

inline const char *PhysicalOperatorType_to_string(PhysicalOperatorType type) {
  switch (type) {
    case PhysicalOperatorType::TABLE_SCAN:       return "TABLE_SCAN";
    case PhysicalOperatorType::TABLE_SCAN_VEC:   return "TABLE_SCAN_VEC";
    case PhysicalOperatorType::INDEX_SCAN:       return "INDEX_SCAN";
    case PhysicalOperatorType::NESTED_LOOP_JOIN: return "NESTED_LOOP_JOIN";
    case PhysicalOperatorType::HASH_JOIN:        return "HASH_JOIN";
    case PhysicalOperatorType::EXPLAIN:          return "EXPLAIN";
    case PhysicalOperatorType::PREDICATE:        return "PREDICATE";
    case PhysicalOperatorType::PREDICATE_VEC:    return "PREDICATE_VEC";
    case PhysicalOperatorType::PROJECT:          return "PROJECT";
    case PhysicalOperatorType::PROJECT_CACHE:    return "PROJECT_CACHE";
    case PhysicalOperatorType::PROJECT_VEC:      return "PROJECT_VEC";
    case PhysicalOperatorType::ORDER_BY:         return "ORDER_BY";
    case PhysicalOperatorType::CALC:             return "CALC";
    case PhysicalOperatorType::STRING_LIST:      return "STRING_LIST";
    case PhysicalOperatorType::DELETE:           return "DELETE";
    case PhysicalOperatorType::UPDATE:           return "UPDATE";
    case PhysicalOperatorType::INSERT:           return "INSERT";
    case PhysicalOperatorType::SCALAR_GROUP_BY:  return "SCALAR_GROUP_BY";
    case PhysicalOperatorType::HASH_GROUP_BY:    return "HASH_GROUP_BY";
    case PhysicalOperatorType::GROUP_BY_VEC:     return "GROUP_BY_VEC";
    case PhysicalOperatorType::AGGREGATE_VEC:    return "AGGREGATE_VEC";
    case PhysicalOperatorType::EXPR_VEC:         return "EXPR_VEC";
    case PhysicalOperatorType::CREATE_TABLE:    return "CREATE_TABLE";
    case PhysicalOperatorType::CREATE_VIEW:     return "CREATE_VIEW";
    case PhysicalOperatorType::NOTHING:           return "NOTHING";
    case PhysicalOperatorType::PIPELINE_CACHE:    return "PIPELINE_CACHE";
    case PhysicalOperatorType::VIEW_TRANSLATE:   return "VIEW_TRANSLATE";
    default:                                     return "UNKNOWN";
  }
}

/**
 * @brief 与LogicalOperator对应，物理算子描述执行计划将如何执行
 * @ingroup PhysicalOperator
 */
class PhysicalOperator : public OperatorNode
{
public:
  PhysicalOperator() = default;

  virtual ~PhysicalOperator() = default;

  /**
   * 这两个函数是为了打印时使用的，比如在explain中
   */
  virtual string name() const;
  virtual string param() const;

  bool is_physical() const override { return true; }
  bool is_logical() const override { return false; }

  virtual PhysicalOperatorType type() const = 0;

  virtual RC open(Trx *trx) = 0;
  virtual RC next() { return RC::UNIMPLEMENTED; }
  virtual RC next(Chunk &chunk) { return RC::UNIMPLEMENTED; }
  virtual RC close() = 0;

  virtual Tuple *current_tuple() { return nullptr; }

  virtual RC tuple_schema(TupleSchema &schema) const { return RC::UNIMPLEMENTED; }

  void add_child(unique_ptr<PhysicalOperator> oper) { children_.emplace_back(std::move(oper)); }

  virtual vector<unique_ptr<PhysicalOperator>> &children() { return children_; }

  void set_parent_tuple(const Tuple* parent_tuple){ parent_tuple_ = parent_tuple; }

  void print_tree(int depth = 0);

  virtual RC need_row() { return RC::SUCCESS; }

  virtual RC get_row_tuple(Table* table, Tuple*& tuple) { return RC::UNIMPLEMENTED; }

protected:
  vector<unique_ptr<PhysicalOperator>> children_;
  const Tuple*                          parent_tuple_ = nullptr;
};
