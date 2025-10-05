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
// Created by WangYunlai on 2024/05/30.
//

#include "common/log/log.h"
#include "sql/operator/scalar_group_by_physical_operator.h"
#include "sql/expr/expression_tuple.h"
#include "sql/expr/composite_tuple.h"

using namespace std;
using namespace common;

// 全局聚合，没有分组字段
ScalarGroupByPhysicalOperator::ScalarGroupByPhysicalOperator(vector<Expression *> &&expressions)
    : GroupByPhysicalOperator(std::move(expressions))
{}

RC ScalarGroupByPhysicalOperator::open(Trx *trx)
{
  // groupby 算子的子节点一般为tableget或predicate
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }
  if(parent_tuple_ != nullptr){
    child.set_parent_tuple(parent_tuple_);
  }

  // value_expressions_: 内部的聚合字段
  ExpressionTuple<Expression *> group_value_expression_tuple(value_expressions_);

  ValueListTuple group_by_evaluated_tuple;

  // 将聚合器初始化的时机提前至子节点next之前，避免出现子节点无tuple返回的情况
  AggregatorList aggregator_list;
  create_aggregator_list(aggregator_list);
  CompositeTuple composite_tuple;
  group_value_ = make_unique<GroupValueType>(std::move(aggregator_list), std::move(composite_tuple));

  bool init = false;
  while (OB_SUCC(rc = child.next())) {
    Tuple *child_tuple = child.current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }

    // 计算需要做聚合的值
    group_value_expression_tuple.set_tuple(child_tuple);
    if(!init){
      ValueListTuple child_tuple_to_value;
      rc = ValueListTuple::make(*child_tuple, child_tuple_to_value);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to make tuple to value list. rc=%s", strrc(rc));
        return rc;
      }
      get<1>(*group_value_).add_tuple(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));

      init = true;
    }
    
    rc = aggregate(get<0>(*group_value_), group_value_expression_tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to aggregate values. rc=%s", strrc(rc));
      return rc;
    }
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  }

  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));
    return rc;
  }

  // 得到最终聚合后的值
  if (group_value_) {
    // evaluate计算的过程中，会向group_value_的CompositeTuple中添加一个新的ValueListTuple,
    // 最终，CompositeTuple中会有两个ValueListTuple, 其一为该组第一个tuple的完整数据，其二为最终聚合的结果。
    // 该组的第一个完整tuple，据猜测应该是标识该组聚合结果，在无分组字段的scalar_group_by中，这个tuple没有什么意义
    rc = evaluate(*group_value_);
  }

  emitted_ = false;
  return rc;
}

RC ScalarGroupByPhysicalOperator::next()
{
  if (group_value_ == nullptr || emitted_) {
    return RC::RECORD_EOF;
  }

  emitted_ = true;

  return RC::SUCCESS;
}

RC ScalarGroupByPhysicalOperator::close()
{
  group_value_.reset();
  emitted_ = false;
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *ScalarGroupByPhysicalOperator::current_tuple()
{
  if (group_value_ == nullptr) {
    return nullptr;
  }

  return &get<1>(*group_value_);
}