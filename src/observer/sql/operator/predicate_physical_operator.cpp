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

#include "sql/operator/predicate_physical_operator.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/field/field.h"
#include "storage/record/record.h"

PredicatePhysicalOperator::PredicatePhysicalOperator(std::unique_ptr<Expression> expr) : expression_(std::move(expr))
{
  // 谓词算子用于过滤的表达式，必须能够针对一个tuple进行bool类型的判断，从而选择是否接收
  ASSERT(expression_->value_type() == AttrType::BOOLEANS, "predicate's expression should be BOOLEAN type");
}

RC PredicatePhysicalOperator::open(Trx *trx)
{
  if (children_.size() < 1) {
    LOG_WARN("predicate operator must has at least one child");
    return RC::INTERNAL;
  }

  // 谓词算子的子算子，一般可以是table_get算子
  // 子查询的情况下，子算子也可能是projectcache算子
  RC rc = RC::SUCCESS;
  for(size_t i=0;i<children_.size();i++){
    rc = children_[i]->open(trx);
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to open child oper");
      return rc;
    }
  }
  return rc;
}

RC PredicatePhysicalOperator::next()
{
  RC                rc   = RC::SUCCESS;
  PhysicalOperator *oper = children_.back().get();
  oper->set_parent_tuple(parent_tuple_);
  // 有关子查询场景下的predicate遍历逻辑，核心是围绕predicate自身底层的table_get(或join)展开遍历
  // 每从其中获取到一个tuple，再访问其他的projectcache子算子，得到子查询的结果。再将子查询的结果和自身的tuple一起送入bool判断

  while (RC::SUCCESS == (rc = oper->next())) {
    // 得到自身底层的算子返回的tuple
    Tuple *tuple = oper->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }

    // 遍历访问所有子查询的算子返回的tuple
    CompositeTuple sub_query_tuple;
    for(size_t i=0; i<children_.size()-1;i++){
      // 子查询的project是一定能在一轮next+curr_tuple下拿到结果的
      PhysicalOperator *sub_oper = children_[i].get();
      LOG_INFO("prepare to set parent tuple %s",tuple->to_string().c_str());
      sub_oper->set_parent_tuple(tuple);
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

      ValueListTuple child_tuple_to_value;
      rc = ValueListTuple::make(*sub_tuple, child_tuple_to_value);
      LOG_INFO("in predicate sub query get tuple %s", child_tuple_to_value.to_string().c_str());
      sub_query_tuple.add_tuple(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));
    }

    // CompositeTuple中再把tuple推进去
    ValueListTuple origin_tuple;
    rc = ValueListTuple::make(*tuple, origin_tuple);
    sub_query_tuple.add_tuple(make_unique<ValueListTuple>(std::move(origin_tuple)));

    // CompositeTuple中还需要把父查询传进来的parent_tuple推进来
    if(parent_tuple_ != nullptr){
      ValueListTuple parent_tuple_pack;
      rc = ValueListTuple::make(*parent_tuple_, parent_tuple_pack);
      sub_query_tuple.add_tuple(make_unique<ValueListTuple>(std::move(parent_tuple_pack)));
    }

    // 谓词算子的表达式一般可以是 ConjunctionExpr，通过ConjunctionExpr计算tuple的值
    Value value;
    rc = expression_->get_value(sub_query_tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    // 不断循环，直至找到一个使 ConjunctionExpr 表达式为true的tuple
    if (value.get_boolean()) {
      return rc;
    }
  }
  return rc;
}

RC PredicatePhysicalOperator::close()
{
  for(size_t i=0;i<children_.size();i++){
    children_[i]->close();
  }
  return RC::SUCCESS;
}

Tuple *PredicatePhysicalOperator::current_tuple() { 
  // 当前 子算子中的最后一个代表了本查询真正的底层算子，其余均为子查询
  return children_.back()->current_tuple(); 
}

RC PredicatePhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  return children_.back()->tuple_schema(schema);
}
