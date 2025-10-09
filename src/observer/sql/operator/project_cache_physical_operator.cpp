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
// Created by WangYunlai on 2022/07/01.
//

#include "sql/operator/project_cache_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

ProjectCachePhysicalOperator::ProjectCachePhysicalOperator(vector<unique_ptr<Expression>> &&expressions, bool is_relevant, bool is_check)
  : expressions_(std::move(expressions)), project_tuple_(expressions_)
{
  is_relevant_ = is_relevant;
  is_check_ = is_check;
  // ProjectCachePhysicalOperator和ProjectPhysicalOperator的内部构造保持完全一样即可
}

RC ProjectCachePhysicalOperator::open(Trx *trx)
{
  // 打开子算子的功能下放到next部分执行
  trx_ = trx;
  return RC::SUCCESS;
}

RC ProjectCachePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if((is_finished && !is_relevant_) || children_.size() == 0){
    // is_finished代表已经完整迭代过一轮了，对于非相关子查询，此时可以省略本次迭代
    // curr_tuple中也依然维持着上一次的结果
    return rc;
  }

  PhysicalOperator *child = children_[0].get();
  if(is_relevant_){
    // 相关子查询时，需要project_cache将父查询当前的tuple传下去，非相关子查询就不需要了，
    // parent_tuple默认为nullptr，由底层的predicate或者tablescan各自进行处理
    child->set_parent_tuple(this->parent_tuple_);
  }
  // groupby类算子，功能实现放在open中，因此需要在open之前就把parent_tuple传下去，对于predicate和tableget则没有影响。
  rc = child->open(trx_);
  if(rc != RC::SUCCESS){
    LOG_WARN("Project Cache failed to open sub oper");
    child->close();
    return rc;
  }

  tuple_.clear();
  while(OB_SUCC(rc = child->next())){
    Tuple *child_tuple = child->current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      child->close();
      return RC::INTERNAL;
    }
    project_tuple_.set_tuple(child_tuple);

    // 子节点返回的curr_tuple重新copy成一份valuelisttuple
    ValueListTuple child_tuple_to_value;
    rc = ValueListTuple::make(project_tuple_, child_tuple_to_value);
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to make valuelist tuple");
      child->close();
      return rc;
    }
    tuple_.add_tuple(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));
  }
  child->close();

  if(rc != RC::SUCCESS && rc != RC::RECORD_EOF){
    LOG_WARN("Project Cache failed to execute sub oper, rc is %s", strrc(rc));
    return rc;
  }

  is_finished = true;
  return RC::SUCCESS;
}

RC ProjectCachePhysicalOperator::close()
{
  // 关闭子算子的功能上提至next部分
  return RC::SUCCESS;
}
Tuple *ProjectCachePhysicalOperator::current_tuple()
{
  // current_tuple部分需要适当修改
  is_finished = true;
  return &tuple_;
}

RC ProjectCachePhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  for (const unique_ptr<Expression> &expression : expressions_) {
    schema.append_cell(expression->name());
  }
  return RC::SUCCESS;
}