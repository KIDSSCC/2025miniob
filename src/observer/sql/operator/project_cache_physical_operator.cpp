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

ProjectCachePhysicalOperator::ProjectCachePhysicalOperator(vector<unique_ptr<Expression>> &&expressions, bool is_relevant)
  : expressions_(std::move(expressions)), project_tuple_(expressions_)
{
  is_relevant_ = is_relevant;
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
  LOG_INFO("ProjectCachePhysicalOperator::next, parent_tuple is %d, is_relevant is %d", parent_tuple_ == nullptr, is_relevant_);
  if((is_finished && !is_relevant_) || children_.size() == 0){
    // is_finished代表已经完整迭代过一轮了，对于非相关子查询，此时可以省略本次迭代
    // curr_tuple中也依然维持着上一次的结果
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  child->open(trx_);
  if(is_relevant_){
    // 相关子查询时，需要project_cache将父查询当前的tuple传下去，非相关子查询就不需要了，
    // parent_tuple默认为nullptr，由底层的predicate或者tablescan各自进行处理
    child->set_parent_tuple(this->parent_tuple_);
  }

  LOG_INFO("begin to while");
  RC rc = RC::SUCCESS;
  tuple_.clear();
  while(OB_SUCC(rc = child->next())){
    Tuple *child_tuple = child->current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }
    project_tuple_.set_tuple(child_tuple);

    // 子节点返回的curr_tuple重新copy成一份valuelisttuple
    ValueListTuple child_tuple_to_value;
    rc = ValueListTuple::make(project_tuple_, child_tuple_to_value);
    LOG_INFO("project cache get tuple %s", child_tuple_to_value.to_string().c_str());
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to make valuelist tuple");
      return rc;
    }
    tuple_.add_tuple(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));
  }

  LOG_INFO("final tuple %s", tuple_.to_string().c_str());
  child->close();

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