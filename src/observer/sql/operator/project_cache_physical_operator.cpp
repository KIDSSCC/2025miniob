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

ProjectCachePhysicalOperator::ProjectCachePhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
  : expressions_(std::move(expressions)), project_tuple_(expressions_)
{
  // ProjectCachePhysicalOperator和ProjectPhysicalOperator的内部构造保持完全一样即可
}

RC ProjectCachePhysicalOperator::open(Trx *trx)
{
  // open函数不需要做调整，仅打开子算子
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectCachePhysicalOperator::next()
{
  if(is_finished || children_.empty()){
    // 如果已经统计并输出完了，next返回RECORD_EOF
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  RC rc = RC::SUCCESS;
  while(OB_SUCC(rc = child->next())){
    Tuple *child_tuple = child->current_tuple();
    LOG_INFO("in project cache ,child_tuple spec is %s", child_tuple->spec_to_string().c_str());
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }
    project_tuple_.set_tuple(child_tuple);
    LOG_INFO("in project cache ,project_tuple_ spec is %s", project_tuple_.spec_to_string().c_str());

    // 子节点返回的curr_tuple重新copy成一份valuelisttuple
    ValueListTuple child_tuple_to_value;
    rc = ValueListTuple::make(project_tuple_, child_tuple_to_value);
    if(rc != RC::SUCCESS){
      LOG_WARN("Failed to make valuelist tuple");
      return rc;
    }
    tuple_.add_tuple(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));
  }

  is_finished = true;
  return RC::SUCCESS;
}

RC ProjectCachePhysicalOperator::close()
{
  // close部分不需要调整，仅关闭子算子
  if (!children_.empty()) {
    children_[0]->close();
  }
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