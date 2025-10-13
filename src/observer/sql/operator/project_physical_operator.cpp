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

#include "sql/operator/project_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

ProjectPhysicalOperator::ProjectPhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
  : expressions_(std::move(expressions)), tuple_(expressions_)
{
}

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while(OB_SUCC(rc = child->next())){
    Tuple *child_tuple = child->current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }
    tuple_.set_tuple(child_tuple);
    ValueListTuple child_tuple_to_value;
    rc = ValueListTuple::make(tuple_, child_tuple_to_value);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make tuple to value list. rc=%s", strrc(rc));
      return rc;
    }

    all_tuple.emplace_back(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));
  }

  if(rc != RC::RECORD_EOF && rc != RC::SUCCESS){
    LOG_WARN("Error when open project");
    return rc;
  }
  curr_tuple_ = all_tuple.begin();
  first_emited_ = false;
  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (curr_tuple_ == all_tuple.end()) {
    return RC::RECORD_EOF;
  }
  if (first_emited_) {
    ++curr_tuple_;
  } else {
    first_emited_ = true;
  }
  if (curr_tuple_ == all_tuple.end()) {
    return RC::RECORD_EOF;
  }
  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }

  all_tuple.clear();
  first_emited_ = false;
  return RC::SUCCESS;
}
Tuple *ProjectPhysicalOperator::current_tuple()
{
  if (curr_tuple_ != all_tuple.end()) {
    Tuple* res = (*curr_tuple_).get();
    return res;
  }
  return nullptr;
}

RC ProjectPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  for (const unique_ptr<Expression> &expression : expressions_) {
    schema.append_cell(expression->name());
  }
  return RC::SUCCESS;
}