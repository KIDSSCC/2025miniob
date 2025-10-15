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

#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/operator/project_physical_operator.h"

/**
 * @brief 选择/投影物理算子
 * @ingroup PhysicalOperator
 */
class PipelineCachePhysicalOperator : public PhysicalOperator
{
public:
  PipelineCachePhysicalOperator();

  virtual ~PipelineCachePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::PIPELINE_CACHE; }
  OpType               get_op_type() const override { return OpType::PIPELINE_CACHE; }

  virtual double calculate_cost(
      LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
  {
    return (cm->cpu_op()) * prop->get_card();
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const {
    ProjectPhysicalOperator* content_ = static_cast<ProjectPhysicalOperator*>(children_[0].get());
    return content_->cell_num(); 
  }

  Tuple *current_tuple() override;

  RC tuple_schema(TupleSchema &schema) const override;

  vector<unique_ptr<Expression>>& expressions() { 
    ProjectPhysicalOperator* content_ = static_cast<ProjectPhysicalOperator*>(children_[0].get());
    return content_->expressions(); 
  }

private:

  vector<unique_ptr<ValueListTuple>>                  all_tuple;
  vector<unique_ptr<ValueListTuple>>::iterator        curr_tuple_;
  bool                                                first_emited_ = false;
};
