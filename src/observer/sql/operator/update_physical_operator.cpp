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

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return rc;
  }

  // 调整之后的update，子节点中的最后一个，是tableget或predicate，排在前面的依次是新值中的子查询
  // update的方案是先把所有的value确定下来，再依次调用tableget从底层获取record，修改record，写回
  vector<Value> new_values;
  for(size_t i=0;i<new_expr_.size();i++){
    unique_ptr<Expression>& expr = new_expr_[i];
    if(expr->type() == ExprType::SELECT_T){
      // 子查询,需要先通过子节点拿到tuple，将tuple解析成值列表
      int node_pos = expr->pos();
      unique_ptr<PhysicalOperator>& sub_oper = children_[node_pos];
      rc = sub_oper->open(trx);
      if(rc != RC::SUCCESS){
        LOG_WARN("Failed to execute open for child oper");
        return rc;
      }

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

      vector<Value> valuelist;
      expr->get_valuelist(*sub_tuple, valuelist);

      if(valuelist.size() > 1){
        LOG_WARN("The number of results returned by the subquery is not 1");
        sub_oper->close();
        return RC::INTERNAL;
      }

      if(new_values.empty()){
        // valuelist为空集，此时目标字段更新为NULL
        Value null_value;
        null_value.set_type(AttrType::UNDEFINED);
        null_value.set_null();
        new_values.emplace_back(null_value);
      }else{
        // 返回的valuelist中仅有一个元素，是合理的情况
        new_values.emplace_back(std::move(valuelist[0]));
      }


    }else{
      // 一般表达式, 通常情况下不会和某一字段产生联系，先尝试使用try_get_value获取其值
      Value curr_value;
      expr->try_get_value(curr_value);
      new_values.emplace_back(std::move(curr_value));
    }
  }


  // update语句和delete语句一定会带有一个过滤节点。如果where字段为空，则过滤语句也为空，此时将不对记录进行过滤，返回表中全部的记录
  unique_ptr<PhysicalOperator> &child = children_.back();

  rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records_.emplace_back(std::move(record));
  }

  child->close();

  // 先收集记录再进行更新
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如VacuousTrx
  for (Record &record : records_) {
    // 根据旧的record创建新的record
    Record new_record;
    rc = table_->make_record_from_record(record, new_record, field_index_, new_values);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record from record: %s", strrc(rc));
      return rc;
    }


    rc = trx_->update_record(table_, record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}
