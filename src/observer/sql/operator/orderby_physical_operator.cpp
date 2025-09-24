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

#include "sql/operator/orderby_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

OrderByPhysicalOperator::OrderByPhysicalOperator(vector<unique_ptr<Expression>> &&expressions, vector<int> &&order)
  : expressions_(std::move(expressions)), order_(std::move(order))
{
}

RC OrderByPhysicalOperator::open(Trx *trx)
{
  // order算子open阶段需要做的事情会有一点多，首先需要调用子算子的next获取全部的数据，报错在自己的all_tuple中
  // 随后需要根据排序字段和排序顺序，对all_tuple中的记录进行排序
  // 比较麻烦的地方在于排序器的生成，以及暂时不确定待排序字段怎样对应到valuelist中
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  // 提取子节点中所有的tuple
  while(OB_SUCC(rc = child->next())){
    Tuple *child_tuple = child->current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }

    ValueListTuple child_tuple_to_value;
    rc = ValueListTuple::make(*child_tuple, child_tuple_to_value);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make tuple to value list. rc=%s", strrc(rc));
      return rc;
    }
    all_tuple.emplace_back(make_unique<ValueListTuple>(std::move(child_tuple_to_value)));
  }
  
  // 构建排序规则
  function<bool(unique_ptr<ValueListTuple>&, unique_ptr<ValueListTuple>&)> generate_filter = [&](unique_ptr<ValueListTuple>& t1, unique_ptr<ValueListTuple>& t2) -> bool{
    // 根据每一个字段，逐一进行比较
    for(size_t i=0;i<expressions_.size();i++){
      // 默认排序实现为升序排序(a<b时,记录res为-1，返回true)，ASC升序排序矫正因子为1，DESC降序排序矫正因子为-1
      unique_ptr<Expression>& expr = expressions_[i];
      int ctrl = order_[i] == 0 ? 1 : -1;

      Value t1_value, t2_value;
      rc = expr->get_value(*(t1.get()), t1_value);
      if(rc != RC::SUCCESS){
        LOG_WARN("cannot get value from expr no.%d", i);
      }
      rc = expr->get_value(*(t2.get()), t2_value);
      if(rc != RC::SUCCESS){
        LOG_WARN("cannot get value from expr no.%d", i);
      }

      int res = 0;
      if(t1_value.is_null() && !t2_value.is_null()){
        res = -1;
      } else if(t2_value.is_null() && !t1_value.is_null()){
        res = 1;
      } else{
        // 都是null或者都不是null
        if(t1_value.is_null() && t2_value.is_null()){
          res = 0;
        }else{
          res = t1_value.compare(t2_value);
          if(abs(res)>1){
            LOG_WARN("Exception, compare result is %d", res);
            res = 1;
          }
        }
      }

      res = res * ctrl;
      if(res == -1){
        return true;
      }else if(res == 1){
        return false;
      }else{
        continue;
      }
    }

    // 所有比较字段全部遍历完，默认返回true，即t1排在t2前面
    return true;
  };
  
  sort(all_tuple.begin(), all_tuple.end(), generate_filter);

  curr_tuple_ = all_tuple.begin();
  first_emited_ = false;
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::next()
{
  // order算子的next阶段只需要推进迭代器
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

RC OrderByPhysicalOperator::close()
{
  // close不需要做调整
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple()
{
  // curr_tuple根据迭代器进行返回
  if (curr_tuple_ != all_tuple.end()) {
    Tuple* res = (*curr_tuple_).get();
    return res;
  }
  return nullptr;
}