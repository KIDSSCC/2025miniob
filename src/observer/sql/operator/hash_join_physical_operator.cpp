/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/hash_join_physical_operator.h"

HashJoinPhysicalOperator::HashJoinPhysicalOperator(unique_ptr<Expression>& left_expr, unique_ptr<Expression>& right_expr){
    // 构造函数
    left_expr_ = std::move(left_expr);
    right_expr_ = std::move(right_expr);
}

RC HashJoinPhysicalOperator::open(Trx *trx){
    RC rc = RC::SUCCESS;
    if (children_.size() != 2) {
        LOG_WARN("hash join operator should have 2 children");
        return RC::INTERNAL;
    }

    left_         = children_[0].get();
    right_        = children_[1].get();

    trx_ = trx;
    rc   = left_->open(trx);
    if(rc != RC::SUCCESS){
        LOG_WARN("Failed to open left child, rc is %s", strrc(rc));
        return rc;
    }

    rc   = right_->open(trx);
    if(rc != RC::SUCCESS){
        LOG_WARN("Failed to open right child, rc is %s", strrc(rc));
        return rc;
    }
    // 遍历右子节点的所有tuple，构建哈希表
    while(rc = right_->next(), rc == RC::SUCCESS) {
        Tuple *right_tuple = right_->current_tuple();
        if (right_tuple == nullptr) {
            LOG_WARN("failed to get current tuple from right oper");
            return RC::INTERNAL;
        }

        Value key;
        rc = right_expr_->get_value(*right_tuple, key);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to get value from right_expr. rc=%s", strrc(rc));
            return rc;
        }

        ValueListTuple value_list;
        rc = ValueListTuple::make(*right_tuple, value_list);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to make valuelist tuple. rc=%s", strrc(rc));
            return rc;
        }
        right_hash.insert({key, make_unique<ValueListTuple>(std::move(value_list))});
    }

    if(rc != RC::RECORD_EOF && rc != RC::SUCCESS) {
        LOG_WARN("failed to get next tuple from right oper. rc=%s", strrc(rc));
        return rc;
    }

    rc = right_->close();
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
        return rc;
    }

    right_is_null = true;

    return RC::SUCCESS;
}

RC HashJoinPhysicalOperator::next(){
    RC   rc             = RC::SUCCESS;
    // 先考虑左值的情况。
    // 1.刚开始的时候，left_tuple_指针为nullptr，左值需要步进。
    // 2.右值迭代器走到了上一组候选的end，左值需要步进。同时更新右值
    // 左值步进后，如果没有对应的候选右值，左值需要再次步进

    bool left_need_step = (left_tuple_ == nullptr) || (right_is_null == true) || (right_iter == candidate_right.second);
    while(left_need_step){
        rc = left_next();
        if(rc == RC::RECORD_EOF){
            // 左值遍历结束了，整个hashjoin也就结束了
            return rc;
        }
        if(rc != RC::SUCCESS){
            LOG_WARN("Failed to execute left_next, rc is %s", strrc(rc));
            return rc;
        }

        // 拿到了一个有效的左值，检查其是否有候选右值
        Value left_key;
        rc = left_expr_->get_value(*left_tuple_, left_key);
        if(rc != RC::SUCCESS){
            LOG_WARN("Failed to get value from left_expr. rc is %s", strrc(rc));
            return rc;
        }

        left_need_step = (right_hash.count(left_key) == 0);

        if(!left_need_step){
            // 对当前遍历得到的左值，有若干候选右值
            candidate_right = right_hash.equal_range(left_key);
            right_iter = candidate_right.first;
            right_is_null = false;
        }
    }

    // 最核心的部分，设置左值和对应的右值，右值迭代器向前步进。
    joined_tuple_.set_left(left_tuple_);
    joined_tuple_.set_right((*right_iter).second.get());
    right_iter++;

    return RC::SUCCESS;
}

RC HashJoinPhysicalOperator::close(){
    left_->close();
    right_is_null = true;
    right_hash.clear();
    return RC::SUCCESS;
}

Tuple* HashJoinPhysicalOperator::current_tuple(){
    return &joined_tuple_;
}

RC HashJoinPhysicalOperator::left_next()
{
  RC rc = RC::SUCCESS;
  rc    = left_->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  left_tuple_ = left_->current_tuple();
  return rc;
}