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
// Created by WangYunlai on 2022/12/30.
//

#include "sql/operator/nested_loop_join_physical_operator.h"
#include <chrono>

NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator() {}

RC NestedLoopJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("nlj operator should have 2 children");
    return RC::INTERNAL;
  }

  RC rc         = RC::SUCCESS;

  // join算子在构建的时候是左规约，尝试将下层的join算子转到right的位置上，使其被调用后能够立即被回收
  // left_         = children_[0].get();
  // right_        = children_[1].get();
  left_         = children_[1].get();
  right_        = children_[0].get();

  right_closed_ = true;
  round_done_   = true;

  rc   = left_->open(trx);
  trx_ = trx;

  // 对右表，直接打开并获取所有元素，缓存在join算子中
  rc = right_->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open right oper. rc=%s", strrc(rc));
    return rc;
  }

  while(rc = right_->next(), rc == RC::SUCCESS) {
    Tuple *right_tuple = right_->current_tuple();
    if (right_tuple == nullptr) {
      LOG_WARN("failed to get current tuple from right oper");
      return RC::INTERNAL;
    }
    ValueListTuple value_list;
    rc = ValueListTuple::make(*right_tuple, value_list);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make valuelist tuple. rc=%s", strrc(rc));
      return rc;
    }
    right_cache.push_back(make_unique<ValueListTuple>(std::move(value_list)));
  }

  if(rc != RC::RECORD_EOF && rc != RC::SUCCESS) {
    LOG_WARN("failed to get next tuple from right oper. rc=%s", strrc(rc));
    return rc;
  }

  right_cache_iter = right_cache.end();

  rc = right_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;

  // return rc;
}

// RC NestedLoopJoinPhysicalOperator::next()
// {
//   bool left_need_step = (left_tuple_ == nullptr);
//   RC   rc             = RC::SUCCESS;
//   if (round_done_) {
//     // 如果右表已经结束了一轮遍历，则左表需要向前，获取下一条记录
//     left_need_step = true;
//   } else {
//     // 如果右表未完成一轮遍历，当前仅需要从右表中获取下一条记录
//     rc = right_next();
//     if (rc != RC::SUCCESS) {
//       if (rc == RC::RECORD_EOF) {
//         left_need_step = true;
//       } else {
//         return rc;
//       }
//     } else {
//       return rc;  // got one tuple from right
//     }
//   }

//   if (left_need_step) {
//     rc = left_next();
//     if (rc != RC::SUCCESS) {
//       return rc;
//     }
//   }

//   rc = right_next();
//   return rc;
// }

RC NestedLoopJoinPhysicalOperator::next()
{
  bool left_need_step = (left_tuple_ == nullptr);
  RC   rc             = RC::SUCCESS;

  if(right_cache_iter == right_cache.end()) {
    // 右表已经结束了一轮遍历，则左表需要向前，获取下一条记录
    left_need_step = true;
    right_cache_iter = right_cache.begin();
  }else{
    // 如果右表未完成一轮遍历，当前仅需要从右表中获取下一条记录
    joined_tuple_.set_left((*right_cache_iter).get());
    right_cache_iter++;
    return RC::SUCCESS;  // got one tuple from right
  }

  if(left_need_step){
    rc = left_next();
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  if(right_cache_iter != right_cache.end()) {
    joined_tuple_.set_left((*right_cache_iter).get());
    right_cache_iter++;
  }else{
    return RC::RECORD_EOF;
  }
  return RC::SUCCESS;
}


RC NestedLoopJoinPhysicalOperator::close()
{
  RC rc = left_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));
  }

  // 清理已有资源的同时尝试回收内存
  right_cache.clear();
  right_cache.shrink_to_fit();

  // if (!right_closed_) {
  //   rc = right_->close();
  //   if (rc != RC::SUCCESS) {
  //     LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
  //   } else {
  //     right_closed_ = true;
  //   }
  // }
  return rc;
}

Tuple *NestedLoopJoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

RC NestedLoopJoinPhysicalOperator::left_next()
{
  RC rc = RC::SUCCESS;
  rc    = left_->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  left_tuple_ = left_->current_tuple();
  // joined_tuple_.set_left(left_tuple_);
  joined_tuple_.set_right(left_tuple_);
  return rc;
}

RC NestedLoopJoinPhysicalOperator::right_next()
{
  RC rc = RC::SUCCESS;
  if (round_done_) {
    if (!right_closed_) {
      rc = right_->close();

      right_closed_ = true;
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }

    rc = right_->open(trx_);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    right_closed_ = false;

    round_done_ = false;
  }

  rc = right_->next();
  if (rc != RC::SUCCESS) {
    if (rc == RC::RECORD_EOF) {
      round_done_ = true;
    }
    return rc;
  }

  right_tuple_ = right_->current_tuple();
  joined_tuple_.set_right(right_tuple_);
  return rc;
}
