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
// Created by Wangyunlai on 2022/12/30.
//

#include "sql/optimizer/hash_join_rewriter.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"

RC HashJoinRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  // LOG_INFO("logical oper type is %d", oper->type());
  RC rc = RC::SUCCESS;

  bool sign = false;
  // HashJoin改写所针对的结构：当前算子无所谓，其子算子为predicate, 子算子的子算子为join
  if(!oper->children().empty() && oper->children().back()->type() == LogicalOperatorType::PREDICATE){
    unique_ptr<LogicalOperator>& child = oper->children().back();
    if(!child->children().empty() && child->children().back()->type() == LogicalOperatorType::JOIN){
      sign = true;
    }
  }

  if(!sign){
    return rc;
  }

  // 满足条件的算子结构继续执行下面的改写
  PredicateLogicalOperator* pred_oper = static_cast<PredicateLogicalOperator*>(oper->children().back().get());
  JoinLogicalOperator* join_oper = static_cast<JoinLogicalOperator*>(pred_oper->children().back().get());

  // pred_oper需只有一个compare节点，且为两个field字段的等值比较
  // join_oper需为两张表的连接
  // pred_oper中的两个比较字段需分别对应join_oper的两张表

  sign = (pred_oper->expressions().size() == 1) && (pred_oper->expressions()[0]->type() == ExprType::CONJUNCTION || pred_oper->expressions()[0]->type() == ExprType::COMPARISON);
  if(!sign){
    return rc;
  }

  ComparisonExpr* comp_expr = nullptr;
  if(pred_oper->expressions()[0]->type() == ExprType::CONJUNCTION){
    ConjunctionExpr* conj_expr = static_cast<ConjunctionExpr*>(pred_oper->expressions()[0].get());
    sign = (conj_expr->children().size() == 1) && (conj_expr->children()[0]->type() == ExprType::COMPARISON);
    if(!sign){
      return rc;
    }
    comp_expr = static_cast<ComparisonExpr*>(conj_expr->children()[0].get());
  }else{
    comp_expr = static_cast<ComparisonExpr*>(pred_oper->expressions()[0].get());
  }

  sign = (comp_expr->comp() == CompOp::EQUAL_TO) && (comp_expr->left()->type() == ExprType::FIELD) && (comp_expr->right()->type() == ExprType::FIELD);
  if(!sign){
    return rc;
  }

  string left_field_from = static_cast<FieldExpr*>(comp_expr->left().get())->table_name();
  string right_field_from = static_cast<FieldExpr*>(comp_expr->right().get())->table_name();
  
  sign = (join_oper->children().size() == 2) && (join_oper->children()[0]->type() == LogicalOperatorType::TABLE_GET) && (join_oper->children()[1]->type() == LogicalOperatorType::TABLE_GET);
  if(!sign){
    return rc;
  }

  string left_table_name = static_cast<TableGetLogicalOperator*>(join_oper->children()[0].get())->table()->name();
  string right_table_name = static_cast<TableGetLogicalOperator*>(join_oper->children()[1].get())->table()->name();

  sign = ((left_field_from == left_table_name) && (right_field_from == right_table_name)) || ((left_field_from == right_table_name) && (right_field_from == left_table_name));
  if(!sign){
    return rc;
  }

  // 至此，满足了所有改写的条件
  unique_ptr<Expression> left_field_expr = static_cast<FieldExpr*>(comp_expr->left().get())->copy();
  unique_ptr<Expression> right_field_expr = static_cast<FieldExpr*>(comp_expr->right().get())->copy();
  join_oper->can_use_hash_join_ = true;
  if(left_field_from == left_table_name){
    // 左对左，右对右
    join_oper->left_expr_ = static_cast<FieldExpr*>(comp_expr->left().get())->copy();
    join_oper->right_expr_ = static_cast<FieldExpr*>(comp_expr->right().get())->copy();
  }else{
    // 左对右，右对左
    join_oper->left_expr_ = static_cast<FieldExpr*>(comp_expr->right().get())->copy();
    join_oper->right_expr_ = static_cast<FieldExpr*>(comp_expr->left().get())->copy();
  }

  unique_ptr<Expression>& conj = pred_oper->expressions()[0];
  Value value((bool)true);
  conj = unique_ptr<Expression>(new ValueExpr(value));
  LOG_INFO("predicate push down to hash join");

  change_made = true;
  
  return rc;
}