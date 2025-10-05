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
// Created by Wangyunlai on 2022/12/26.
//

#include "sql/optimizer/conjunction_simplification_rule.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"

RC try_to_get_bool_constant(unique_ptr<Expression> &expr, bool &constant_value)
{
  if (expr->type() == ExprType::VALUE && expr->value_type() == AttrType::BOOLEANS) {
    auto value_expr = static_cast<ValueExpr *>(expr.get());
    constant_value  = (value_expr->get_value().get_boolean() == 1);
    return RC::SUCCESS;
  }
  return RC::INTERNAL;
}
RC ConjunctionSimplificationRule::rewrite(unique_ptr<Expression> &expr, bool &change_made)
{
  RC rc = RC::SUCCESS;
  if (expr->type() != ExprType::CONJUNCTION) {
    return rc;
  }

  change_made                                                = false;
  auto                                      conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());
  vector<unique_ptr<Expression>> &child_exprs      = conjunction_expr->children();

  // 先看看有没有能够直接去掉的表达式。比如AND时恒为true的表达式可以删除
  // 或者是否可以直接计算出当前表达式的值。比如AND时，如果有一个表达式为false，那么整个表达式就是false
  for (auto iter = child_exprs.begin(); iter != child_exprs.end();) {
    bool constant_value = false;

    rc                  = try_to_get_bool_constant(*iter, constant_value);
    if (rc != RC::SUCCESS) {
      rc = RC::SUCCESS;
      ++iter;
      continue;
    }

    // 此处相当意义不明，A AND B 下，A不可知，B为True，此时应忽略B。B为False，那整个Conjunction就都一边凉快去吧
    // 同理，A OR B 下，A不可知，B为True，此时整个Conjunction均为True B为False，此时应忽略B
    if (conjunction_expr->conjunction_type() == ConjunctionExpr::Type::AND) {
      if (constant_value == true) {
        child_exprs.erase(iter);
      } else {
        // always be false
        child_exprs.clear();
        conjunction_expr->set_default(-1);
        return rc;
        // unique_ptr<Expression> child_expr = std::move(child_exprs.front());
        // child_exprs.clear();
        // expr = std::move(child_expr);
      }
    } else {
      // conjunction_type == OR
      if (constant_value == true) {
        // always be true
        child_exprs.clear();
        conjunction_expr->set_default(1);
        return rc;
        // unique_ptr<Expression> child_expr = std::move(child_exprs.front());
        // child_exprs.clear();
        // expr = std::move(child_expr);
      } else {
        child_exprs.erase(iter);
      }
    }
  }

  // 这里子节点数量为1有两种可能，一种是本身有两个，被常量优化掉一个，一种是本来就一个
  if (child_exprs.size() == 1) {
    LOG_TRACE("conjunction expression has only 1 child");
    unique_ptr<Expression> child_expr = std::move(child_exprs.front());
    child_exprs.clear();
    expr = std::move(child_expr);

    change_made = true;
  }

  return rc;
}
