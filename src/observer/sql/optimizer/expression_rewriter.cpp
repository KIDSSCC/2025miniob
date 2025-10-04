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
// Created by Wangyunlai on 2022/12/13.
//

#include "sql/optimizer/expression_rewriter.h"
#include "common/log/log.h"
#include "sql/optimizer/comparison_simplification_rule.h"
#include "sql/optimizer/conjunction_simplification_rule.h"

using namespace std;

ExpressionRewriter::ExpressionRewriter()
{
  // 表达式重写默认包含两条规则，比较表达式简化（常量推断？） + conjunction简化（单conjuntion退化为comparison）
  expr_rewrite_rules_.emplace_back(new ComparisonSimplificationRule);
  expr_rewrite_rules_.emplace_back(new ConjunctionSimplificationRule);
}

RC ExpressionRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;

  bool sub_change_made = false;

  vector<unique_ptr<Expression>> &expressions = oper->expressions();
  for (unique_ptr<Expression> &expr : expressions) {
    rc = rewrite_expression(expr, sub_change_made);
    if (rc != RC::SUCCESS) {
      break;
    }

    if (sub_change_made && !change_made) {
      change_made = true;
    }
  }

  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 完成了对本逻辑节点的改写后，尝试改写逻辑节点树中的其他节点。
  vector<unique_ptr<LogicalOperator>> &child_opers = oper->children();
  for (unique_ptr<LogicalOperator> &child_oper : child_opers) {
    bool sub_change_made = false;
    rc                   = rewrite(child_oper, sub_change_made);
    if (sub_change_made && !change_made) {
      change_made = true;
    }
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC ExpressionRewriter::rewrite_expression(unique_ptr<Expression> &expr, bool &change_made)
{
  // 尝试重写表达式
  RC rc = RC::SUCCESS;

  change_made = false;
  for (unique_ptr<ExpressionRewriteRule> &rule : expr_rewrite_rules_) {
    // 逐一应用每一条重写规则
    bool sub_change_made = false;

    rc                   = rule->rewrite(expr, sub_change_made);
    if (sub_change_made && !change_made) {
      change_made = true;
    }
    if (rc != RC::SUCCESS) {
      break;
    }
  }

  // change_made为true时，说明已经对本表达式进行了改写，此时可以直接返回，否则，即通过后续的switch部分遍历本表达式的子表达式
  if (change_made || rc != RC::SUCCESS) {
    return rc;
  }

  switch (expr->type()) {
    case ExprType::FIELD:
    case ExprType::VALUE: {
      // do nothing
    } break;

    case ExprType::CAST: {
      // CAST表达式尝试重写底层的原表达式
      unique_ptr<Expression> &child_expr = (static_cast<CastExpr *>(expr.get()))->child();

      rc                                      = rewrite_expression(child_expr, change_made);
    } break;

    case ExprType::COMPARISON: {
      // 比较表达式尝试重写左右表达式
      auto                         comparison_expr = static_cast<ComparisonExpr *>(expr.get());

      unique_ptr<Expression> &left_expr       = comparison_expr->left();
      unique_ptr<Expression> &right_expr      = comparison_expr->right();

      bool left_change_made = false;

      rc                    = rewrite_expression(left_expr, left_change_made);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      bool right_change_made = false;

      rc                     = rewrite_expression(right_expr, right_change_made);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      if (left_change_made || right_change_made) {
        change_made = true;
      }
    } break;

    case ExprType::CONJUNCTION: {
      // conjunction表达式尝试重写每一个子表达式
      auto                                      conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());

      vector<unique_ptr<Expression>> &children         = conjunction_expr->children();
      for (unique_ptr<Expression> &child_expr : children) {
        bool sub_change_made = false;

        rc                   = rewrite_expression(child_expr, sub_change_made);
        if (rc != RC::SUCCESS) {

          LOG_WARN("failed to rewriter conjunction sub expression. rc=%s", strrc(rc));
          return rc;
        }

        if (sub_change_made && !change_made) {
          change_made = true;
        }
      }
    } break;

    default: {
      // do nothing
    } break;
  }
  return rc;
}
