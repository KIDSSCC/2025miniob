/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#include "sql/optimizer/logical_plan_generator.h"

#include "common/log/log.h"

#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/orderby_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/group_by_logical_operator.h"

#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"

#include "sql/expr/expression_iterator.h"

using namespace std;
using namespace common;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);

      rc = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);

      rc = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);

      rc = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::UPDATE:{
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);

      rc = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);

      rc = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);

      rc = create_plan(explain_stmt, logical_operator);
    } break;
    default: {
      rc = RC::UNIMPLEMENTED;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> *last_oper = nullptr;

  unique_ptr<LogicalOperator> table_oper(nullptr);
  last_oper = &table_oper;
  unique_ptr<LogicalOperator> predicate_oper;

  RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  // 将原有的相关表全连接，替换为条件join
  function<unique_ptr<LogicalOperator>(const unique_ptr<RelationNode>&)> generate_join_oper = [&](const unique_ptr<RelationNode>& relation_node) -> unique_ptr<LogicalOperator> {
    if(!relation_node->is_join){
      unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(relation_node->table_ptr, ReadWriteMode::READ_ONLY));
      return table_get_oper;
    }else{
      unique_ptr<LogicalOperator> ret_node;

      unique_ptr<LogicalOperator> left_node = generate_join_oper(relation_node->left);
      unique_ptr<LogicalOperator> right_node = generate_join_oper(relation_node->right);
      if(left_node == nullptr || right_node == nullptr){
        LOG_WARN("failed to create join node");
        return nullptr;
      }

      unique_ptr<JoinLogicalOperator> join_oper = make_unique<JoinLogicalOperator>();
      join_oper->add_child(std::move(left_node));
      join_oper->add_child(std::move(right_node));

      // 两张表在inner join连接下，创建单独的predicate算子
      if(relation_node->join_type == JoinOp::INNER_JOIN){
        unique_ptr<LogicalOperator> join_predicate_oper;
        rc = create_plan(relation_node->filter_stmt.get(), join_predicate_oper);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
          return nullptr;
        }
        
        if(join_predicate_oper){
          join_predicate_oper->add_child(std::move(join_oper));
          ret_node = std::move(join_predicate_oper);
        }else{
          ret_node = std::move(join_oper);
        }
      }else{
        // cross join下，直接取join节点作为当前节点
        ret_node = std::move(join_oper);
      }

      // // 这里略显抽象，RelationNode中的filter_stmt对象，由于声明的过早，不能在RelationNode中进行析构，只能放在这里释放掉
      // // 但在逻辑计划生成过程中出现错误的时候，这个分支时执行不到，也就会导致对象泄露。但既然逻辑计划生成都已经错误了，内存对象泄露其实也就不是重点了
      // // 这里最正确的解决方案应该是在stmt层构建一个新的中间件，承接起RelationNode，并添加新的filter_stmt。这样可以由中间件在stmt层释放资源。但RelationNode向中间件的转换又要涉及递归转换。暂时不想写，先这样吧...
      // if(relation_node->filter_stmt!=nullptr){
      //   delete relation_node->filter_stmt;
      //   relation_node->filter_stmt = nullptr;
      // }

      return ret_node;
    }
  };

  unique_ptr<RelationNode>& relations = select_stmt->relations();
  table_oper = generate_join_oper(relations);


  if (predicate_oper) {
    if (*last_oper) {
      predicate_oper->add_child(std::move(*last_oper));
    }

    last_oper = &predicate_oper;
  }

  unique_ptr<LogicalOperator> group_by_oper;
  rc = create_group_by_plan(select_stmt, group_by_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create group by logical plan. rc=%s", strrc(rc));
    return rc;
  }

  if (group_by_oper) {
    if (*last_oper) {
      group_by_oper->add_child(std::move(*last_oper));
    }

    last_oper = &group_by_oper;
  }

  // 这里还需要叠加一层having的处理
  unique_ptr<LogicalOperator> having_predicate_oper;
  rc = create_plan(select_stmt->having_stmt(), having_predicate_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create having predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }
  if (having_predicate_oper) {
    if (*last_oper) {
      having_predicate_oper->add_child(std::move(*last_oper));
    }

    last_oper = &having_predicate_oper;
  }

  // 这里需要叠加一层order by的处理，不需要复杂的计划生成逻辑，将stmt中的orderby数组传过去即可。
  vector<pair<int, unique_ptr<Expression>>>& order_by = select_stmt->order_by();
  unique_ptr<LogicalOperator> orderby_oper;
  if(!order_by.empty()){
    unique_ptr<OrderByLogicalOperator> oper = make_unique<OrderByLogicalOperator>(order_by);
    orderby_oper = std::move(oper);
    if(*last_oper){
      orderby_oper->add_child(std::move(*last_oper));
    }
    last_oper = &orderby_oper;
  }

  // table_get -> predicate -> group_by -> having -> project
  auto project_oper = make_unique<ProjectLogicalOperator>(std::move(select_stmt->query_expressions()));
  if (*last_oper) {
    project_oper->add_child(std::move(*last_oper));
  }

  logical_operator = std::move(project_oper);
  return RC::SUCCESS;
}

// 针对条件过滤谓词生成逻辑计划
RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC                                  rc = RC::SUCCESS;
  vector<unique_ptr<Expression>> cmp_exprs;
  const vector<FilterUnit *>    &filter_units = filter_stmt->filter_units();

  // 将FilterUnit逐一转换为ComparisonExpr
  for (const FilterUnit *filter_unit : filter_units) {
    const FilterObj &filter_obj_left  = filter_unit->left();
    const FilterObj &filter_obj_right = filter_unit->right();

    unique_ptr<Expression> left;
    if(filter_obj_left.is_attr == 1){
      left = std::make_unique<FieldExpr>(filter_obj_left.field);
    }else if(filter_obj_left.is_attr == 0){
      left = std::make_unique<ValueExpr>(filter_obj_left.value);
    }else{
      left = move(const_cast<FilterObj &>(filter_obj_left).expr);
    }

    unique_ptr<Expression> right;
    if(filter_obj_right.is_attr == 1){
      right = std::make_unique<FieldExpr>(filter_obj_right.field);
    }else if(filter_obj_right.is_attr == 0){
      right = std::make_unique<ValueExpr>(filter_obj_right.value);
    }else{
      right = move(const_cast<FilterObj &>(filter_obj_right).expr);
    }

    // 条件过滤语句，左右Value类型不一致时，按照转换开销进行转换并比较
    if (left->value_type() != right->value_type()) {

      // 考虑整型对浮点这一种特殊的情况
      if((left->value_type() == AttrType::INTS && right->value_type() == AttrType::CHARS) || (left->value_type() == AttrType::CHARS && right->value_type() == AttrType::INTS)){
        AttrType target_type = AttrType::FLOATS;
        // 左值转float
        ExprType left_type = left->type();
        auto cast_expr = make_unique<CastExpr>(std::move(left), target_type);
        if (left_type == ExprType::VALUE) {
          Value left_val;
          if (OB_FAIL(rc = cast_expr->try_get_value(left_val)))
          {
            LOG_WARN("failed to get value from left child", strrc(rc));
            return rc;
          }
          left = make_unique<ValueExpr>(left_val);
        } else {
          left = std::move(cast_expr);
        }
        // 右值转float
        ExprType right_type = right->type();
        cast_expr = make_unique<CastExpr>(std::move(right), target_type);
        if (right_type == ExprType::VALUE) {
          Value right_val;
          if (OB_FAIL(rc = cast_expr->try_get_value(right_val)))
          {
            LOG_WARN("failed to get value from right child", strrc(rc));
            return rc;
          }
          right = make_unique<ValueExpr>(right_val);
        } else {
          right = std::move(cast_expr);
        }
      }else{
        // 普通的数据转换流程
        auto left_to_right_cost = implicit_cast_cost(left->value_type(), right->value_type());
        auto right_to_left_cost = implicit_cast_cost(right->value_type(), left->value_type());
  
        if (left_to_right_cost <= right_to_left_cost && left_to_right_cost != INT32_MAX) {
          // 左转右开销低于右转左
          ExprType left_type = left->type();
          auto cast_expr = make_unique<CastExpr>(std::move(left), right->value_type());
  
          // 左值可能是一个字段，也可能是一个值，无论是哪一种，都首先生成了一个CastExpr。
          // 如果左值是一个常量值，则直接通过CastExpr获取其值，形成一个ValueExpr，如果左值是一个字段，则保留为CastExpr
          // 如果左边是ValueExpr，直接转换为ValueExpr，否则转换为CastExpr
          if (left_type == ExprType::VALUE) {
            Value left_val;
            if (OB_FAIL(rc = cast_expr->try_get_value(left_val)))
            {
              LOG_WARN("failed to get value from left child", strrc(rc));
              return rc;
            }
            left = make_unique<ValueExpr>(left_val);
          } else {
            left = std::move(cast_expr);
          }
        } else if (right_to_left_cost < left_to_right_cost && right_to_left_cost != INT32_MAX) {
          // 右转左开销低于左转右
          ExprType right_type = right->type();
          auto cast_expr = make_unique<CastExpr>(std::move(right), left->value_type());
  
          // 如果右边是ValueExpr，直接转换为ValueExpr，否则转换为CastExpr
          if (right_type == ExprType::VALUE) {
            Value right_val;
            if (OB_FAIL(rc = cast_expr->try_get_value(right_val)))
            {
              LOG_WARN("failed to get value from right child", strrc(rc));
              return rc;
            }
            right = make_unique<ValueExpr>(right_val);
          } else {
            right = std::move(cast_expr);
          }
  
        } else {
          rc = RC::UNSUPPORTED;
          LOG_WARN("unsupported cast from %s to %s", attr_type_to_string(left->value_type()), attr_type_to_string(right->value_type()));
          return rc;
        }
      }
    }
    ComparisonExpr *cmp_expr = new ComparisonExpr(filter_unit->comp(), std::move(left), std::move(right));
    cmp_exprs.emplace_back(cmp_expr);
  }

  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (!cmp_exprs.empty()) {
    // conjunction_expr，将若干ComparisonExpr以何种逻辑进行连接，默认是and逻辑
    unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::move(conjunction_expr)));
  }
  logical_operator = std::move(predicate_oper);
  return rc;
}

int LogicalPlanGenerator::implicit_cast_cost(AttrType from, AttrType to)
{
  if (from == to) {
    return 0;
  }

  if (to == AttrType::UNDEFINED){
    return INT32_MAX;
  }

  if(from == AttrType::UNDEFINED){
    return INT32_MIN;
  }
  return DataType::type_instance(from)->cast_cost(to);
}

RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table        *table = insert_stmt->table();
  vector<Value> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());

  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, values);
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;

  Table* table = update_stmt->table();
  FilterStmt* filter_stmt = update_stmt->filter_stmt();
  int field_index = update_stmt->value_amount();
  const Value* value = update_stmt->values();

  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));
  unique_ptr<LogicalOperator> predicate_oper;

  rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to create plan for filter stmt");
    return rc;
  }

  unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table, field_index, value));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    update_oper->add_child(std::move(predicate_oper));
  } else {
    update_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(update_oper);

  return rc;
}

RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table                      *table       = delete_stmt->table();
  FilterStmt                 *filter_stmt = delete_stmt->filter_stmt();
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));

  unique_ptr<LogicalOperator> predicate_oper;

  // 在包含where字段时，predicate_oper最终获得一个PredicateLogicalOperator
  RC rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    delete_oper->add_child(std::move(predicate_oper));
  } else {
    delete_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(delete_oper);
  return rc;
}

RC LogicalPlanGenerator::create_plan(ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> child_oper;

  Stmt *child_stmt = explain_stmt->child();

  RC rc = create(child_stmt, child_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}

RC LogicalPlanGenerator::create_group_by_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  vector<unique_ptr<Expression>> &group_by_expressions = select_stmt->group_by();
  vector<Expression *> aggregate_expressions;
  vector<unique_ptr<Expression>> &query_expressions = select_stmt->query_expressions();

  // 绑定聚合部分，主要是收集所有的聚合函数并确定其在结果列中的位置
  function<RC(unique_ptr<Expression>&)> collector = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      expr->set_pos(aggregate_expressions.size() + group_by_expressions.size());
      aggregate_expressions.push_back(expr.get());
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, collector);
    return rc;
  };

  // 绑定分组部分，主要是确定查询字段在分组字段中的位置，这里假设分组字段都是单一字段，无表达式
  function<RC(unique_ptr<Expression>&)> bind_group_by_expr = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    for (size_t i = 0; i < group_by_expressions.size(); i++) {
      auto &group_by = group_by_expressions[i];
      if (expr->type() == ExprType::AGGREGATION) {
        break;
      } else if (expr->equal(*group_by)) {
        expr->set_pos(i);
        continue;
      } else {
        rc = ExpressionIterator::iterate_child_expr(*expr, bind_group_by_expr);
      }
    }
    return rc;
  };

  // 检查所有的非聚合查询字段是否均在分组字段中出现了
 bool found_unbound_column = false;
  function<RC(unique_ptr<Expression>&)> find_unbound_column = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      // do nothing
    } else if (expr->pos() != -1) {
      // do nothing
    } else if (expr->type() == ExprType::FIELD) {
      // pos为-1说明该字段未在分组字段中出现，标记found_unbound_column
      found_unbound_column = true;
    }else {
      rc = ExpressionIterator::iterate_child_expr(*expr, find_unbound_column);
    }
    return rc;
  };
  
  for (unique_ptr<Expression> &expression : query_expressions) {
    bind_group_by_expr(expression);
  }
  
  for (unique_ptr<Expression> &expression : query_expressions) {
    find_unbound_column(expression);
  }

  // 收集所有查询字段中的聚合函数
  for (unique_ptr<Expression> &expression : query_expressions) {
    collector(expression);
  }

  // 收集所有在having字段中的聚合函数
  for(unique_ptr<Expression> &expression : select_stmt->having_expressions()){
    collector(expression);
  }

  if (group_by_expressions.empty() && aggregate_expressions.empty()) {
    // 既没有group by也没有聚合函数，不需要group by
    return RC::SUCCESS;
  }

  if (found_unbound_column) {
    LOG_WARN("column must appear in the GROUP BY clause or must be part of an aggregate function");
    return RC::INVALID_ARGUMENT;
  }

  // 如果只需要聚合，但是没有group by 语句，需要生成一个空的group by 语句

  auto group_by_oper = make_unique<GroupByLogicalOperator>(std::move(group_by_expressions),
                                                           std::move(aggregate_expressions));
  logical_operator = std::move(group_by_oper);
  return RC::SUCCESS;
}
