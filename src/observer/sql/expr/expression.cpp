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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/composite_tuple.h"
#include "sql/expr/arithmetic_operator.hpp"

using namespace std;

void init_destruction(SelectStmt*){}

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

bool FieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  // 暂时不确定是否需要针对NULL进行调整
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_);
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  RC rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{
}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, int &result) const
{
  RC  rc         = RC::SUCCESS;
  result         = -1;
  // 比较运算，针对NULL的运算结果均为false
  if(comp_ < IS_T &&(left.is_null()||right.is_null())){
    result = 0;
    return rc;
  }
  
  // 标量值的比较运算，有一种特殊情况也会落入这一层计算，即针对元素数为1的值列表。此时需要单独处理in和exist计算
  // 值列表元素数为1，in运算等价于equal，exist运算为真
  
  switch (comp_) {
    case EQUAL_TO: {
      int cmp_result = left.compare(right);
      result = (0 == cmp_result)? 1 : -1;
    } break;
    case LESS_EQUAL: {
      int cmp_result = left.compare(right);
      result = (cmp_result <= 0)? 1 : -1;
    } break;
    case NOT_EQUAL: {
      int cmp_result = left.compare(right);
      result = (cmp_result != 0)? 1 : -1;
    } break;
    case LESS_THAN: {
      int cmp_result = left.compare(right);
      result = (cmp_result < 0)? 1 : -1;
    } break;
    case GREAT_EQUAL: {
      int cmp_result = left.compare(right);
      result = (cmp_result >= 0)? 1 : -1;
    } break;
    case GREAT_THAN: {
      int cmp_result = left.compare(right);
      result = (cmp_result > 0)? 1 : -1;
    } break;
    case LIKE_OP: {
      int like_result = left.like(right);
      if (like_result < 0) {
        LOG_WARN("failed to compare value with like operator");
        rc = RC::INTERNAL;
      } else {
        result = (like_result != 0)? 1 : -1;
      }
    } break;
    case NOT_LIKE:{
      int like_result = left.like(right);
      if (like_result < 0) {
        LOG_WARN("failed to compare value with like operator");
        rc = RC::INTERNAL;
      } else {
        result = (like_result == 0)? 1 : -1;
      }
    } break;
    case IS_T:{
      // is 判断只适用于NULL
      if(left.is_null() && right.is_null()){
        result = 1;
      }else{
        result = -1;
      }
    } break;
    case IS_NOT:{
      // is 判断只适用于NULL
      if(left.is_null() && right.is_null()){
        result = -1;
      }else{
        result = 1;
      }
    } break;
    case IN_T:{
      // 因为是一对一比较，二者有一个为null，in判断都是不成立的
      if(left.is_null() || right.is_null()){
        result = 0;
      }else{
        int cmp_result = left.compare(right);
        result = (0 == cmp_result)? 1 : -1;
      }
    }break;
    case NOT_IN:{
      // 一对一比较，二者有一个为null， not in判断都是不成立的
      if(left.is_null() || right.is_null()){
        result = 0;
      }else {
        int cmp_result = left.compare(right);
        result = (0 != cmp_result)? 1 : -1;
      }
    } break;
    case EXIST_T:{
      // 已经确定值列表元素数为1，直接true就行
      result = 1;
    }break;
    case NOT_EXIST:{
      result = -1;
    }break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::compare_value_list(const vector<Value> &left, const vector<Value> &right, int &result) const
{
  RC  rc         = RC::SUCCESS;
  result         = -1;

  // 针对值列表的in运算和exist运算
  switch (comp_){
    // IN 和 EXIST 相关运算下，均是右值为值列表，左值为标量
    case IN_T:{
      // 需要将非NULL的左值依次与右值进行比较
      if(left[0].is_null()){
        result = 0;
        break;
      }

      bool right_have_null = false;
      for(size_t i=0;i<right.size();i++){
        if(right[i].is_null()){
          right_have_null = true;
          continue;
        }
        int cmp_result = left[0].compare(right[i]);
        result = (0 == cmp_result) ? 1 : -1;
        if(result == 1){
          return RC::SUCCESS;
        }
      }
      // for循环转完了，但仍然没退出，说明result一直是-1.
      if(right_have_null){
        result = 0;
      }
    }break;
    case NOT_IN:{
      // 需要将非NULL的左值依次与右值进行比较
      if(left[0].is_null()){
        result = 0;
        break;
      }
      bool right_have_null = false;
      for(size_t i=0;i<right.size();i++){
        if(right[i].is_null()){
          right_have_null = true;
          continue;
        }
        int cmp_result = left[0].compare(right[i]);
        if(cmp_result == 0){
          // 存在相等的元素，直接短路false
          result = -1;
          return RC::SUCCESS;
        }
      }
      // for循环转完了，但仍然没退出，left起码不在right中
      if(right_have_null){
        result = 0;
      }else{
        result = 1;
      }
    }break;
    case EXIST_T:{
      result = !right.empty();
    }break;
    case NOT_EXIST:{
      result = right.empty();
    }break;
    default: {
      // 针对值列表相关的非in exist比较，在值列表为空时返回false，在值列表多个元素时报错
      if(left.size()>1 || right.size()>1){
        LOG_WARN("unsupported comparison. %d", comp_);
        rc = RC::INTERNAL;
      }else{
        // 这里可能需要进一步扩展，考虑null的情况
        result = -1;
      }
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr *  left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr *  right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    int value = -1;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if(left_->type() < ExprType::VALUELIST && right_->type() < ExprType::VALUELIST){
    // 左值右值均没有valuelist，按照正常的比较逻辑进行比较即可
    Value left_value;
    Value right_value;
    rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
    
    int bool_value = -1;
  
    rc = compare_value(left_value, right_value, bool_value);
    if (rc == RC::SUCCESS) {
      value.set_boolean(bool_value);
    }
  }
  else{
    // 左值右值均有可能出现valuelist
    vector<Value> left_values;
    vector<Value> right_values;

    if(left_->type() >= ExprType::VALUELIST){
      rc = left_->get_valuelist(tuple, left_values);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get valuelist of left expression. rc=%s", strrc(rc));
        return rc;
      }
    }else{
      Value left_value;
      rc = left_->get_value(tuple, left_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
        return rc;
      }
      left_values.emplace_back(left_value);
    }

    if(right_->type() >= ExprType::VALUELIST){
      rc = right_->get_valuelist(tuple, right_values);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get valuelist of right expression. rc=%s", strrc(rc));
        return rc;
      }
    }else{
      Value right_value;
      rc = right_->get_value(tuple, right_value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
        return rc;
      }
      right_values.emplace_back(right_value);
    }
    
    int bool_value = -1;
    if(left_values.size() == 1 && right_values.size() == 1){
      // 标量值比较，退化到compare_value上
      rc = compare_value(left_values[0], right_values[0], bool_value);
    }else{
      // 值列表比较，使用compare_value_list
      rc = compare_value_list(left_values, right_values, bool_value);
    }
    if (rc == RC::SUCCESS) {
      value.set_boolean(bool_value);
    }
  }
  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  // tuple为当前要进行判断的record，判断的结果写入value
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(1);
    return rc;
  }

  if(children_.size() > 2){
    LOG_WARN("Conjunction cannot process more than two child");
    return RC::INTERNAL;
  }

  Value left_value;
  rc = children_[0]->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
    return rc;
  }

  if(conjunction_type_ == Type::AND && left_value.get_boolean() == -1){
    // left已确定为false，在and的场景下，整个表达式一定为false
    value.set_boolean(-1);
    return rc;
  }

  if(conjunction_type_ == Type::OR && left_value.get_boolean() == 1){
    // left已确定为true，在or的场景下，整个表达式一定为true
    value.set_boolean(1);
  }

  // 非短路场景下，需要结合right进行判断
  Value right_value;
  rc = children_[1]->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
    return rc;
  }

  if(conjunction_type_ == Type::AND){
    value.set_boolean(min(left_value.get_boolean(), right_value.get_boolean()));
  }else{
    value.set_boolean(max(left_value.get_boolean(), right_value.get_boolean()));
  }

  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  value.set_type(target_type);

  switch (arithmetic_type_) {
    case Type::ADD: {
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {
      Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {
      Value::negative(left_value, value);
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if(right_){
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, unique_ptr<Expression> child)
    : aggregate_name_(aggregate_name), child_(std::move(child))
{}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    case Type::COUNT:{
      aggregator = make_unique<CountAggregator>((this->child_->type() == ExprType::VALUE));
      break;
    }
    case Type::AVG:{
      aggregator = make_unique<AvgAggregator>();
      break;
    }
    case Type::MAX:{
      aggregator = make_unique<MaxAggregator>();
      break;
    }
    case Type::MIN:{
      aggregator = make_unique<MinAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ValueListExpr::ValueListExpr(vector<unique_ptr<Expression>> *expr_list){
  if(expr_list != nullptr){
    for(size_t i=0;i<expr_list->size();i++){
      vec_.emplace_back(std::move((*expr_list)[i].release()));
    }
  }
}

ValueListExpr::ValueListExpr(const vector<unique_ptr<Expression>> &expr_list) {
  for (const auto &e : expr_list) {
    vec_.emplace_back(e->copy());
  }
}

unique_ptr<Expression> ValueListExpr::copy() const {
  return make_unique<ValueListExpr>(vec_);
}

bool ValueListExpr::equal(const Expression &other) const{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }

  auto &other_valuelist_expr = static_cast<const ValueListExpr &>(other);
  if(this->vec_.size()!=other_valuelist_expr.vec_.size()){
    return false;
  }
  for(size_t i=0;i<this->vec_.size();i++){
    if(!vec_[i]->equal(*other_valuelist_expr.vec_[i])){
      return false;
    }
  }

  return true;
}

AttrType ValueListExpr::value_type() const{
  AttrType final_type = AttrType::UNDEFINED;
  if(!vec_.empty()){
    final_type = vec_[0]->value_type();
  }

  for(size_t i=0;i<vec_.size();i++){
    if(vec_[i]->value_type()!=final_type){
      // 两个数据类型不一样，考虑进行转换
      AttrType right_type = vec_[i]->value_type();
      auto left_to_right_cost = implicit_cast_cost(final_type, right_type);
      auto right_to_left_cost = implicit_cast_cost(right_type, final_type);
      if (left_to_right_cost <= right_to_left_cost && left_to_right_cost != INT32_MAX){
        // 左转右开销低于右转左, 最终类型取右
        final_type = right_type;
      }else if (right_to_left_cost < left_to_right_cost && right_to_left_cost != INT32_MAX){
        // 右转左开销低于左转右, 最终类型取左
      }else{
        ASSERT(1, "Data types that cannot be converted to each other appeared in the value list %s and %s",
            attr_type_to_string(final_type),
            attr_type_to_string(right_type));
      }
    }
  }

  return final_type;
}

int ValueListExpr::implicit_cast_cost(AttrType from, AttrType to) const
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

int ValueListExpr::value_length() const{
  int length = 0;
  for(size_t i=0;i<vec_.size();i++){
    length += vec_[i]->value_length();
  }
  return length;
}

RC ValueListExpr::get_value(const Tuple &tuple, Value &value) const {
  LOG_WARN("In theory, the value list cannot get_value separately");
  return RC::UNSUPPORTED;
}

RC ValueListExpr::try_get_value(Value &value){
  LOG_WARN("In theory, the value list cannot try_get_value separately");
  return RC::UNSUPPORTED;
}

RC ValueListExpr::get_valuelist(const Tuple &tuple, vector<Value> &values) const {
  RC rc = RC::SUCCESS;
  for(size_t i=0;i<vec_.size();i++){
    Value curr_value;
    rc = vec_[i]->get_value(tuple, curr_value);
    if(rc != RC::SUCCESS){
      LOG_WARN("failed to get value of valuelist idx is %d, rc is %s", i, strrc(rc));
      return rc;
    }
    values.emplace_back(curr_value);
  }

  return rc;
}

////////////////////////////////////////////////////////////////////////////////
unique_ptr<Expression> SelectExpr::copy() const{
  ASSERT(0, "SelectExpr cannot copy");
  return nullptr;
}

bool SelectExpr::equal(const Expression &other) const{
  ASSERT(0, "SelectExpr cannot compare");
  return false;
}

AttrType SelectExpr::value_type() const{
  // 在stmt层进行绑定时，会通过SelectSqlNode创建出对应的SelectStmt
  // 绑定之后，可以通过SelectStmt的query_expressions中的第一个字段来确认子查询的字段属性
  return this->value_type_;
}

int SelectExpr::value_length() const{
  // TODO: 子查询没有办法计算字段长度
  return 0;
}

RC SelectExpr::get_value(const Tuple &tuple, Value &value) const{
  // TODO: 子查询获取值可能不需要通过表达式完成，先占位
  return RC::SUCCESS;
}

RC SelectExpr::get_valuelist(const Tuple &tuple, vector<Value> &values) const {
  // 子查询的值以value集合返回

  // 先根据pos，从tuple中解析出对应自身子查询部分的composite tuple, 实质已经被封装成了valuelistetuple
  if(tuple.type() == TupleType::COMPOSITE){
    const CompositeTuple& origin_tuple = static_cast<const CompositeTuple&>(tuple);
    Tuple* sub_tuple = origin_tuple.get_part(this->pos_);
  
    // sub_tuple_内部理论上应该为若干valuelisttuple，对应子查询的每一个结果，直接解析结果即可
    int cell_num = sub_tuple->cell_num();
    values.clear();
    values.reserve(cell_num);
    for(int i=0;i<cell_num;i++){
      Value curr_cell;
      sub_tuple->cell_at(i, curr_cell);
      values.emplace_back(curr_cell);
    }
    return RC::SUCCESS;
  }else{
    LOG_WARN("tuple is not composite");
    return RC::INTERNAL;
  }
}

