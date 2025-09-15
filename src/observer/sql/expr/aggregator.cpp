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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  // 第一个值无论是正常值还是null值，都直接接收
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  if(value.is_null()){
    // 新传入的值为null，忽略
    return RC::SUCCESS;
  }else if(value_.is_null()){
    // 当前值为null，则用传入值覆盖
    value_ = value;
    return RC::SUCCESS;
  }else{
    // 两个值都不为null，正常累加
    Value::add(value, value_, value_);
    return RC::SUCCESS;
  }

  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value){
  
  if(value_.attr_type() == AttrType::UNDEFINED){
    if(accept_null_ || !value.is_null()){
      value_.set_int(1);
    }
    return RC::SUCCESS;
  }
  if(accept_null_ || !value.is_null()){
    Value i1(1);
    Value::add(i1, value_, value_);
  }
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value& result){
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value){
  // 传入的value必须为int或者float类型，并最终都转换为
  Value real_value;
  RC rc = Value::cast_to(value, AttrType::FLOATS, real_value);
  if(OB_FAIL(rc)){
    LOG_DEBUG("Failed to cast %s to floats when avg aggragate", attr_type_to_string(value.attr_type()));
    return rc;
  }
  if(value_.attr_type() == AttrType::UNDEFINED){
    value_ = real_value;
    if(!real_value.is_null()){
      count += 1;
    }
    return RC::SUCCESS;
  }
  
  if(real_value.is_null()){
    // 新传入的值为null，忽略
    return RC::SUCCESS;
  }else if(value_.is_null()){
    // 当前值为null，则用传入值覆盖
    value_ = real_value;
    count += 1;
    return RC::SUCCESS;
  }else{
    // 两个值都不为null，正常累加
    Value::add(value_, real_value, value_);
    count += 1;
    return RC::SUCCESS;
  }
}

RC AvgAggregator::evaluate(Value& result){
  if(value_.is_null() || count == 0){
    result = value_;
    return RC::SUCCESS;
  }

  value_.set_float(value_.get_float() / count);
  result = value_;
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value){
  if(value_.attr_type() == AttrType::UNDEFINED){
    value_ = value;
  }

  ASSERT(value_.attr_type() == value.attr_type(), "value type is different, %s and %s", 
    attr_type_to_string(value_.attr_type()), 
    attr_type_to_string(value.attr_type())
  );

  if(value.is_null()){
    // 新传入的值为null，忽略
    return RC::SUCCESS;
  }else if(value_.is_null()){
    // 当前值为null，则用传入值覆盖
    value_ = value;
    return RC::SUCCESS;
  }else{
    // 两个值都不为null，正常比较
    int comp = value_.compare(value);
    if(comp == -1){
      value_ = value;
    }
    return RC::SUCCESS;
  }

  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value& result){
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value){
  if(value_.attr_type() == AttrType::UNDEFINED){
    value_ = value;
  }

  ASSERT(value_.attr_type() == value.attr_type(), "value type is different, %s and %s", 
    attr_type_to_string(value_.attr_type()), 
    attr_type_to_string(value.attr_type())
  );

  if(value.is_null()){
    // 新传入的值为null，忽略
    return RC::SUCCESS;
  }else if(value_.is_null()){
    // 当前值为null，则用传入值覆盖
    value_ = value;
    return RC::SUCCESS;
  }else{
    // 两个值都不为null，正常比较
    int comp = value_.compare(value);
    if(comp == 1){
      value_ = value;
    }
    return RC::SUCCESS;
  }

  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value& result){
  result = value_;
  return RC::SUCCESS;
}
