/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"

#include <regex>

bool parse_date(const std::string& date_str, int& result){
  // 用正则表达式匹配 YYYY-MM-DD
  static const std::regex pattern(R"(^(\d{4})[-](\d{1,2})[-](\d{1,2})$)");
  std::smatch match;

  if (!std::regex_match(date_str, match, pattern)) {
    return false; // 格式不符
  }
  try{
    int year  = std::stoi(match[1].str());
    int month = std::stoi(match[2].str());
    int day   = std::stoi(match[3].str());

    // 月份合法性检查
    if (month < 1 || month > 12) return false;

    // 天数合法性检查
    int max_day = DateType::days_in_month(year, month);
    if (day < 1 || day > max_day) return false;

    result = year * 10000 + month * 100 + day; // 转换为 YYYYMMDD 格式
    return true;
  } catch (...){
    LOG_ERROR("Failed to parse date string: %s", date_str.c_str());
    return false; // 转换失败
  }
}

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATES:{
      // 字符串转换为日期类型
      string str = val.get_string();
      int date_value = 0;
      if (!parse_date(str, date_value)) {
        LOG_WARN("Invalid date format: %s", str.c_str());
        return RC::INVALID_ARGUMENT;  
      }
      else{
        result.set_date(date_value);
        return RC::SUCCESS;
      }
    }
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  if (type == AttrType::DATES) {
    return 1; // 假设转换到日期类型的开销为1
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}