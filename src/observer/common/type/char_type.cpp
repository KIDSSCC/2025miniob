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
#include <cstdlib> // for strtod
#include <cerrno>  // for errno

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

float string2float(const std::string& str){
  if (str.empty()) {
      return 0.0;
  }
  char* endptr = nullptr;
  errno = 0; // 重置 errno
  float result = std::strtod(str.c_str(), &endptr);
  // 检查是否发生错误（如溢出）
  if (errno == ERANGE) {
      // 溢出时根据需求可返回 0 或 HUGE_VAL，这里按题意返回 0
      return 0.0;
  }

  // 如果没有任何字符被转换（endptr 指向开头），说明无法转换
  if (endptr == str.c_str()) {
      return 0.0;
  }

  return result;
}

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

int CharType::like_compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  const char *str = left.value_.pointer_value_;
  const char *pat = right.value_.pointer_value_;
  int         slen = left.length_;
  int         plen = right.length_;

  int i = 0;        // left 当前匹配位置
  int j = 0;        // right 当前匹配位置
  int last_star = -1;   // 最近一个 '%' 在 right 中的位置
  int last_star_pos_in_left = 0; // 当时 left 的位置，用于回溯
  while (i < slen) {
    if(j < plen){
      char p = pat[j];
      if (p == '%') {
        // 记录 '%' 的位置，并尝试匹配零个字符
        last_star = j;
        last_star_pos_in_left = i;
        j++;  // 移动到下一个模式字符
        continue;
      }
      // 普通字符或 '_' 匹配
      bool matched = false;
      if (p == '_') {
        // '_' 匹配一个非单引号字符
        if (str[i] != '\'') {
            matched = true;
        }
      }else{
        // 普通字符必须完全相等，且不能是 '\''
        if (str[i] == p && str[i] != '\'') {
            matched = true;
        }
      }
      if (matched) {
        i++;
        j++;
        continue;
      }
    }
    // 如果当前无法匹配，且之前有 '%'，尝试让 '%' 多吞一个字符
    if (last_star != -1) {
      // 回溯：让 '%' 多覆盖一个字符
      last_star_pos_in_left++;
      i = last_star_pos_in_left;
      j = last_star + 1;  // 模式回到 '%' 后一位
      continue;
    }
    // 无法匹配，且无回溯点
    return 0;
  }
  // left 已匹配完，跳过 right 末尾所有连续的 '%'
  while (j < plen && pat[j] == '%') {
      j++;
  }
  // 所有模式字符都处理完才算匹配成功
  return j == plen;
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
    } break;
    case AttrType::FLOATS:{
      string str = val.get_string();
      float res = string2float(str);
      result.set_float(res);
      return RC::SUCCESS;
    } break;
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
  if (type == AttrType::FLOATS) {
    return 1; // char -> float 设置为1， float -> char 设置为2，优先转换为float
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