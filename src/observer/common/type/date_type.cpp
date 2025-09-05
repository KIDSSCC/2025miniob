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
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"

// 把日期转成"天数编号"，这里选择以 0001-01-01 为 day 1
long long to_days(const int& d) {
  int year  = d / 10000;
  int month = (d / 100) % 100;
  int day   = d % 100;
    long long days = 0;

    // 1. 累加从 1 年到 (year-1) 年的天数
    for (int y = 1; y < year; ++y) {
        days += DateType::is_leap_year(y) ? 366 : 365;
    }

    // 2. 累加当年已过的月份天数
    for (int m = 1; m < month; ++m) {
        days += DateType::days_in_month(year, m);
    }

    // 3. 加上当月的天数
    days += day;

    return days;
}

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not Date");
  ASSERT(right.attr_type() == AttrType::DATES, "right type is not Date");
  if (right.attr_type() == AttrType::DATES) {
    return common::compare_date((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
  } 

  return INT32_MAX; // 未实现的比较
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
  case AttrType::INTS: {
    int date_value = val.get_int();
    result.set_int(date_value);
    return RC::SUCCESS;
  }
  default:
    LOG_WARN("unsupported type %d", type);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

int DateType::cast_cost(AttrType type)
{
  if (type == AttrType::DATES) {
    return 0;
  }
  if (type == AttrType::CHARS) {
    return 2; // date -> char设置为2， char -> date设置为1。比较时优先转换为char
  }
  return INT32_MAX;
}

RC DateType::add(const Value &left, const Value &right, Value &result) const
{
  // result.set_int(left.get_int() + right.get_int());
  // return RC::SUCCESS;
  return RC::UNSUPPORTED;
}

RC DateType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_int(to_days(left.get_int()) - to_days(right.get_int()));
  return RC::SUCCESS;
}

RC DateType::multiply(const Value &left, const Value &right, Value &result) const
{
  // result.set_int(left.get_int() * right.get_int());
  // return RC::SUCCESS;
  return RC::UNSUPPORTED;
}

RC DateType::negative(const Value &val, Value &result) const
{
  // result.set_int(-val.get_int());
  // return RC::SUCCESS;
  return RC::UNSUPPORTED;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  // RC                rc = RC::SUCCESS;
  // stringstream deserialize_stream;
  // deserialize_stream.clear();  // 清理stream的状态，防止多次解析出现异常
  // deserialize_stream.str(data);
  // int int_value;
  // deserialize_stream >> int_value;
  // if (!deserialize_stream || !deserialize_stream.eof()) {
  //   rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
  // } else {
  //   val.set_int(int_value);
  // }
  // return rc;
  return RC::UNSUPPORTED;
}

RC DateType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  int date_value = val.value_.int_value_;
  int year = date_value / 10000;
  int month = (date_value / 100) % 100;
  int day = date_value % 100;
  ss << year << "-" << (month < 10 ? "0" : "") << month << "-" << (day < 10 ? "0" : "") << day;
  result = ss.str();
  return RC::SUCCESS;
}

bool DateType::is_leap_year(int year)
{
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

int DateType::days_in_month(int year, int month)
{
  static const int days[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    if (month == 2 && DateType::is_leap_year(year)) {
        return 29;
    }
    return days[month];
}