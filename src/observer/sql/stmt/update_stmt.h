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
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"
#include "sql/parser/expression_binder.h"

class Db;
class Table;
class FilterStmt;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt(Table *table, vector<unique_ptr<Expression>>& values, vector<int> value_amount, FilterStmt *filter_stmt);
  ~UpdateStmt() override;

public:
  static RC create(Db *db, UpdateSqlNode &update_sql, Stmt *&stmt);
  StmtType type() const override { return StmtType::UPDATE; }

public:
  Table *table() const { return table_; }
  vector<unique_ptr<Expression>>& values() { return values_; }
  vector<int> value_amount() { return value_amount_; }
  FilterStmt *filter_stmt() const { return filter_stmt_; }

public:
  Db *db_ = nullptr;

private:
  Table *table_        = nullptr;
  vector<unique_ptr<Expression>> values_;
  vector<int> value_amount_;
  FilterStmt *filter_stmt_ = nullptr;
};
