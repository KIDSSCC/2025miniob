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
// Created by WangYunlai on 2022/6/9.
//

#pragma once

#include "sql/operator/physical_operator.h"

class Trx;

/**
 * @brief 物理算子，创建视图
 * @ingroup PhysicalOperator
 */
class CreateViewPhysicalOperator : public PhysicalOperator
{
public:
  CreateViewPhysicalOperator() {}

  virtual ~CreateViewPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::CREATE_VIEW; }

  OpType get_op_type() const override { return OpType::CREATE_VIEW; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

public:
  Trx           *trx_   = nullptr;

  Db            *db_ = nullptr;
  string table_name_;
  vector<string>          src_fields_;
  vector<AttrInfoSqlNode> attr_infos_;
  vector<string>          primary_keys_;
  StorageFormat           storage_format_;
};
