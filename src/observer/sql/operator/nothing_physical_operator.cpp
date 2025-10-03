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
// Created by WangYunlai on 2022/07/01.
//

#include "sql/operator/nothing_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

RC NothingPhysicalOperator::open(Trx *trx)
{
  return RC::SUCCESS;
}

RC NothingPhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC NothingPhysicalOperator::close()
{
  return RC::SUCCESS;
}
Tuple *NothingPhysicalOperator::current_tuple()
{
  return nullptr;
}

RC NothingPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  return RC::SUCCESS;
}