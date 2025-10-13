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

#include "sql/operator/projectpack_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

ProjectPackPhysicalOperator::ProjectPackPhysicalOperator(shared_ptr<PhysicalOperator> content)
  : content_(content)
{
}

RC ProjectPackPhysicalOperator::open(Trx *trx)
{
  return content_->open(trx);
}

RC ProjectPackPhysicalOperator::next()
{
  return content_->next();
}

RC ProjectPackPhysicalOperator::close()
{
  return content_->close();
}
Tuple *ProjectPackPhysicalOperator::current_tuple()
{
  return content_->current_tuple();
}

RC ProjectPackPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  return content_->tuple_schema(schema);
}