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

#include "sql/operator/view_translate_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

ViewTranslatePhysicalOperator::ViewTranslatePhysicalOperator(Table* table, shared_ptr<PhysicalOperator> content)
  : content_(content)
{
  table_ = table;
}

RC ViewTranslatePhysicalOperator::open(Trx *trx)
{
  const vector<FieldMeta>* field_metas = table_->table_meta().field_metas();
  for(size_t i=table_->table_meta().sys_field_num();i<field_metas->size();i++){
    TupleCellSpec spec(table_->name(), field_metas->at(i).name());
    specs_.push_back(spec);
  }
  return content_->open(trx);
}

RC ViewTranslatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  bool filter_result = false;
  while((rc = content_->next()) == RC::SUCCESS){
    Tuple* curr_tuple = content_->current_tuple();
    tuple_.clear();
    ValueListTuple::make(*curr_tuple, tuple_);
    tuple_.set_spec(specs_);
    
    rc = filter(tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      LOG_TRACE("record filtered failed=%s", strrc(rc));
      return rc;
    }

    if (filter_result) {
      break;
    }
  }
  return rc;
}

RC ViewTranslatePhysicalOperator::close()
{
  return content_->close();
}
Tuple *ViewTranslatePhysicalOperator::current_tuple()
{
  LOG_INFO("view tuple %s, spec %s", tuple_.to_string().c_str(), tuple_.spec_to_string().c_str());
  return &tuple_;
}

RC ViewTranslatePhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  return content_->tuple_schema(schema);
}

RC ViewTranslatePhysicalOperator::filter(Tuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = (value.get_boolean() == 1);
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}

RC ViewTranslatePhysicalOperator::need_row() {

  RC rc = content_->need_row();
  return rc;
}

RC ViewTranslatePhysicalOperator::get_row_tuple(Table* table, Tuple*& tuple) {
  return content_->get_row_tuple(table, tuple);
}