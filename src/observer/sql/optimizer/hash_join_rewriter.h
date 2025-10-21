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
// Created by Wangyunlai on 2022/12/30.
//

#pragma once

#include "common/lang/vector.h"
#include "sql/optimizer/rewrite_rule.h"

/**
 * @brief 将predicate中单独的等值比较下推到join中，构造hash join
 * @ingroup Rewriter
 * @details 这样可以加快join连接的速度
 */
class HashJoinRewriter : public RewriteRule
{
public:
  HashJoinRewriter()          = default;
  virtual ~HashJoinRewriter() = default;

  RC rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made) override;
};
