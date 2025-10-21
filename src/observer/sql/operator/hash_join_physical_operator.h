/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include <unordered_map>

struct ValueHash{
    size_t operator()(const Value &v) const noexcept{
        size_t h1 = std::hash<std::string>()(v.to_string());
        size_t h2 = std::hash<std::string>()(attr_type_to_string(v.attr_type()));
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

struct ValueEqual{
    bool operator()(const Value &a, const Value &b) const noexcept {
        size_t a1 = std::hash<std::string>()(a.to_string());
        size_t a2 = std::hash<std::string>()(attr_type_to_string(a.attr_type()));
        size_t a3 = a1 ^ (a2 + 0x9e3779b9 + (a1 << 6) + (a1 >> 2));

        size_t b1 = std::hash<std::string>()(b.to_string());
        size_t b2 = std::hash<std::string>()(attr_type_to_string(b.attr_type()));
        size_t b3 = b1 ^ (b2 + 0x9e3779b9 + (b1 << 6) + (b1 >> 2));

        return a3 == b3;
    }
};

/**
 * @brief Hash Join 算子
 * @ingroup PhysicalOperator
 */
class HashJoinPhysicalOperator : public PhysicalOperator
{
public:
    HashJoinPhysicalOperator(unique_ptr<Expression>& left_expr, unique_ptr<Expression>& right_expr);
    virtual ~HashJoinPhysicalOperator() = default;

    PhysicalOperatorType type() const override { return PhysicalOperatorType::HASH_JOIN; }

    OpType get_op_type() const override { return OpType::INNERHASHJOIN; }

    virtual double calculate_cost(LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
    {
        return 0.0;
    }

    RC     open(Trx *trx) override;
    RC     next() override;
    RC     close() override;
    Tuple *current_tuple() override;

    RC left_next();

protected:
    using IterType = std::unordered_multimap<Value, std::unique_ptr<ValueListTuple>, ValueHash, ValueEqual>::iterator;

private:
    Trx *trx_ = nullptr;

    PhysicalOperator *left_        = nullptr;
    PhysicalOperator *right_       = nullptr;
    Tuple            *left_tuple_  = nullptr;

    unique_ptr<Expression> left_expr_;
    unique_ptr<Expression> right_expr_;
    JoinedTuple       joined_tuple_;

    std::unordered_multimap<Value, unique_ptr<ValueListTuple>, ValueHash, ValueEqual> right_hash;
    IterType right_iter;
    std::pair<IterType, IterType> candidate_right;
    bool right_is_null = true;
};