#!/bin/bash

# 遇到错误时返回
set -e

# 获取脚本的绝对路径
script_dir=$(dirname "$(realpath "${BASH_SOURCE[0]}")")

# 定位到parser目录
parser_dir="$script_dir/../src/observer/sql/parser"

cd "$parser_dir"

# 生成parser文件
flex --outfile lex_sql.cpp --header-file=lex_sql.h lex_sql.l
`which bison` -d -Wcounterexamples --output yacc_sql.cpp yacc_sql.y