#!/bin/bash

# 遇到错误时返回
set -e

# 获取脚本的绝对路径
script_dir=$(dirname "$(realpath "${BASH_SOURCE[0]}")")

# 重新编译解析器
cd "$script_dir"
./gen_parser.sh

build_dir = 