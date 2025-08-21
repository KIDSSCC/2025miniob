#!/bin/bash
# Usage:
# ./build.sh
# ./build.sh -d

# 获取脚本的绝对路径
script_dir=$(dirname "$(realpath "${BASH_SOURCE[0]}")")

build_dir="$script_dir/../build"
cd "$build_dir"

./bin/observer -f ../etc/observer.ini -P cli