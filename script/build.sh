#!/bin/bash
# Usage:
# ./build.sh
# ./build.sh -d

# 遇到错误时返回
set -e

# 获取脚本的绝对路径
script_dir=$(dirname "$(realpath "${BASH_SOURCE[0]}")")

# 重新编译解析器
cd "$script_dir"
./gen_parser.sh

build_dir="$script_dir/../build"
cd "$build_dir"

# 初始化一个变量来标记是否包含 -d 选项
d_flag=false

# 使用 getopts 解析命令行选项
while getopts "d" opt; do
    case $opt in
        d)
            d_flag=true  # 如果有 -d 选项，则标记为 true
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

# rm -rf CMakeCache.txt
rm -rf *log*

if [ "$d_flag" = true ]; then
    rm -rf miniob
    # 在这里编写需要在有 -d 选项时执行的操作
fi

cmake ..
make -j16
