# XQ - SQL Query Tool for Multiple Data Sources

XQ是一个强大的命令行工具，允许你使用SQL语法从多种数据源（HTTP、本地文件等）查询数据，并以表格、JSON或CSV格式展示结果。

## 特性

- 🔍 **SQL查询支持**: 支持标准SQL语法进行数据查询
- 🌐 **多数据源**: 支持HTTP/HTTPS URL和本地文件（CSV、JSON格式）
- 📊 **多种输出格式**: 表格、JSON、CSV格式输出
- 🚀 **高性能**: 基于Polars构建，提供快速的数据处理能力
- 🎯 **丰富的SQL功能**:
  - SELECT with column selection or *
  - WHERE条件过滤
  - GROUP BY分组
  - 聚合函数: SUM, COUNT (支持 COUNT(1), COUNT(*), COUNT(column)), MAX, MIN, AVG
  - ORDER BY排序（ASC/DESC）
  - LIMIT和OFFSET分页

## 安装

```bash
cargo build --release
```

## 使用方法

### 基本用法

```bash
xq "<SQL_QUERY>" [--format <FORMAT>]
```

### 参数说明

- `SQL_QUERY`: SQL查询语句
- `--format`: 输出格式，可选值：table（默认）、json、csv

### 示例

#### 1. 查询CSV文件

```bash
# 查询本地CSV文件
xq "SELECT * FROM file:///path/to/data.csv WHERE value > 100"

# 带条件和排序
xq "SELECT name, age, score FROM file:///data.csv WHERE age > 20 ORDER BY score DESC"
```

#### 2. 查询JSON文件

```bash
# 查询JSON文件并分组统计
xq "SELECT city, COUNT(*) as count, AVG(salary) FROM file:///data.json GROUP BY city"
```

#### 3. 查询HTTP数据源

```bash
# 从HTTP URL查询CSV数据
xq "SELECT location, total_cases FROM https://example.com/covid-data.csv WHERE total_cases > 1000000 LIMIT 10"
```

#### 4. 不同输出格式

```bash
# 表格格式（默认）
xq "SELECT * FROM file:///data.csv"

# JSON格式
xq "SELECT * FROM file:///data.csv" --format json

# CSV格式
xq "SELECT * FROM file:///data.csv" --format csv
```

#### 5. 复杂查询示例

```bash
# 多重聚合函数
xq "SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary, MAX(salary) as max_salary FROM file:///employees.csv GROUP BY department ORDER BY avg_salary DESC"

# 使用LIMIT和OFFSET
xq "SELECT * FROM file:///large_dataset.csv ORDER BY timestamp DESC LIMIT 100 OFFSET 200"
```

## 运行示例

项目包含了完整的示例文件和演示程序：

```bash
# 运行演示程序
cargo run --example demo

# 运行COVID数据示例
cargo run --example covid
```

## 测试

```bash
# 运行所有测试
cargo test

# 运行特定测试
cargo test test_csv_query
```

## 技术栈

- **Rust**: 系统编程语言
- **Polars**: 高性能数据处理框架
- **SQLParser**: SQL解析器
- **Tokio**: 异步运行时
- **Reqwest**: HTTP客户端
- **PrettyTable**: 表格格式化

## 项目结构

```
xq/
├── src/
│   ├── main.rs        # 主程序入口
│   ├── lib.rs         # 核心库功能
│   ├── convert.rs     # SQL到Polars表达式转换
│   ├── dialect.rs     # SQL方言定义
│   ├── fetcher.rs     # 数据获取模块
│   └── loader.rs      # 数据加载模块
├── examples/
│   ├── demo.rs        # 演示程序
│   ├── covid.rs       # COVID数据查询示例
│   ├── a.csv          # 示例CSV文件
│   └── data.json      # 示例JSON文件
└── tests/             # 测试文件
```

## 贡献

欢迎提交Issue和Pull Request！

## 许可证

MIT License