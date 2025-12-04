# WhaleX DAT 数据驱动

<p align="center"></p>

<p align="center">
  <a href="https://github.com/SDIC-Intelligence/whalex-data-driver/stargazers">
    <img src="https://img.shields.io/github/stars/SDIC-Intelligence/whalex-data-driver.svg" alt="GitHub Stars">
  </a>
  <a href="https://github.com/SDIC-Intelligence/whalex-data-driver/releases">
    <img src="https://img.shields.io/github/release/SDIC-Intelligence/whalex-data-driver.svg" alt="GitHub Release">
  </a>
  <a href="https://opensource.org/licenses/Apache-2.0">
    <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License">
  </a>
  <a href="https://zread.ai/SDIC-Intelligence/whalex-data-driver" target="_blank">
    <img src="https://img.shields.io/badge/Ask_Zread-_.svg?style=flat&color=00b0aa&labelColor=000000&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyB3aWR0aD0iMTYiIGhlaWdodD0iMTYiIHZpZXdCb3g9IjAgMCAxNiAxNiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTQuOTYxNTYgMS42MDAxSDIuMjQxNTZDMS44ODgxIDEuNjAwMSAxLjYwMTU2IDEuODg2NjQgMS42MDE1NiAyLjI0MDFWNC45NjAxQzEuNjAxNTYgNS4zMTM1NiAxLjg4ODEgNS42MDAxIDIuMjQxNTYgNS42MDAxSDQuOTYxNTZDNS4zMTUwMiA1LjYwMDEgNS42MDE1NiA1LjMxMzU2IDUuNjAxNTYgNC45NjAxVjIuMjQwMUM1LjYwMTU2IDEuODg2NjQgNS4zMTUwMiAxLjYwMDEgNC45NjE1NiAxLjYwMDFaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00Ljk2MTU2IDEwLjM5OTlIMi4yNDE1NkMxLjg4ODEgMTAuMzk5OSAxLjYwMTU2IDEwLjY4NjQgMS42MDE1NiAxMS4wMzk5VjEzLjc1OTlDMS42MDE1NiAxNC4xMTM0IDEuODg4MSAxNC4zOTk5IDIuMjQxNTYgMTQuMzk5OUg0Ljk2MTU2QzUuMzE1MDIgMTQuMzk5OSA1LjYwMTU2IDE0LjExMzQgNS42MDE1NiAxMy43NTk5VjExLjAzOTlDNS42MDE1NiAxMC42ODY0IDUuMzE1MDIgMTAuMzk5OSA0Ljk2MTU2IDEwLjM5OTlaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik0xMy43NTg0IDEuNjAwMUgxMS4wMzg0QzEwLjY4NSAxLjYwMDEgMTAuMzk4NCAxLjg4NjY0IDEwLjM5ODQgMi4yNDAxVjQuOTYwMUMxMC4zOTg0IDUuMzEzNTYgMTAuNjg1IDUuNjAwMSAxMS4wMzg0IDUuNjAwMUgxMy43NTg0QzE0LjExMTkgNS42MDAxIDE0LjM5ODQgNS4zMTM1NiAxNC4zOTg0IDQuOTYwMVYyLjI0MDFDMTQuMzk4NCAxLjg4NjY0IDE0LjExMTkgMS42MDAxIDEzLjc1ODQgMS42MDAxWiIgZmlsbD0iI2ZmZiIvPgo8cGF0aCBkPSJNNCAxMkwxMiA0TDQgMTJaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00IDEyTDEyIDQiIHN0cm9rZT0iI2ZmZiIgc3Ryb2tlLXdpZHRoPSIxLjUiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIvPgo8L3N2Zz4K&logoColor=ffffff" alt="zread"/>
  </a>
</p>

## 项目简介

WhaleX DAT 数据驱动（whalex-data-driver）是一个面向多种数据源的统一驱动框架，支持包括关系型数据库、NoSQL数据库、大数据平台、图数据库等多种数据存储系统。通过统一的API接口，简化了不同数据源的操作复杂度，提供了高性能、高可用的数据访问能力。

## 支持的数据源

- **关系型数据库**: MySQL、Oracle、PostgreSQL、SQL Server、达梦等
- **NoSQL数据库**: MongoDB、Redis、Elasticsearch、HBase等
- **大数据平台**: Hive、Hadoop、Spark等
- **图数据库**: Neo4j、JanusGraph、NebulaGraph等
- **消息队列**: Kafka
- **文件系统**: HDFS、S3、FTP等

## 技术栈

- Java 8+
- Maven 3.6+
- HuTool工具库
- Lombok
- Slf4j + Logback

## 快速开始

### 环境要求

- JDK 8 或更高版本
- Maven 3.6+

### 克隆项目

```bash
git clone https://github.com/SDIC-Intelligence/whalex-data-driver.git
cd whalex-data-driver
```

### 构建项目

项目支持多种构建模式，可以根据需要选择不同的Profile进行构建：

```bash
# 构建基础驱动
mvn clean install -P base-driver

# 构建所有模块
mvn clean install -P all

# 构建特定数据源模块（以MySQL为例）
mvn clean install -P mysql
```

构建后的jar包位于各模块的 `target` 目录下。

## 项目结构

```
whalex-data-driver/
├── whalex-db-abstract/           # 抽象层定义模块
├── whalex-data-common/           # 公共工具类模块
├── whalex-data-modal/            # 数据模型模块
├── whalex-dat-calcite-extends/   # Calcite扩展模块
├── whalex-db-rdbms/              # RDBMS基础模块
├── whalex-dat-graph/             # 图数据库模块
├── whalex-dat-filesystem/        # 文件系统模块
├── whalex-dat-cache/             # 缓存模块
├── whalex-dat-sql/               # SQL解析模块
├── 各种数据源适配模块...
```

## 组件适配流程

1. 新增一个Maven模块
   ```
   ArtifactId 命名规则：whalex-db-[厂商]-[组件]，例如 whalex-db-meiya-mongo
   ```

2. 在java目录下创建 `com.meiya.whalex.db` 根包名

3. 配置pom.xml文件，添加必要的依赖项

4. 定义标准包结构：
   ```
   -src
   --main
   ---java
   ----com.meiya.whalex.db
   -----entity.document            // 存放组件库信息类、表信息类、操作类
   -----module.document            // 存放组件操作服务接口实现类
   -----util
   ------helper.impl.document      // 存放组件配置加载、缓存和数据源创建工具类
   ------param.impl.document       // 存放组件参数转换工具类
   ---resources                    // 组件相关配置文件
   ```

5. 定义组件相关类：
   - 组件数据库配置信息实体（继承AbstractDatabaseInfo.java类）
   - 数据库表配置信息实体（继承AbstractDbTableInfo.java类）
   - 组件操作实体（继承AbstractDbHandler.java类）

6. 定义组件数据源配置管理类（继承AbstractDbModuleConfigHelper类）

7. 定义组件参数转换工具类（继承AbstractDbModuleParamUtil类）

8. 定义组件服务实现类（继承AbstractDbModuleBaseService抽象类）

详细开发指南请参考[开发文档](docs/development-guide.md)。

## 使用示例

```java
// 示例代码
// TODO: 添加具体的使用示例
```

## 配置说明

主要配置文件位于各个模块的 `src/main/resources` 目录下，可根据实际需求进行调整。

## 测试

运行单元测试：
```bash
mvn test
```

运行集成测试：
```bash
mvn verify
```

## 详细技术文档

[![zread](https://img.shields.io/badge/Ask_Zread-_.svg?style=flat&color=00b0aa&labelColor=000000&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyB3aWR0aD0iMTYiIGhlaWdodD0iMTYiIHZpZXdCb3g9IjAgMCAxNiAxNiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTQuOTYxNTYgMS42MDAxSDIuMjQxNTZDMS44ODgxIDEuNjAwMSAxLjYwMTU2IDEuODg2NjQgMS42MDE1NiAyLjI0MDFWNC45NjAxQzEuNjAxNTYgNS4zMTM1NiAxLjg4ODEgNS42MDAxIDIuMjQxNTYgNS42MDAxSDQuOTYxNTZDNS4zMTUwMiA1LjYwMDEgNS42MDE1NiA1LjMxMzU2IDUuNjAxNTYgNC45NjAxVjIuMjQwMUM1LjYwMTU2IDEuODg2NjQgNS4zMTUwMiAxLjYwMDEgNC45NjE1NiAxLjYwMDFaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00Ljk2MTU2IDEwLjM5OTlIMi4yNDE1NkMxLjg4ODEgMTAuMzk5OSAxLjYwMTU2IDEwLjY4NjQgMS42MDE1NiAxMS4wMzk5VjEzLjc1OTlDMS42MDE1NiAxNC4xMTM0IDEuODg4MSAxNC4zOTk5IDIuMjQxNTYgMTQuMzk5OUg0Ljk2MTU2QzUuMzE1MDIgMTQuMzk5OSA1LjYwMTU2IDE0LjExMzQgNS42MDE1NiAxMy43NTk5VjExLjAzOTlDNS42MDE1NiAxMC42ODY0IDUuMzE1MDIgMTAuMzk5OSA0Ljk2MTU2IDEwLjM5OTlaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik0xMy43NTg0IDEuNjAwMUgxMS4wMzg0QzEwLjY4NSAxLjYwMDEgMTAuMzk4NCAxLjg4NjY0IDEwLjM5ODQgMi4yNDAxVjQuOTYwMUMxMC4zOTg0IDUuMzEzNTYgMTAuNjg1IDUuNjAwMSAxMS4wMzg0IDUuNjAwMUgxMy43NTg0QzE0LjExMTkgNS42MDAxIDE0LjM5ODQgNS4zMTM1NiAxNC4zOTg0IDQuOTYwMVYyLjI0MDFDMTQuMzk4NCAxLjg4NjY0IDE0LjExMTkgMS42MDAxIDEzLjc1ODQgMS42MDAxWiIgZmlsbD0iI2ZmZiIvPgo8cGF0aCBkPSJNNCAxMkwxMiA0TDQgMTJaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00IDEyTDEyIDQiIHN0cm9rZT0iI2ZmZiIgc3Ryb2tlLXdpZHRoPSIxLjUiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIvPgo8L3N2Zz4K&logoColor=ffffff)](https://zread.ai/SDIC-Intelligence/whalex-data-driver)

## 贡献指南

我们欢迎任何形式的贡献，包括但不限于：

1. Bug修复
2. 功能增强
3. 文档完善
4. 性能优化

贡献步骤：
1. Fork项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启Pull Request

请确保您的代码遵循项目的编码规范，并通过所有测试。

## 许可证

本项目采用Apache License 2.0许可证，详情请见[LICENSE](LICENSE)文件。

## 联系方式

- 项目维护者: SDIC Intelligence团队
- 邮箱: qkos@300188.cn
- GitHub Issues: [项目问题跟踪](https://github.com/SDIC-Intelligence/whalex-data-driver/issues)

## 致谢

感谢所有为本项目做出贡献的开发者们！