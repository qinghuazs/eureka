# Eureka 源码学习资料库

> 本目录是 `dongmian` 分支的核心产出：一套完整的 Netflix Eureka 1.x 源码学习资料，
> 包含 **18 份技术文档、10 篇博客、约 1050 行源码中文注释、16 个学习验证测试**，
> 全部结论经过真实编译运行验证（67 个测试通过：51 官方 + 16 学习验证）。

## 这个分支有什么

| 内容 | 位置 | 说明 |
|------|------|------|
| 📖 源码中文注释 | `eureka-client/` `eureka-core/` 的 31 个核心类 | 纯注释零代码变更，`git diff master` 可见 |
| 📑 技术文档 18 份 | `docs/*.md` | 流程分析（含时序图/状态图/ADR/FMEA）+ 横向主题 + 实践指南 |
| ✍️ 博客系列 10 篇 | `docs/blog/*.md` | 可直接发布的问题驱动式文章 |
| 🧪 学习验证测试 | `eureka-core/src/test/java/com/netflix/eureka/learning/` | 用可运行代码验证文档论断 |

## 怎么用这套资料

### 路线A：系统学习（1-2 周）

```
1. 读 [《源码阅读指南》](源码阅读指南.md) 了解全局
2. 按 01→05 顺序读核心流程文档，同时对照带注释的源码
3. 读 06/07/09 横向主题补全细节
4. 跟着 08 文档在本机把项目跑起来
5. 运行学习验证测试，确认理解
6. 用 12 面试题集自测
```

### 路线B：快速面试准备（1-2 天）

```
1. 读 11《全景架构大图》建立框架
2. 精读 12《源码级面试题集》16 道题
3. 对不熟的题目跳回对应流程文档补课
```

### 路线C：碎片阅读（通勤场景）

直接读 `blog/` 目录的 10 篇博客，每篇 10 分钟，连起来就是完整体系。

## 文档目录

**0. [源码阅读指南](源码阅读指南.md) —— 先读这篇：全局总纲、模块划分与推荐阅读顺序**

### 核心流程（mfd full 模板：TL;DR/C4/ADR/时序图/状态图/FMEA/面试题）

1. [服务注册流程](01.服务注册流程.md) —— register 链路 + 集群复制
2. [心跳续约流程](02.心跳续约流程.md) —— 保活 + 404/409 数据对账
3. [拉取注册表流程](03.拉取注册表流程.md) —— 三级缓存 + 增量/全量
4. [服务下线与剔除流程](04.服务下线与剔除流程.md) —— 主动/被动下线 + 自我保护
5. [服务端启动与集群同步流程](05.服务端启动与集群同步流程.md) —— 自举设计 + syncUp + 批处理

### 横向主题

6. [客户端传输层与故障转移](06.客户端传输层与故障转移.md) —— 装饰器链
7. [状态覆盖规则链](07.状态覆盖规则链.md) —— 状态裁决机制
8. [本地运行与测试验证指南](08.本地运行与测试验证指南.md) —— JDK21 实测全记录
9. [数据模型与序列化批处理](09.数据模型与序列化批处理.md) —— InstanceInfo/hashcode/batcher

### 扩展与输出

10. [SpringCloud集成与注册中心对比](10.SpringCloud集成与注册中心对比.md)
11. [全景架构大图](11.全景架构大图.md)
12. [源码级面试题集](12.源码级面试题集.md)
13. [博客系列索引](13.博客系列大纲与样例.md) → [blog/ 目录](blog/)
14. [源码学习方法论](14.源码学习方法论.md) ★ 本项目方法的完整提炼，可复用于任何项目

### 补充主题（覆盖盲区补全）

15. [客户端健康检查机制](15.客户端健康检查机制.md) —— HealthCheckHandler/Callback/Bridge，连 Spring Cloud `healthcheck.enabled`
16. [服务端请求过滤器链](16.服务端请求过滤器链.md) —— StatusFilter/Auth/RateLimiting/Gzip 四道入口关口
17. [客户端冷启动与 backupRegistry 兜底](17.客户端冷启动与backupRegistry兜底.md) —— Server 全挂时新客户端如何自救
18. [Eureka 的 AWS 基因（选读）](18.Eureka的AWS基因.md) —— EIP 绑定 / ASG 整组摘流 / DataCenterInfo 的来历；自建机房不触发

## 快速命令

```bash
# 编译（需先创建阿里云镜像init脚本,见文档08）
./gradlew -I /tmp/eureka-init.gradle :eureka-core:compileJava :eureka-client:compileJava

# 运行学习验证测试（验证文档中的核心论断）
./gradlew -I /tmp/eureka-init.gradle :eureka-core:test --tests "*LearningVerificationTest"

# 运行端到端集成测试（启动真实的Eureka Server）
./gradlew -I /tmp/eureka-init.gradle :eureka-server:test --tests "*EurekaClientServerRestIntegrationTest"

# 查看所有中文注释（31 个核心类、约 1050 行；排除学习验证测试文件）
git diff master --stat -- '*.java' ':(exclude)*learning*'
```

## 学习成果一览

```
✅ 5 条核心流程全链路分析（注册/续约/拉取/下线/启动）
✅ 31 个核心类中文注释（约 1050 行）
✅ 39 张 mermaid 图（时序/流程/状态/架构）
✅ JDK 21 + macOS 实测通过（51 个官方测试 + 16 个学习验证测试）
✅ 16 道源码级面试题
✅ 10 篇可发布博客（约 3 万字）
```

---
*生成时间：2026-06 | 基于 Netflix Eureka 1.x master 分支（commit 8227a727）*
