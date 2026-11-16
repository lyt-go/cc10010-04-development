# 数据仓库ETL任务编排平台

面向数据仓库ETL任务开发、作业编排调度、任务依赖解析、源端数据校验、异常任务重跑、血缘分板与运行全景监控的ETL工程管理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| ETL作业 | `etljob` | 作业定义 | 作业编号、作业名称、源端数据表 |
| 任务编排 | `dag` | 编排图 | 图编号、图名称、拓扑结构 |
| 依赖解析 | `dependency` | 依赖关系 | 关系编号、上游作业、下游作业 |
| 源端校验 | `sourcecheck` | 校验配置 | 配置编号、源端数据表、行数下限 |
| 运行实例 | `runtime` | 运行记录 | 记录编号、作业名称、开始时间 |
| 异常重跑 | `rerun` | 重跑任务 | 任务编号、原始运行编号、重跑区间开始 |
| 基线管理 | `baseline` | 基线定义 | 基线编号、基线名称、承诺完成时间 |
| 分区检测 | `partitioncheck` | 检测规则 | 规则编号、数据表名、期望分区数 |
| 数据比对 | `comparator` | 比对任务 | 任务编号、源表名、目标表名 |
| 元数据存储 | `metastore` | 元数据表 | 表编号、表名、所属数仓层 |
| 资源配置 | `resource` | 资源池 | 池编号、池名称、最大并发数 |
| SLA监控 | `slavalidate` | SLA规则 | 规则编号、适用作业、完成截止时间 |
| SQL模板 | `sqltpl` | 模板定义 | 模板编号、模板名称、模板类型 |
| 数据备份 | `backuprestore` | 备份策略 | 策略编号、数据表名、备份周期 |
| 日志归档 | `logarchive` | 归档策略 | 策略编号、日志类型、保留天数 |
| 通知渠道 | `notifychan` | 渠道配置 | 渠道编号、渠道类型、接收地址 |
| 发布计划 | `publishplan` | 发布单 | 发布编号、发布内容、变更说明 |

### 任务编排的拓扑发布预检

`dag` 模块在原有登记/评审/停用之外，补了一条「画布保存 → 拓扑预检 → 发布快照 →
运行侧生成失败传播清单」的链路：

```text
POST /api/dag/{id}/saves                      保存一次画布（多次保存按节点/边版本合并）
GET  /api/dag/{id}/saves                      历次保存清单
GET  /api/dag/{id}/merge                      合并后的节点、依赖边与版本决议
POST /api/dag/{id}/prechecks                  执行拓扑预检（环、孤立节点、悬空边、缺失分区）
GET  /api/dag/{id}/prechecks[/{pid}]          预检历史 / 单次明细（含阻断原因、重试次数）
POST /api/dag/{id}/publish                    发布通过预检的拓扑快照（事务内写状态+快照+传播清单）
GET  /api/dag/{id}/snapshots                  快照历史（同一时刻只有一份「生效」）
GET  /api/dag/{id}/partition-dependencies     分区依赖数据 + 失败传播清单（或阻断原因）

POST /api/runtime/{id}/propagation            按生效快照生成失败传播清单
GET  /api/runtime/{id}/propagation            读取清单，状态实时跟随编排图预检结论
GET  /api/runtime/{id}/propagations           生成历史（含阻断 / 异常记录与重试次数）
```

规则说明：

- **版本合并**：同一编排图的每一次画布保存都保留。节点按 `节点版本` 取较新版本，
  同一版本内容不一致会记入 `版本冲突` 并说明采用了哪次保存；较晚保存的孤立节点
  同样进入预检输入，不会被最后一次画布覆盖掉。撤销节点/边需要显式提交 `撤销: true`。
- **阻断判定**：存在环、孤立节点、悬空依赖边、缺失目标数据表/分区、同版本冲突时
  预检为「阻断」，返回逐条可定位的阻断原因。
- **发布约束**：只有通过预检、且预检后画布未再保存（拓扑版本与保存指纹一致）的
  拓扑才能发布；并发发布同一编排图时只有一份快照生效，其余请求幂等返回或明确拒绝。
  发布失败不覆盖旧快照，旧快照继续可执行。
- **可解释性**：预检 / 生成中途异常会留下「异常」或「预检中断」记录与原因，
  读取侧不会返回没有解释的空结果；预检恢复但尚未发布时状态为「待发布」，
  快照切换后运行侧状态为「待刷新」，都提示可以重试。

后端测试：`cd backend && python -m unittest discover -s tests -v`

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
