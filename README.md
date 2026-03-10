# Gupiao Lab

Windows-first 的本地桌面辅助选股工具。桌面层使用 `PyInstaller + pywebview + React + TypeScript`，分析引擎使用 `Python + FastAPI + SQLite`。运行时固定使用 `akshare` 抓取沪深主板与创业板 A 股数据，服务目标是“收盘后筛候选 + 观察池跟踪”，不是研究平台。

当前版本新增了第二层本地离线研究校准链路：

- 业务库继续服务主 UI，不阻塞同步和候选体验
- 研究库单独存放在 `gupiao_research.db`
- 研究刷新默认在同步成功后后台增量启动
- 只有高质量 `actual` PIT 样本会进入 headline 统计并接管线上参数
- 在仍使用免费公开代理口径时，研究状态可能显示为“研究受限”，主流程仍可正常使用

## 现在保留的功能

- A 股全市场同步：覆盖沪深主板 + 创业板，带同步进度、覆盖率和基础数据审计
- 默认候选流水线：固定单一 `balanced` 候选逻辑，输出今日候选、综合强度、计划位和风险提示
- 单股详情：展示当前概览、最近走势、操作计划、关键风险和折叠的高级分析
- 隐藏研究诊断：在“展开高级分析”里查看当前参数版本、样本覆盖和因子漂移
- 观察池：只保留目标价、止损、止盈、备注 4 个核心字段
- 每日报告：只输出“今天先看 / 为什么看 / 今天别做什么”
- 本地缓存：SQLite 落地 `stock_meta`、`daily_price`、`financial_snapshot`、`factor_snapshot`、`strategy_run`、`validation_cache`、`data_audit_snapshot`、`eligibility_snapshot`、`model_health_snapshot`、`daily_report`、`watchlist_item`
- 研究库：SQLite 单独落地 `research_security_state_event`、`research_price_bar`、`research_financial_record`、`research_sample`、`research_parameter_version`、`research_diagnostic_snapshot`

## 已删除的研究功能

- 策略实验室与多策略持久化
- 公开回测接口
- 公开 validation / paper performance 接口
- signal feed
- journal / 交易日志

内部仍保留校准和模型健康缓存，只用于稳定当前候选排序，不再作为公开功能展示。

## 开发环境

- Node.js 22+
- Python 3.12+
- Windows 首发；Linux/macOS 可用于开发

## 启动

1. 安装前端依赖：

```bash
npm install
```

2. 安装后端依赖到项目本地目录：

```bash
python3 -m pip install --target .pydeps -r backend/requirements.txt
```

Windows PowerShell 通常改成：

```powershell
py -3 -m pip install --target .pydeps -r backend/requirements.txt
```

3. 启动前后端开发模式：

```bash
npm run dev
```

如果要直接以桌面窗口形式从源码运行：

```bash
npm run desktop
```

## 桌面打包

```bash
npm run package
```

打包产物默认输出到 [dist/Gupiao Lab](/mnt/c/Users/bazs0/Desktop/gupiao/dist/Gupiao%20Lab)，详细流程见 [RELEASE.md](/mnt/c/Users/bazs0/Desktop/gupiao/RELEASE.md)。

## 数据源与同步

- 运行时固定使用 `akshare`
- `POST /sync/eod` 触发后台同步并返回 `run_id`
- 前端轮询 `/sync/runs/{run_id}` 获取阶段、百分比、`processed / queued / skipped`
- 同步完成表示默认候选和日报已经可用
- 同步成功后会在后台启动一次研究增量刷新，不阻塞主界面

## 离线研究刷新

手动跑一次离线研究增量刷新：

```bash
npm run research:refresh
```

如果要完整重建研究库：

```bash
node scripts/run-python.mjs -m backend.app.research_cli --mode rebuild
```

## 测试

```bash
npm run test:web
npm run test:api
```

## API

- `GET /health`
- `GET /data/status`
- `GET /analytics/research-diagnostics`
- `POST /sync/eod`
- `GET /sync/runs/latest`
- `GET /sync/runs/{run_id}`
- `GET /rankings`
- `GET /stocks/{code}`
- `GET /reports/daily`
- `GET /watchlists/{id}`
- `PUT /watchlists/{id}`

## 说明

- 当前运行时只使用 `akshare`
- 默认 UI 只保留“今日候选”和“观察池”两个主视图
- 数据状态只由同步、覆盖率、财务滞后和关键审计异常决定
- `GET /data/status` 额外返回 `research_status`、`research_as_of_date`、`parameter_version`、`research_sample_count`
- `/analytics/research-diagnostics` 只服务高级分析，不进入默认主流程
- 当前桌面壳使用 `pywebview`，Windows 目标机建议具备 WebView2 运行时
"# gupiao" 
