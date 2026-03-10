# 发布说明

这个项目当前采用 `PyInstaller` 生成的 Windows 桌面目录包发布。每次更新后，都重新打包一个新的目录或压缩包发给用户。

## 常规发版流程

1. 修改版本号  
   打开 [package.json](/mnt/c/Users/bazs0/Desktop/gupiao/package.json)，更新 `version`。

2. 运行检查

```bash
npm run lint
npm run test:api
npm run test:web
```

3. 重新打包

```bash
npm run package
```

4. 取发布产物

- [dist/Gupiao Lab](/mnt/c/Users/bazs0/Desktop/gupiao/dist/Gupiao%20Lab)
- [dist/Gupiao Lab/Gupiao Lab.exe](/mnt/c/Users/bazs0/Desktop/gupiao/dist/Gupiao%20Lab/Gupiao%20Lab.exe)

## 什么时候必须重新打包

- 前端页面、交互、样式有改动
- Python 桌面 launcher 有改动
- Python 后端代码有改动
- `backend/requirements-desktop.txt` 有改动
- `backend/requirements.txt` 有改动且开发环境依赖需要同步
- 打包脚本、应用名、版本号有改动

## 当前产品发布口径

发版前确认下面 5 件事：

- 首页能正常打开
- 点击“同步收盘数据”不会报错，顶部进度条会推进
- 今日候选、单股详情、日报、观察池能正常加载
- 真实数据模式下 `akshare` 同步能启动
- `dist/Gupiao Lab/Gupiao Lab.exe` 能在 Windows 下正常打开

当前版本不再包含：

- 策略实验室
- 回测入口
- validation / paper performance 页面
- signal feed
- journal / 日志入口

当前版本新增：

- 独立研究库 `gupiao_research.db`
- 同步成功后的后台研究增量刷新
- 隐藏的高级研究诊断接口 `/analytics/research-diagnostics`
- 本地研究重建命令 `npm run research:refresh`

## 给用户发新版时的注意事项

- 重新打包不会自动覆盖用户本地数据库
- 用户数据在 `%APPDATA%\\gupiao-desktop-mvp`，不在安装包目录内
- 如果新版改了数据库结构，发版前要额外验证迁移
- 如果新版改了研究库结构，也要额外验证 `gupiao_research.db` 的重建或兼容策略
- 如果只是普通功能升级，直接替换用户手里的发布目录即可

## 常用命令

```bash
npm run lint
npm run test:api
npm run test:web
npm run research:refresh
npm run package
```

## 当前已知事项

- 当前打包目标是 Windows 桌面目录包
- 当前桌面窗口依赖 `pywebview`，Windows 目标机建议有 WebView2
- 当前没有自定义应用图标
