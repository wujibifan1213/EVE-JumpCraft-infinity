# JumpCraft

曙光版图改动了大量星系和星门数据，直接用常规数据源容易出现不可达星系的问题，所以这东西直接从 ESI 拉取可达星系来构建地图，算是一个曙光专用的旗舰跳跃路线计算器。

支持星门+旗舰跳跃混合规划路线，也支持土路模式——起点或终点没有建筑的时候会自动找最近的有建筑星系中转。

登录 EVE SSO 之后可以搜你自己能停靠的建筑，把建筑当跳板用。不过有一点要注意：搜到的建筑没法保证还活着，有可能是已经拆了的，这块得你自己确认。

## 普通用户

直接下载 dist 目录里的 exe 版本，双击运行就行。第一次启动会自动拉取星系数据，等个 10-15 分钟就好了，之后启动是秒开的。浏览器会自动打开，没开的话自己访问 `http://localhost:8000`。

## 开发者

### 环境准备

Python 3.10+，然后：

```bash
pip install -r requirements.txt
cp .env.example .env
```

`.env` 里的配置按需改，默认值已经填好了曙光服务器的地址，一般不用动。

### 启动参数

```
python main.py [选项]

选项:
  --refresh      强制从 ESI 重新拉取全部星系数据
  --host HOST    监听地址，默认 0.0.0.0
  --port PORT    监听端口，默认 8000
  --no-init      跳过数据初始化，直接启动服务
  --no-browser   不自动打开浏览器
  --login        通过 EVE SSO 登录并保存 Token
```

默认启动会检查本地缓存，有缓存就直接用，没有就自动拉取。数据有问题的时候加 `--refresh` 重新拉一遍就好。

### SSO 登录

想搜自己的建筑需要登录。在 `.env` 里把 `EVE_SSO_CLIENT_ID` 填上，然后：

```bash
python main.py --login
```

会打开浏览器让你授权，授权完 Token 会自动保存，之后启动服务就不用再登录了。Token 过期了会自动刷新，不用管。

### 项目结构

```
main.py                启动入口
config.py              配置管理
pkg_utils.py           路径工具（兼容打包后的路径）
esi/                   ESI API 客户端
  auth.py              SSO 登录和 Token 刷新
  client.py            HTTP 请求、ETag 缓存、限速
  search.py            玩家建筑搜索
  universe.py          星系/星门/空间站数据拉取
graph/                 星图图论计算
  builder.py           从数据库构建 NetworkX 图
  geometry.py           光年距离计算
  routes.py            路线规划（旗舰跳跃+野路）
  validator.py          不可达星系检测与清理
cache/
  storage.py           SQLite 缓存层
services/
  sync.py              数据拉取与重建逻辑
web/
  app.py               FastAPI 路由和 API
  static/              静态资源
  templates/            HTML 模板
data/sde/
  npc_stations.json    NPC 空间站列表
scripts/
  build_npc_stations.py  从 SDE 提取 NPC 空间站数据
```

## 许可证

MIT