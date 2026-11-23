# MoviePilot-Plugins

本仓库用于存放 MoviePilot 插件。目前包含以下插件：

| 插件 | 说明 |
| --- | --- |
| [CD2Storage](plugins.v2/cd2storage/) | 将 CloudDrive2 接入 MoviePilot 存储模块，支持直写上传、远程上传和可选的路径额度分流。 |

## CD2Storage

### 功能

- 使用 CloudDrive2 API Token 认证，不使用账号密码登录。
- 支持**直写上传**和**远程上传**两种上传模式。
- 将 CloudDrive2 注册为 MoviePilot 存储，可由 MoviePilot 的存储功能调用。
- 可选配置路径分流：根据传入路径前缀，将文件放到一个或多个分支目录，并为每个分支设置滚动 24 小时上传额度。
- 插件数据页展示连接配置摘要、分流目录额度和近 24 小时统计。

### 配置

在 MoviePilot 的插件设置中启用 CD2Storage，并填写：

| 配置项 | 说明 |
| --- | --- |
| CloudDrive 地址 | CloudDrive2 服务地址或主机名，不要填写 `http://` 或 `https://`。 |
| 端口 | CloudDrive2 API 端口，默认 `19798`。 |
| API 令牌 | CloudDrive2 的 API Token。令牌不会显示在插件数据页。 |
| 上传模式 | 选择直写上传或远程上传。 |
| 上传路径分流规则 | 可选。留空时不进行路径分流；填写格式见下方示例。 |

### 路径分流规则示例

将以下 JSON 填入“上传路径分流规则”配置项：

```json
[
  {
    "prefix": "/abc/",
    "branches": [
      {
        "path": "/abc1/",
        "capacity_gb": 20
      },
      {
        "path": "/abc2/",
        "capacity_gb": 30
      }
    ]
  }
]
```

以上规则表示：

1. 外部传入路径以 `/abc/` 开头时，按 `branches` 的顺序尝试分支目录。
2. 分支目录 `/abc1/` 在滚动 24 小时内的成功上传量未超过 `20 GB` 时，优先使用该目录；额度不足时再尝试 `/abc2/` 的 `30 GB` 额度。
3. 目标路径中 `/abc/` 后面的相对路径会保留。例如 `/abc/电影/影片.mkv` 被分流到 `/abc1/` 时，目标路径为 `/abc1/电影/影片.mkv`。
4. 所有分支额度都不足时，回退到外部传入的原始路径；没有匹配规则时也使用原始路径。
5. 多条规则按配置顺序匹配，命中第一条后不再检查后续规则。路径使用以 `/` 分隔的 POSIX 格式，不要使用 `\`、`.` 或 `..` 路径段。
6. 容量单位为十进制 GB（`1 GB = 1,000,000,000` 字节）。额度统计只记录本插件成功上传到已配置分支目录的文件，按每次上传时间滚动计算 24 小时；失败上传不计入。回退到原始路径的上传不占用分支目录额度。

可以继续在 JSON 数组中增加规则，例如：

```json
[
  {
    "prefix": "/abc/",
    "branches": [
      { "path": "/abc1/", "capacity_gb": 20 },
      { "path": "/abc2/", "capacity_gb": 30 }
    ]
  },
  {
    "prefix": "/movies/",
    "branches": [
      { "path": "/movie-a/", "capacity_gb": 50 },
      { "path": "/movie-b/", "capacity_gb": 50 }
    ]
  }
]
```

## 更多插件

后续插件会继续在此 README 中增加独立说明，并在上方插件列表中登记。
