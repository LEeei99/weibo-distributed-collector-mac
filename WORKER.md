# Mac 工作机操作说明

适用 macOS 14+、原生 Apple Silicon Python 3.13 和 Microsoft Edge。每台机器使用不同的已签发子包与 Worker ID，数据库和媒体放在普通本地磁盘。

## 安装与身份

在仓库目录运行 bash Setup-Mac.sh；也可将 Python 3.13 的完整路径作为其唯一参数。不要复制其他机器的虚拟环境。脚本检查原生 arm64、安装依赖，并用 Edge 加载本地内存页面。

协调人私下提供 PACKAGE_ID.jsonl、PACKAGE_ID.manifest.json、PACKAGE_ID.assignment.json。签收绑定 snapshot_id、package_id、SHA-256、worker_id 和 assignment_id。不要手改字段或复用另一台机器的签收。

以下 PACKAGE_ID 和 ASSIGNED-WORKER-ID 都是占位符。替换后设置：

```bash
pkg="$HOME/WeiboPrivateInbox/PACKAGE_ID/PACKAGE_ID.jsonl"
worker_id="ASSIGNED-WORKER-ID"
work_root="$HOME/WeiboWork"
```

脚本按自身位置寻找代码与 Python；参数中的相对路径按当前终端目录解释。带空格的路径保留引号。推荐 ~/WeiboWork；避开 Desktop/Documents 的 iCloud 同步、~/Library/CloudStorage、Mobile Documents、Google Drive、OneDrive、Dropbox 和网络盘。路径检查无法检测所有自定义同步规则。

Mac 本机身份使用硬件 UUID 的 SHA-256，原始 UUID 不写入工作目录或日志。改名不改变身份；迁移到另一台机器需由协调人安排，不能复制断点后手改身份字段。

## 离线验证与两轮试跑

```bash
bash Run-Worker.sh prepare --package "$pkg" --worker-id "$worker_id" --worker-root "$work_root"
bash Run-Worker.sh run --package "$pkg" --worker-id "$worker_id" --worker-root "$work_root"
```

prepare 校验分包哈希、数量、重复 ID/序号、链接、签收和本机身份，导入独立 state.sqlite，不发采集请求，也不读取原始 Excel。

默认 run 相当于 --max-posts 2 --max-batches 1 --request-limit 2。完成后再运行同一命令，核对累计请求增加且旧帖不重复请求；随后重启机器，用原包、ID 和工作目录继续验证。不要清空或替换断点。

--max-posts 是每批预算；--request-limit 是这次调用内新增普通帖子请求的上限。独立门禁诊断请求另记，不包含在普通请求上限中。没有自动失败重试。

如协调人要求扩大到 100 条：

```bash
bash Run-Worker.sh run --package "$pkg" --worker-id "$worker_id" --worker-root "$work_root" \
  --max-posts 100 --request-limit 100
```

确认媒体和回传可用后，才运行完整子包：

```bash
bash Run-Worker.sh run --package "$pkg" --worker-id "$worker_id" --worker-root "$work_root" --until-finished
```

显式 --until-finished 取消包装脚本的 2 条默认限额，使用 Python 工具正常批次默认值。任务完成或触发保护条件时停止，不要使用无限重启循环。

## 监视与长时间运行

第二个终端运行：

```bash
bash Monitor-Worker.sh --out "$work_root/PACKAGE_ID" --interval 5
```

加 --once 只显示一次。监视只读 worker_progress.json，心跳默认每 15 秒更新，活动心跳超过 90 秒显示 Unknown；陈旧心跳不能证明进程已停止。首版 Mac 使用此心跳监视，code/weibo_monitor.py 的 Windows PID 查询在 Mac 上不受支持。

暂停时在采集终端按一次 Ctrl+C，等程序保存并退出。不要关掉采集终端、删除锁文件或复制正在写入的 SQLite。锁文件保留是正常现象，操作系统锁随进程退出释放。

长期运行时连接电源并保持网络。如需临时防闲置休眠：

```bash
caffeinate -i bash Run-Worker.sh run --package "$pkg" --worker-id "$worker_id" \
  --worker-root "$work_root" --until-finished
```

该命令仅在运行期间防闲置休眠，不修改系统设置，也不保证合盖后运行。默认每帖间隔 5 秒，单媒体上限 100 MB、每批媒体预算 1024 MB、空闲空间停止阈值 10 GB；这些不代表整包所需磁盘空间。

媒体默认下载。仅在明确要做元数据采集时添加 --metadata-only；普通续跑不会自动补齐此前跳过的媒体。验证码、限流、连续网络故障和存储不足等停止原因须检查后处理，不能通过重启循环绕过。

## 回传

停止采集后执行，每次使用新目录：

```bash
bash Pack-Return.sh --package "$pkg" --work "$work_root/PACKAGE_ID" \
  --bundle "$HOME/WeiboReturns/PACKAGE_ID-r01"
```

打包必须取得工作机锁和采集锁；运行中被拒绝时先正常暂停，不能删除锁。完整回传 return.json、posts.jsonl 和媒体，通过私密渠道交给协调人。后续用 r02 等新目录。

中央端使用原始分包和唯一 dispatch.sqlite 核对归属、ordinal/URL、请求历史、媒体大小与 SHA-256。回传成功不等于正式合并。同份回传重复导入应去重；先在隔离汇总库验证 Mac → 现有 Windows 中央端往返。

保留原工作目录的 host_identity.json、断点、媒体、日志和运行记录。原始 Excel、冻结库和来源映射仍由中央端保管。分包、签收、收据、数据库、媒体、心跳、日志、凭据及虚拟环境均不进代码仓库。
