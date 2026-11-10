# weibo-distributed-collector-mac

微博匿名首轮采集的 Mac 工作机版本，适用 macOS 14+、Apple Silicon（包括 M3 及更新芯片）、原生 Python 3.13 和 Microsoft Edge。

基于 [Windows 项目](https://github.com/LEeei99/weibo-distributed-collector) 的提交 993f5558385d0180acb42bd77dbf9c61dcf2aec7 适配，沿用分包、签收、SQLite 断点和回传格式，中央端继续维护唯一登记库。来源与兼容边界见 [UPSTREAM.md](UPSTREAM.md)。

## 安装

安装原生 Apple Silicon Python 3.13 和 Microsoft Edge，将仓库放在普通本地目录，在仓库目录打开终端：

```bash
bash Setup-Mac.sh
```

脚本检查系统和 Python，建立本机虚拟环境、安装锁定依赖，再让 Edge 加载内存中的本地页面。它不会访问微博或开始采集。请勿在 Rosetta 下使用 Intel Python。

## 第一次使用

从协调人私下取得同名 JSONL、manifest、assignment 三件套。替换以下占位符，使用真实签发的 Worker ID：

```bash
pkg="$HOME/WeiboPrivateInbox/PACKAGE_ID/PACKAGE_ID.jsonl"
worker_id="ASSIGNED-WORKER-ID"
work_root="$HOME/WeiboWork"

bash Run-Worker.sh prepare --package "$pkg" --worker-id "$worker_id" --worker-root "$work_root"
bash Run-Worker.sh run --package "$pkg" --worker-id "$worker_id" --worker-root "$work_root"
```

prepare 仅离线验证并导入。默认 run 每次最多 2 个普通帖子请求、1 批；另行记录的门禁诊断请求不计入普通请求上限。重复同一 run 命令验证续跑，再检查重启后的断点。

在第二个终端查看进度：

```bash
bash Monitor-Worker.sh --out "$work_root/PACKAGE_ID"
```

暂停时在采集终端按一次 Ctrl+C，等待保存和退出。停机后回传，每次使用一个不存在的新目录：

```bash
bash Pack-Return.sh --package "$pkg" --work "$work_root/PACKAGE_ID" \
  --bundle "$HOME/WeiboReturns/PACKAGE_ID-r01"
```

完整限量试跑、整包采集、防休眠与回传步骤见 [WORKER.md](WORKER.md)。

## 两台 Mac 与 Windows 协作

每台 Mac 使用不同 Worker ID、不同已签发子包和独立本地工作目录。不要复制 Windows 或另一台 Mac 的已绑定断点、签收或虚拟环境。数据放在普通本地磁盘，避开 iCloud、CloudStorage、Google Drive、OneDrive 和网络盘。停止采集后再传输回传目录。

分包、签收、数据库、媒体、日志和凭据不进入代码仓库。仓库不会领取任务或签发分包；由协调人使用现有唯一 dispatch.sqlite 签发。回传经私密渠道送中央端，用中央原始包校验后合并。

## 验证

```bash
.venv/bin/python -B -m unittest discover -s tests -v
.venv/bin/python -B -m unittest discover -s code/tests -v
```

[GitHub Actions](https://github.com/LEeei99/weibo-distributed-collector-mac/actions) 运行合成分包、文件锁、断点、回传和进程监督测试，不执行真实微博采集。托管测试通过后，每台实际 Mac 仍应验收 Edge、限量请求、Ctrl+C 保存、重启续跑、媒体下载，以及 Mac 回传到现有 Windows 中央端的合并。先在隔离汇总库试验。

这套任务不需要 GPU 模型环境。媒体量、网络和访问限制会影响速度，不能保证增加两台机器就有两倍吞吐。
