# 部署到 GitHub · 完整指南

> 把本地这套 PyFlink 教程推到 GitHub，让别人 clone 下来就能跑。
> 包含：仓库初始化、敏感信息处理、JAR 管理策略、自动化脚本、常见坑。

---

## 目录

- [一、推之前必须想清楚的 3 件事](#一推之前必须想清楚的-3-件事)
- [二、仓库结构设计](#二仓库结构设计)
- [三、敏感信息处理（最重要）](#三敏感信息处理最重要)
- [四、JAR 包管理策略](#四jar-包管理策略)
- [五、一步一步推到 GitHub](#五一步一步推到-github)
- [六、验证别人能跑起来](#六验证别人能跑起来)
- [七、常见坑](#七常见坑)
- [八、日常维护命令](#八日常维护命令)

---

## 一、推之前必须想清楚的 3 件事

推到 GitHub 前，先回答这三个问题。**第 1 个没处理好，等于把密码公开发布。**

### 1. 有没有硬编码的密码？ 🔴

这套工程里散落着不少**明文密码**，直接推上去就是安全事故：

| 位置 | 内容 | 风险 |
|---|---|---|
| `docker/docker-compose.yml` | MySQL `root123456`、pg `shop123` | 数据库口令泄露 |
| `examples/*.py` | 连接串里的 `shop123` / `cdc123` | 同上 |
| `scripts/*.sh` | 各种连接参数 | 同上 |

> **好消息**：这些都是**本地开发用的弱口令**，而且是教程演示数据。
> 但即便这样，也不该直接推到公开仓库 —— 有人会拿它去撞你的其他服务。

**处理方式见[第三章](#三敏感信息处理最重要)。**

### 2. 大文件怎么办？ 🟡

`jars/` 目录 28MB，其中 CDC 胖包单个就 **20MB**。

GitHub 单文件限制 100MB（硬限制），仓库建议 < 1GB（软限制）。
28MB 其实推得上去，但：

- 每次改代码都会带上这些二进制，仓库迅速膨胀
- 别人 clone 要等很久
- JAR 是**可重新下载**的产物，不适合版本控制

**处理方式见[第四章](#四jar-包管理策略)。**

### 3. 别人 clone 下来能跑吗？ 🟡

你现在能跑，是因为本机装好了 pyflink、拉好了镜像、配好了 pg。
别人拿到代码后需要：

- 一份「前置条件清单」（Python 版本、Docker、内存要求）
- 一条能自动检查环境的命令
- 明确知道哪些步骤会耗时很久（首次构建镜像 ~10 分钟）

**处理方式见[第二章](#二仓库结构设计) + [第六章](#六验证别人能跑起来)。**

---

## 二、仓库结构设计

### 推荐的目录结构

```
pyflink-tutorial/
├── README.md                    # 门面：5 分钟能看懂这仓库干嘛的
├── LICENSE                      # 建议加（MIT / Apache-2.0）
├── .gitignore                   # ⚠️ 推之前必须写好
├── .env.example                 # ⚠️ 配置模板（不含真实密码）
│
├── docs/                        # 教程文档
│   ├── pyflink-tutorial.md      #   主教程（+ 同名 .html）
│   ├── docker-deployment-tutorial.md
│   ├── pygresql-integration.md
│   ├── github-deployment.md     #   本文档
│   └── *.html                   #   GitHub Pages 可直接托管
│
├── examples/                    # 示例代码（核心资产）
│   ├── 01_hello_wordcount.py
│   ├── 02_table_api.py
│   ├── ...
│   └── 10_postgres_cdc.py
│
├── docker/
│   ├── docker-compose.yml
│   └── pyflink-image/
│       └── Dockerfile           #   自定义镜像（官方镜像无 Python）
│
├── scripts/                     # 自动化脚本
│   ├── docker_up.sh             #   一键起集群
│   ├── docker_run.sh            #   跑示例
│   ├── fetch_jars.sh            #   ⭐ 补下载 JAR
│   └── ...
│
├── jars/                        # ⚠️ 不进 Git（.gitignore 排除）
└── jars-optional/               # ⚠️ 同上
```

### 为什么这么分层

- **`examples/` 是核心资产** —— 别人主要来看代码，放最外层一眼可见
- **`docs/*.html` 可白嫖 GitHub Pages** —— 开启 Pages 后免费得到一个在线教程站
- **`jars/` 排除在外** —— 用 `fetch_jars.sh` 补，见第四章

---

## 三、敏感信息处理（最重要）

### 方案 A：环境变量 + `.env.example`（推荐）

**第 1 步**：创建 `.env.example`（这个**要**提交，作为模板）

```bash
# .env.example —— 配置模板，复制成 .env 后填真实值
# ⚠️ .env 已在 .gitignore 中排除，不会进 Git

# ---- MySQL ----
MYSQL_ROOT_PASSWORD=change_me
MYSQL_DATABASE=shop
MYSQL_PORT=13306

# ---- PostgreSQL ----
PG_HOST=host.docker.internal
PG_PORT=5432
PG_DB=shop
PG_USER=shop
PG_PASSWORD=change_me

# ---- PostgreSQL CDC ----
CDC_USER=flink_cdc
CDC_PASSWORD=change_me
CDC_SLOT=flink_users_slot
CDC_PUBLICATION=flink_cdc_pub

# ---- Kafka ----
KAFKA_BOOTSTRAP=kafka:9092
```

**第 2 步**：创建 `.env`（**不要**提交）

```bash
cp .env.example .env
# 然后编辑 .env，把 change_me 换成真实值
```

**第 3 步**：`.gitignore` 里排除

```gitignore
.env
.env.local
*.env
!.env.example      # ← 注意这个感叹号，表示"但这个要提交"
```

**第 4 步**：代码里改读环境变量

```python
import os

PG_PASSWORD = os.environ.get("PG_PASSWORD")
if not PG_PASSWORD:
    raise RuntimeError(
        "未设置 PG_PASSWORD。请先 cp .env.example .env 并填入真实密码。"
    )
```

> 💡 示例 10 已经大量使用 `os.environ.get()` 模式了，
> 只需要把剩下的硬编码值替换掉即可。

### 方案 B：`docker-compose.yml` 用变量占位

```yaml
services:
  mysql:
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD:?必须设置 MYSQL_ROOT_PASSWORD}
      MYSQL_DATABASE: ${MYSQL_DATABASE:-shop}
```

`${VAR:?错误信息}` = 变量没设置就**直接启动失败并提示**，
比默默用默认值安全得多（避免"以为改了其实没改"）。

### ⚠️ 已经推错了怎么办？

如果密码已经推到 GitHub，**改密码比删 commit 有用**：

1. **立即轮换密码** —— 这是唯一真正止损的动作
2. 清理 Git 历史：

```bash
# 用 git-filter-repo（比 filter-branch 快很多）
pip install git-filter-repo
git filter-repo --path .env --invert-paths

# 强制推送覆盖远端
git push --force --all
```

3. 联系 GitHub Support 清理缓存（`--force` 后旧 commit 仍可能被访问）

> **顺序很重要**：先改密码，再清历史。
> 只清历史不改密码 = 密码照样有效，只是藏起来了。

---

## 四、JAR 包管理策略

### 三个选择

| 方案 | 优点 | 缺点 | 建议 |
|---|---|---|---|
| 直接提交 JAR | 别人 clone 就能跑 | 仓库臃肿（28MB+），每次改动都带二进制 | ❌ |
| Git LFS | 仓库轻量 | 别人必须装 LFS，否则 clone 的是指针文件 | 🟡 |
| **下载脚本** | 仓库干净，来源可追溯 | 多一步 `fetch_jars.sh` | ✅ 推荐 |

### 推荐做法：下载脚本

**第 1 步**：`.gitignore` 排除

```gitignore
jars/*.jar
jars-optional/*.jar
```

**第 2 步**：写 `scripts/fetch_jars.sh`（本仓库已提供，见该文件）

核心逻辑：

```bash
MAVEN="https://repo1.maven.org/maven2"

fetch() {
    local rel="$1" dest="$2"
    [ -f "$dest" ] && { echo "  ✓ 已存在"; return 0; }
    curl -fsSL --retry 3 "$MAVEN/$rel" -o "$dest"
}
```

**第 3 步**：README 里写清楚

```bash
# 克隆后第一步
bash scripts/fetch_jars.sh
```

**第 4 步**（可选）：Docker 构建时自动拉

如果走 Docker 路线，其实**不需要本地 JAR** ——
`Dockerfile` 里已经用 `fetch` 从 Maven 下载了：

```dockerfile
RUN fetch "$MAVEN_BASE/org/apache/flink/flink-sql-connector-postgres-cdc/3.6.0-1.20/..." \
          "flink-sql-connector-postgres-cdc-3.6.0-1.20.jar"
```

本地 `jars/` 只有走「本地安装」路线时才需要。

### ⚠️ 胖包 vs 瘦包的教训

下 JAR 时容易踩的坑，**已在 `fetch_jars.sh` 注释里标明**：

```
flink-connector-postgres-cdc-3.6.0-1.20.jar      174K  ← 瘦包 ❌ 会报 NoClassDefFoundError
flink-sql-connector-postgres-cdc-3.6.0-1.20.jar   20M  ← 胖包 ✅ SQL 场景用这个
```

规律：**SQL 连接器一律用 `flink-sql-` 前缀的胖包**。

---

## 五、一步一步推到 GitHub

### 前置：确认 gh 已登录

```bash
gh auth status
# ✓ Logged in to github.com account <你的账号>
```

没登录的话：

```bash
gh auth login
# 选 GitHub.com → HTTPS → Login with a web browser
```

### 方式一：gh 一键创建并推送（最简单）

```bash
cd /path/to/pyflink-tutorial

# 初始化
git init
git add .
git commit -m "feat: PyFlink 教程初始版本（10 个示例 + Docker 集群 + 中文文档）"

# 创建远程仓库并推送（一条命令搞定）
gh repo create pyflink-tutorial \
    --public \
    --source=. \
    --description "PyFlink 从入门到生产：10 个可运行示例 + Docker 集群 + 中文教程" \
    --push
```

`--public` 改 `--private` 就是私有仓库。

### 方式二：先建仓库再关联（手动可控）

```bash
# 1. 建空仓库（不初始化 README，避免冲突）
gh repo create pyflink-tutorial --public --description "..."

# 2. 本地初始化
git init
git branch -M main
git add .
git commit -m "feat: 初始版本"

# 3. 关联并推送
git remote add origin https://github.com/<你的用户名>/pyflink-tutorial.git
git push -u origin main
```

### 推送前自检清单

```bash
# ① 确认敏感文件没被跟踪
git ls-files | grep -E "\.env$|\.env\.|password|secret"
# 应该只看到 .env.example，看不到 .env

# ② 确认大文件没被跟踪
git ls-files | grep "\.jar$"
# 应该输出为空

# ③ 看看将要提交的文件总览
git status --short
git diff --cached --stat | tail -5
```

> ⚠️ **务必先 `git ls-files` 检查，再 push**。
> 推错了虽然能清历史，但浪费时间和精力。

---

## 六、验证别人能跑起来

自己觉得"应该能跑"不算数。**换个目录真跑一遍**：

```bash
# 模拟新人：clone 到全新目录
cd /tmp
git clone https://github.com/<你的用户名>/pyflink-tutorial.git test-clone
cd test-clone

# 按 README 的步骤走
bash scripts/fetch_jars.sh
bash scripts/setup.sh --check      # 环境检查
bash scripts/docker_up.sh          # 起集群
bash scripts/docker_run.sh 01      # 跑第一个示例
```

**重点验证**：

- [ ] `fetch_jars.sh` 能把 JAR 都拉下来
- [ ] 缺环境变量时脚本给出**清晰报错**，而不是莫名崩溃
- [ ] README 里的命令**逐条可执行**（不能有"你应该知道要改这里"）
- [ ] 首次构建的耗时提示到位（别人不会以为卡死了）

### README 必须包含的要素

```markdown
## 前置要求
- Docker Desktop（≥ 4.0）
- 内存 ≥ 8GB（Flink 集群 + MySQL + pg 同时跑）
- 磁盘 ≥ 5GB（镜像 + 依赖）
- 首次构建需 ~10 分钟（装 Python 3.11 + PyFlink 约 400MB 依赖）

## 快速开始
（3 条命令以内，从 clone 到看到结果）

## 常见问题
（把你自己踩过的坑写进去）
```

---

## 七、常见坑

### 1. 推完发现漏了文件

```bash
# .env 已经被跟踪了，要取消跟踪（但保留本地文件）
git rm --cached .env
echo ".env" >> .gitignore
git commit -m "chore: 停止跟踪 .env"
git push
```

> `git rm --cached` **只删索引不删本地文件**，
> 别用 `git rm`（那会把本地文件也删了）。

### 2. 想改仓库名 / 描述

```bash
gh repo edit --description "新描述"
gh repo rename 新名字
```

### 3. 行尾符问题（Windows → Linux）

Windows 上 `core.autocrlf` 可能是 `true`，推上去的 `.sh` 在 Linux 上会带 `\r` 导致
`bash: $'\r': command not found`。

```bash
# 推荐：仓库里统一用 LF
git config --global core.autocrlf input

# 并在 .gitattributes 里明确
cat >> .gitattributes <<'EOF'
* text=auto eol=lf
*.sh text eol=lf
*.bat text eol=crlf
*.jar binary
EOF
```

### 4. 中文文件名 / 路径乱码

```bash
git config --global core.quotepath false
```

这样 `git status` 里中文文件名会正常显示，而不是 `\346\226\207...`。

### 5. 忘了加 LICENSE

公开仓库没 License，别人**法律上不能使用**你的代码。

```bash
gh api -X PUT /repos/:owner/:repo/license   # 不直观，建议手动加

# 或者直接从 GitHub 网页加：Add file → Create new file → 输入 "LICENSE"
# 会出现 "Choose a license template" 按钮
```

推荐 **MIT**（最宽松）或 **Apache-2.0**（含专利授权，适合企业）。

---

## 八、日常维护命令

```bash
# 查看状态
git status
git log --oneline -10

# 提交
git add -A
git commit -m "docs: 补充 CDC 章节"
git push

# 看远程仓库
gh repo view --web

# 建 Issue / PR
gh issue create --title "示例 10 在 pg15 上报表不存在" --body "..."
gh pr create --fill

# 打标签（发 release）
git tag -a v1.0.0 -m "首个稳定版：10 个示例全部验证通过"
git push --tags
gh release create v1.0.0 --generate-notes
```

### 建议先做的两件事

1. **开启 GitHub Pages 托管 docs/**
   ```
   Settings → Pages → Source: Deploy from a branch
   Branch: main  /  Folder: /docs
   ```
   之后 `https://<用户名>.github.io/pyflink-tutorial/pyflink-tutorial.html`
   就是一个在线教程站，零成本。

2. **加 CI 做基础检查**（`.github/workflows/check.yml`）
   ```yaml
   name: Check
   on: [push, pull_request]
   jobs:
     lint:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.11'
         - run: pip install flake8
         - run: flake8 examples/ --max-line-length=120 --select=E9,F63,F7,F82
   ```
   只查"语法错误"级别的（`E9,F63...`），不搞风格洁癖 ——
   否则每次提交都被格式问题卡住，很烦。

---

## 附：本仓库的实际推送记录

```bash
# 2026-09-20 首次推送
cd /c/Users/wzm/WorkBuddy/2026-09-20-09-28-01/pyflink-tutorial

git init
git branch -M main
git add .
git commit -m "feat: PyFlink 教程（10 示例 + Docker 集群 + pg CDC 集成）"

gh repo create pyflink-tutorial --public --source=. --push \
    --description "PyFlink 从入门到生产：可运行的示例 + Docker 集群 + 中文教程"
```

> ⚠️ **推送前务必确认**：
> - `jars/*.jar` 未被跟踪（28MB，且可由 `fetch_jars.sh` 重建）
> - 无 `.env` / 明文密码
> - `.sh` 文件是 LF 行尾
