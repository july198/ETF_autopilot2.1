# ETF Auto Trader（按你这份表格的规则自动执行）

这个仓库把你上传的 Excel 工作流，做成了可以**每天自动生成信号 → 自动算出下单股数 →（可选）自动下单 → 记录 TradeLog → 发邮件通知**的一套脚本。

核心规则完全照表格实现（DailyCheck + Config）：
- **First / Second / Third / ReserveOnly** 的触发条件、冷却期、月度上限、200MA 下 First 只买一半并累积待命现金
- 待命现金在 **回到 200MA 上方** 或 **Second/Third 触发** 时**一次性全部动用**
- 每次买入金额、阈值、手续费参数、分数股规则、零钱池滚存，全部可在 `config.yaml` 修改

> 提醒：我无法直接把文件发送到你的 QQ 邮箱，但仓库已经内置“SMTP 发邮件”功能，你填好 QQ 邮箱的 SMTP 授权码后，脚本会自动把每日结果发到你的邮箱。

---

## 数据自动更新（价格 + 汇率）

- 每次运行都会从网络拉取 **5 只 ETF 的最新收盘价**（用于算股数）与 **RSP 历史数据**（用于 MA200、月内最高收盘等）。
- USD/CNY 走 `fx_mode: auto` 时会自动拉取当日汇率；拉不到时用 `fx_fallback_usd_cny` 兜底。
- 运行完成会在 `data/` 输出：
  - `orders_YYYY-MM-DD.json`（当天订单明细 + 汇率 + 当天收盘价）
  - `summary_YYYY-MM-DD.json`（当天摘要）
  - `trade_log.csv`（每天追加一行，含汇率与价格字段）


## QuickStart（小白照做）

### A. 一次性等权建仓（用 50 万 RMB 买 5 只 ETF）
1) 本地电脑安装 Python 3.11  
2) 在项目目录执行：

```bash
pip install -r requirements.txt
cp .env.example .env   # 然后把 .env 里 SMTP_PASS 填成你的 SMTP 授权码（建议只放本地，不要提交到 GitHub）
python scripts/init_equal_weight.py
```

运行后会生成 `data/orders_init_YYYY-MM-DD.json`。按这个清单去下单即可。  
成交后，把实际成交股数写进 `data/holdings.csv`。

### B. 每日自动化（信号 → 下单清单 → 记录日志 → 邮件）
```bash
python scripts/run_daily.py
```

会生成：
- `data/orders_YYYY-MM-DD.json`（当日下单清单）
- `data/trade_log.csv`（自动追加一行日志；包含待命现金与零钱池字段）


---

## 1）你每天在 Excel 里做的事，这里怎么自动化

Excel 的每日 4 步，对应到这里：

1. **获取数据**（自动）
   - RSP 当日收盘
   - 前一日收盘
   - 200MA（用历史数据计算）
   - 本月截至今日的最高收盘

2. **生成信号与推荐买入总额（CNY）**（自动）
   - 完全按 Excel `DailyCheck` 的条件判断

3. **生成“最终下单清单”**（自动）
   - 按组合权重偏离，优先把钱分到权重更低的 1–2 只 ETF
   - 支持整股或分数股
   - 计入手续费并做“二次分配”吃掉取整后的零头
   - 生成订单 JSON + 人类可读的摘要

4. **写入 TradeLog**（自动）
   - 把当日信号、金额、待命现金变化、手续费、零钱池剩余等写进 `data/trade_log.csv`

---

## 2）快速开始（小白版）

### A. 在你电脑上跑（最省事）

1. 安装 Python 3.11（装好后终端能运行 `python --version`）
2. 下载本仓库到本地并进入目录
3. 安装依赖：

```bash
pip install -r requirements.txt
```

4. 复制配置文件并按你的情况改：

```bash
（config.yaml 已经在仓库里准备好，你也可以按需改）
```

5. 填写你的持仓（手动维护即可）：

- `data/holdings.csv`：只需要两列：`ticker,shares`
- 例子已经放好，你把 shares 改成你真实持仓股数

6. 运行一次：

```bash
python scripts/run_daily.py
```

运行后你会得到：
- `data/orders_YYYY-MM-DD.json`（当天订单）
- `data/trade_log.csv`（自动追加一行）
- 终端输出：信号、推荐买入总额、每只 ETF 买多少股、预计手续费、零钱池余额

### B. 想要“全自动每天跑”

两种方式：

- **方式 1：你的电脑定时任务**  
  macOS/Linux 用 cron，Windows 用 任务计划程序。每天在美股收盘后跑 `python scripts/run_daily.py`。

- **方式 2：GitHub Actions 定时跑**（更接近“全自动 GitHub”）  
  你把仓库推到 GitHub，然后按 README 第 5 节配置 Secrets。

---

## 3）交易信号规则（逐条对应 Excel）

这部分对应 Excel `DailyCheck`（我按公式翻译成文字）：

### 3.1 基础计算

- 是否交易日：按 NYSE 交易日历判断  
- 日跌幅：`rsp_close / rsp_prev_close - 1`
- 月内回撤：`rsp_close / month_high_close - 1`
- 是否低于 200MA：`rsp_close < ma200`
- 待命现金余额（CNY）：`sum(reserve_add) - sum(reserve_use)`（来自 TradeLog）
- 冷却期（交易日）：用 NYSE 交易日序号相减，要求 `>= cooldown_days`

### 3.2 First

触发：
- 当月还没做过任何一笔（当月累计笔数 = 0）
- 冷却期满足
- 且满足其一：
  - 日跌幅 <= `first_daily_drop_threshold`
  - 今天是本月第三个星期五（交易日）

金额：
- 若低于 200MA：只买 `first_buy_ratio`（默认 50%），其余进入待命现金
- 若高于 200MA：买满一次基准金额

### 3.3 Second

触发：
- 当月已做过 First
- 当月还没做过 Second
- 冷却期满足
- 当月累计笔数 < 月度上限
- 月内回撤 <= `second_drawdown_threshold`（默认 -5%）

金额：
- 基准金额（默认 5000 CNY）
- 若存在待命现金，并且（收盘 >= 200MA 或 Second/Third 触发）：**待命现金一次性全部叠加到当笔买入**（和 Excel 一样）

### 3.4 Third

触发：
- 当月已做过 Second
- 当月还没做过 Third
- 冷却期满足
- 当月累计笔数 < 月度上限
- 月内回撤 <= `third_drawdown_threshold`（默认 -10%）

金额同 Second。

### 3.5 ReserveOnly（只动用待命现金）

触发：
- 交易日
- 待命现金余额 > 0
- 冷却期满足
- 且满足其一：收盘 >= 200MA（或 Second/Third 当天）
- 同时当日没有触发 First/Second/Third（因此叫 ReserveOnly）

金额：
- 只用待命现金余额（一次性用完）

---

## 4）下单分配规则（Portfolio 逻辑）

对应 Excel `Portfolio`：

- 目标权重默认 20% 等权
- 计算每只 ETF 当前权重
- 对每只 ETF 计算 `UnderScore`：
  - 若权重 >= 上限护栏（默认 25%）：UnderScore = 0
  - 否则 UnderScore = max(0, 目标权重 - 当前权重)

分配：
- 若所有 UnderScore 加总为 0：本次买入金额平均分给 5 只
- 否则：
  - 找 UnderScore 最大的 1–2 只（Top1 / Top2）
  - 若 Top2 = 0：把本次全部金额给 Top1
  - 否则：按 UnderScore 比例在 Top1 / Top2 间分配

整股 / 分数股：
- `allow_fractional_shares: YES`：按 `fractional_step` 向下取整
- `NO`：向下取整到整股

手续费：
- 按 `config.yaml` 的费率/最低收费计算（买入、卖出分别计算）

二次分配（吃掉零头）：
- 把取整后剩余的 USD，优先补给 Top1，再补 Top2  
- 额外股数会合并到同一个 ticker 的订单里，因此不会产生额外“每笔固定费”

---

## 5）GitHub Actions 全自动（可选）

### 5.1 你需要准备的 Secrets

在 GitHub 仓库页面：Settings → Secrets and variables → Actions → New repository secret

建议至少准备：
- `CONFIG_YAML`：把你的 `config.yaml` 全文粘进去
- `HOLDINGS_CSV`：把你的 `data/holdings.csv` 全文粘进去

如果你想让它自动发到 QQ 邮箱，还需要：
- `SMTP_USER`：你的 QQ 邮箱
- `SMTP_PASS`：QQ 邮箱的 SMTP 授权码（在 QQ 邮箱设置里生成）
- `SMTP_TO`：你要收信的邮箱（一般填自己）
- `SMTP_HOST`：smtp.qq.com
- `SMTP_PORT`：465

如果你想让它自动下单（强烈建议你先跑几天 paper 模式），再加：
- Alpaca：`ALPACA_API_KEY` / `ALPACA_API_SECRET`
- IBKR：建议先用本地网关跑，Actions 不太适合 IBKR

### 5.2 启用工作流

仓库里已经带了 `.github/workflows/daily_trade.yml`  
你只要把仓库推上 GitHub，打开 Actions 允许运行即可。

默认定时：**北京时间 08:00（周二到周六）**自动跑一次（对应上一交易日美股收盘后的数据）。
如果你想改时间：编辑 `.github/workflows/daily_trade.yml` 里的 `cron` 表达式即可（GitHub 以 UTC 解释 cron）。

---

## 6）安全建议（小白必看）

- 第一次请把 `broker.mode` 设为 `paper`，连续跑几天确认信号与金额都符合预期
- 确认无误后再切到真实下单
- 任何券商 API 失败时，脚本会保留订单文件并发邮件提醒，你可以手动下单

---

## 目录结构

- `src/etf_auto_trader/`：核心逻辑（信号、分配、手续费、下单、记录）
- `scripts/run_daily.py`：日常增持入口
- `scripts/run_aug_rebalance.py`：8 月再平衡入口（一次性清单）
- `data/`：持仓、订单、TradeLog（CSV）
