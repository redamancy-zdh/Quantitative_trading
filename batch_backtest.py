import pandas as pd
import numpy as np
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import sys

# 动态获取上级目录并加入系统路径，以便跨文件夹导入策略
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from MACD_strategy import apply_strategy


def backtest_worker(stock_data_tuple):
    """
    多进程 Worker：负责单只股票的策略计算与资金账单统计
    """
    code, df = stock_data_tuple
    initial_cash = 100000.0

    # 1. 预清洗
    df = df[(df['最高'] > 0) & (df['最低'] > 0) & (df['收盘'] > 0)].copy()
    if len(df) < 35:
        return None

    stock_name = df['股票名称'].iloc[0] if '股票名称' in df.columns else code

    # 2. 调用核心策略获取信号与撮合结果
    df = df.sort_values('日期').reset_index(drop=True)
    try:
        df_res, trades, strat_stats = apply_strategy(df, initial_cash)
    except Exception:
        return None

    if not trades:
        return None

    # 3. 提取账单流水与盈亏统计
    capital = initial_cash
    shares = 0
    trade_pairs = 0
    wins = 0
    total_fees = 0.0
    records = []

    for t in trades:
        is_buy = t['type'].upper() == 'BUY'
        price = float(t['price'])

        t_shares = t['shares']
        t_fees = t['cost']
        t_cash_left = t['cash_left']
        total_fees += t_fees

        record = {
            'time': pd.to_datetime(t['date']),
            '可用现金': t_cash_left,
            '持有股数': 0,
            '交易金额': t_shares * price,
            '当笔费用': t_fees
        }

        if is_buy:
            capital = t_cash_left
            shares += t_shares
            record['持有股数'] = shares
            records.append(record)
        else:
            revenue = t_shares * price
            last_buy_cost = records[-1]['交易金额'] if records else 0
            last_buy_fee = records[-1]['当笔费用'] if records else 0

            round_trip_fee = last_buy_fee + t_fees
            profit = revenue - last_buy_cost - round_trip_fee

            capital = t_cash_left
            shares -= t_shares

            record['持有股数'] = shares
            records.append(record)

            trade_pairs += 1
            if profit > 0:
                wins += 1

    final_value = capital
    if shares > 0:
        last_close = float(df_res['收盘'].iloc[-1])
        final_value = capital + (shares * last_close)

    total_return = (final_value - initial_cash) / initial_cash
    win_rate = (wins / trade_pairs) if trade_pairs > 0 else 0.0

    # 4. 向量化极速计算最大回撤与夏普比率
    max_drawdown = 0.0
    sharpe_ratio = 0.0
    if records:
        res_df = pd.DataFrame(records)
        equity_df = pd.DataFrame({'time': df_res['日期'], 'close': df_res['收盘']})

        equity_df = pd.merge_asof(equity_df, res_df[['time', '可用现金', '持有股数']], on='time', direction='backward')
        equity_df['可用现金'] = equity_df['可用现金'].fillna(initial_cash)
        equity_df['持有股数'] = equity_df['持有股数'].fillna(0)

        equity_df['total_asset'] = equity_df['可用现金'] + equity_df['持有股数'] * equity_df['close']

        cummax = equity_df['total_asset'].cummax()
        drawdown = (cummax - equity_df['total_asset']) / cummax
        max_drawdown = drawdown.max()

        daily_return = equity_df['total_asset'].pct_change().fillna(0)
        std_daily = daily_return.std()
        if std_daily > 0:
            sharpe_ratio = (daily_return.mean() - 0.03 / 252) / std_daily * np.sqrt(252)

    return {
        '股票代码': code,
        '股票名称': stock_name,
        '最终资产': round(final_value, 2),
        '总收益率': total_return,
        '胜率': win_rate,
        '交易次数(对)': trade_pairs,
        '最大回撤': max_drawdown,
        '夏普比率': round(sharpe_ratio, 2),
        '总手续费': round(total_fees, 2),
        # ⚠️ 修复点：加入文字，防止 Excel 强行将其转为日期
        '挂单重试(买/卖)': f"买{strat_stats['failed_buys']} | 卖{strat_stats['failed_sells']}"
    }


def main():
    file_path = os.path.join('..', 'A_share_all_history(hfq).parquet')
    if not os.path.exists(file_path):
        print(f"❌ 找不到全局数据文件: {file_path}")
        return

    print("🚀 正在将全局数据加载到内存 (这可能需要一小会儿)...")
    df_all = pd.read_parquet(file_path)
    df_all.columns = [c.strip() for c in df_all.columns]
    df_all['日期'] = pd.to_datetime(df_all['日期'])

    print("📦 正在对股票数据进行分组切片...")
    grouped = list(df_all.groupby('股票代码'))
    total_stocks = len(grouped)

    results = []
    cpus = max(1, multiprocessing.cpu_count())
    print(f"⚡ 并行引擎启动：分配 {cpus} 个核心处理 {total_stocks} 只股票...")

    with ProcessPoolExecutor(max_workers=cpus) as executor:
        futures = [executor.submit(backtest_worker, group) for group in grouped]

        for future in tqdm(as_completed(futures), total=total_stocks, desc="回测进度", unit="只"):
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                continue

    res_df = pd.DataFrame(results)
    if not res_df.empty:
        print("\n" + "=" * 50)
        print(f"🎯 批量回测完成！共计产生交易的股票: {len(res_df)} 只")
        print(f"📈 全市场平均收益率: {res_df['总收益率'].mean():.2%}")
        print(f"🏆 整体赚钱比例 (胜率>0): {len(res_df[res_df['总收益率'] > 0]) / len(res_df):.2%}")

        res_df = res_df.sort_values('总收益率', ascending=False)

        print("\n🏆 --- 策略表现最佳 Top 5 ---")
        for _, row in res_df.head(5).iterrows():
            print(
                f"[{row['股票代码']}] {row['股票名称']}: 收益率 {row['总收益率']:.2%} | 胜率 {row['胜率']:.1%} | 交易 {row['交易次数(对)']}次")

        res_df['总收益率'] = res_df['总收益率'].apply(lambda x: f"{x:.2%}")
        res_df['胜率'] = res_df['胜率'].apply(lambda x: f"{x:.2%}")
        res_df['最大回撤'] = res_df['最大回撤'].apply(lambda x: f"{x:.2%}")

        output_csv = 'parallel_backtest_results.csv'
        res_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\n💾 详细结果已完整存入: {output_csv}")
    else:
        print("⚠️ 无有效回测数据，可能是策略未触发任何交易信号。")


if __name__ == "__main__":
    main()