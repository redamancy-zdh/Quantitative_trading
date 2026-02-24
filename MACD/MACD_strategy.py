import pandas as pd
import numpy as np
import sys
import os

# 动态获取上级目录并加入系统路径，以便导入 ashare_broker
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from ashare_broker import AShareBroker


def apply_strategy(df, initial_cash=100000.0):
    """
    MACD 策略核心：包含次日开盘价重试逻辑与失败统计
    """
    df = df.copy()

    # --- 数据预处理与指标计算 ---
    if '昨收' not in df.columns:
        if '收盘' in df.columns:
            df['昨收'] = df['收盘'].shift(1)
        else:
            raise ValueError("数据中缺少 '收盘' 列，无法计算昨收价。")

    if '成交量' not in df.columns:
        df['成交量'] = 10000

    ema_fast = df['收盘'].ewm(span=12, adjust=False).mean()
    ema_slow = df['收盘'].ewm(span=26, adjust=False).mean()
    df['dif'] = ema_fast - ema_slow
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['dif'] - df['dea']) * 2

    df['prev_dif'] = df['dif'].shift(1)
    df['prev_dea'] = df['dea'].shift(1)

    # --- 实例化撮合引擎与状态机 ---
    broker = AShareBroker(initial_capital=initial_cash)
    trades = []

    # 挂单状态机
    pending_buy = False
    pending_sell = False
    failed_buys = 0
    failed_sells = 0

    for row in df.itertuples():
        if pd.isna(row.prev_dif) or pd.isna(row.昨收):
            continue

        date_str = row.日期.strftime('%Y-%m-%d') if hasattr(row.日期, 'strftime') else str(row.日期)

        # -------------------------------------------------
        # 1. 优先处理前一日未成交的挂单（以当日开盘价执行）
        # -------------------------------------------------
        if pending_buy:
            success, result = broker.execute_buy(
                date=date_str, price=row.开盘, pre_close=row.昨收,
                high_price=row.最高, low_price=row.最低, volume=row.成交量
            )
            if success:
                trades.append(result)
                pending_buy = False
            else:
                failed_buys += 1
                continue  # 挂单未解决前，跳过当日的新信号判断（除非你想取消挂单）

        elif pending_sell:
            success, result = broker.execute_sell(
                date=date_str, price=row.开盘, pre_close=row.昨收,
                high_price=row.最高, low_price=row.最低, volume=row.成交量
            )
            if success:
                trades.append(result)
                pending_sell = False
            else:
                failed_sells += 1
                continue  # 挂单未解决前，跳过当日的新信号判断

        # -------------------------------------------------
        # 2. 正常日内信号判断（以当日收盘价执行）
        # -------------------------------------------------
        if not pending_buy and not pending_sell:
            # 买入逻辑：金叉
            if broker.position_shares == 0 and row.prev_dif <= row.prev_dea and row.dif > row.dea:
                success, result = broker.execute_buy(
                    date=date_str, price=row.收盘, pre_close=row.昨收,
                    high_price=row.最高, low_price=row.最低, volume=row.成交量
                )
                if success:
                    trades.append(result)
                else:
                    failed_buys += 1
                    pending_buy = True  # 当日收盘买入失败，转为次日开盘挂单

            # 卖出逻辑：死叉
            elif broker.position_shares > 0 and row.prev_dif >= row.prev_dea and row.dif < row.dea:
                success, result = broker.execute_sell(
                    date=date_str, price=row.收盘, pre_close=row.昨收,
                    high_price=row.最高, low_price=row.最低, volume=row.成交量
                )
                if success:
                    trades.append(result)
                else:
                    failed_sells += 1
                    pending_sell = True  # 当日收盘卖出失败，转为次日开盘挂单

    # 封装备用统计数据返回
    stats = {
        'failed_buys': failed_buys,
        'failed_sells': failed_sells
    }
    return df, trades, stats
