import pandas as pd
import numpy as np
import math


class AShareBroker:
    """
    A股核心交易撮合与账户管理引擎
    负责处理：T+1机制、涨跌停限制、停牌限制、整手买入、印花税、佣金(含最低5元)、过户费。
    """

    def __init__(self, initial_capital=100000.0):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.position_shares = 0  # 当前持股数量
        self.last_buy_date = None  # 记录最后一次买入日期，用于T+1判断

        # A股现行费率标准 (根据2023年8月后最新印花税标准)
        self.commission_rate = 0.0001  # 佣金：万分之一 (双向)
        self.min_commission = 5.0  # 最低佣金：5元
        self.stamp_duty_rate = 0.0005  # 印花税：万分之五 (仅卖出收取)
        self.transfer_fee_rate = 0.00001  # 过户费：十万分之一 (双向)

    def _calculate_costs(self, trade_value, is_buy):
        """计算交易总成本"""
        # 1. 佣金 (不足5元按5元收)
        commission = max(trade_value * self.commission_rate, self.min_commission)
        # 2. 过户费
        transfer_fee = trade_value * self.transfer_fee_rate
        # 3. 印花税 (仅卖出收)
        stamp_duty = 0.0 if is_buy else trade_value * self.stamp_duty_rate

        return commission + transfer_fee + stamp_duty

    def _is_limit_up_down(self, exec_price, pre_close, high_price, low_price, is_buy):
        """
        利用真实的高低价和A股动态涨跌停规则判断是否无法交易。
        """
        if pre_close is None or pd.isna(pre_close) or pre_close == 0:
            return False

        # 1. 极端情况：一字板 (全天只有一个价格，最高价等于最低价)
        if high_price == low_price:
            if is_buy and exec_price > pre_close:
                return True  # 一字涨停，买不进
            if not is_buy and exec_price < pre_close:
                return True  # 一字跌停，卖不出

        # 2. 动态匹配真实涨跌停价位 (容差 0.01 元，应对四舍五入误差)
        # 涵盖 ST股(5%)、主板(10%)、科创/创业板(20%)、北交所(30%)
        limits = [0.05, 0.10, 0.20, 0.30]

        if is_buy:
            # 买入时：如果执行价达到了当天的最高价，并且这个价格是涨停价，大概率封板排队无法买入
            if exec_price == high_price:
                for limit in limits:
                    theoretical_limit_up = round(pre_close * (1 + limit), 2)
                    if abs(exec_price - theoretical_limit_up) <= 0.01:
                        return True
        else:
            # 卖出时：如果执行价跌到了当天的最低价，并且这个价格是跌停价，大概率封板排队无法卖出
            if exec_price == low_price:
                for limit in limits:
                    theoretical_limit_down = round(pre_close * (1 - limit), 2)
                    if abs(exec_price - theoretical_limit_down) <= 0.01:
                        return True

        return False

    # --- 同步修改 execute_buy 和 execute_sell，接收 high_price 和 low_price ---

    def execute_buy(self, date, price, pre_close, high_price, low_price, volume):
        if volume == 0: return False, "买入失败: 股票停牌"
        # 传入高低价进行判断
        if self._is_limit_up_down(price, pre_close, high_price, low_price, is_buy=True):
            return False, "买入失败: 触及涨停板无法成交"

        # ... 后续资金扣除代码保持不变 ...
        available_cash = self.cash * 0.998
        import math
        max_shares = math.floor(available_cash / price / 100) * 100
        if max_shares < 100: return False, "买入失败: 资金不足买入1手(100股)"
        trade_value = max_shares * price
        costs = self._calculate_costs(trade_value, is_buy=True)
        total_cost = trade_value + costs
        if self.cash >= total_cost:
            self.cash -= total_cost
            self.position_shares += max_shares
            self.last_buy_date = date
            return True, {'date': date, 'type': 'BUY', 'price': price, 'shares': max_shares, 'cost': round(costs, 2),
                          'cash_left': round(self.cash, 2)}
        return False, "买入失败: 最终结算资金不足"

    def execute_sell(self, date, price, pre_close, high_price, low_price, volume):
        if volume == 0: return False, "卖出失败: 股票停牌"
        # 传入高低价进行判断
        if self._is_limit_up_down(price, pre_close, high_price, low_price, is_buy=False):
            return False, "卖出失败: 触及跌停板无法成交"
        if self.last_buy_date is not None and date <= self.last_buy_date:
            return False, f"卖出失败: 触发T+1限制 (买入日期: {self.last_buy_date})"

        # ... 后续卖出结算代码保持不变 ...
        shares_to_sell = self.position_shares
        if shares_to_sell == 0: return False, "卖出失败: 无持仓"
        trade_value = shares_to_sell * price
        costs = self._calculate_costs(trade_value, is_buy=False)
        net_income = trade_value - costs
        self.cash += net_income
        self.position_shares = 0
        return True, {'date': date, 'type': 'SELL', 'price': price, 'shares': shares_to_sell, 'cost': round(costs, 2),
                      'cash_left': round(self.cash, 2)}