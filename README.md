本项目用于验证股票交易策略的有效性，使用真实股票历史数据进行测试。

ashare_broker.py为股票的买卖判断，包括涨跌停判断，交易费用扣减。

plot_stock_indicators.py为画图代码，可以画出单只股票的K线图，均线，成交量，MACD，KDJ，RSI。

MACD_strategy.py为交易策略，采用最常见的MACD金叉买入，死叉卖出，MACD采用默认值。

single_backtest.py这个为单个股票的回测，会生成一个csv文件，包括了所有的买点和卖点以及交易收益，还会生成一个网站用来画图，可视化展示买点和卖点。

batch_backtest.py为批量回测股票，验证在不同的股票上该策略的胜率如何。

000503_trade_records.csv，parallel_backtest_results.csv，viz_000503.html为生成的结果文件。

