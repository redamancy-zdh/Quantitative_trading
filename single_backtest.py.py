import pandas as pd
import numpy as np
import os
import json
import webbrowser
from MACD_strategy import apply_strategy


def run_backtest(target_code="002030", initial_cash=100000.0, save_csv=True):
    """
    核心回测计算函数：只负责数据读取、撮合交易、计算统计指标及输出流水
    返回: df_res (K线数据), marker_data (图表标记), stats (统计指标字典)
    """
    # 1. 数据路径处理
    data_path = 'A_share_all_history(hfq).parquet'
    if not os.path.exists(data_path):
        data_path = os.path.join('..', 'A_share_all_history(hfq).parquet')
    if not os.path.exists(data_path):
        print(f"❌ 找不到数据文件: {data_path}")
        return None

    print(f"🔍 正在读取股票 {target_code} 的数据进行回测...")

    # 2. 读取并筛选数据
    try:
        df = pd.read_parquet(data_path, filters=[('股票代码', '=', str(target_code))])
        if df.empty:
            df = pd.read_parquet(data_path, filters=[('股票代码', '=', int(target_code))])
    except Exception as e:
        print(f"❌ 读取 Parquet 错误: {e}")
        return None

    if df.empty:
        print(f"❌ 未找到股票 {target_code} 的数据")
        return None

    stock_name = df['股票名称'].iloc[-1] if '股票名称' in df.columns else target_code

    # 3. 计算策略与生成交易信号
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values('日期').reset_index(drop=True)
    df_res, trades, strat_stats = apply_strategy(df, initial_cash)

    # 4. 字段清洗与规范化 (为了后续图表和计算做准备)
    rename_cols = {'开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'}
    for cn, en in rename_cols.items():
        if cn in df_res.columns:
            df_res[en] = pd.to_numeric(df_res[cn], errors='coerce').astype(float)

    df_res['time'] = df_res['日期'].dt.strftime('%Y-%m-%d')
    df_res['MA5'] = df_res['close'].rolling(5).mean()
    df_res['MA20'] = df_res['close'].rolling(20).mean()
    df_res['MA60'] = df_res['close'].rolling(60).mean()

    cols_map = {c.lower(): c for c in df_res.columns}
    df_res['dif'] = df_res[cols_map.get('dif', 'dif')] if 'dif' in cols_map else 0.0
    df_res['dea'] = df_res[cols_map.get('dea', 'dea')] if 'dea' in cols_map else 0.0
    hist_key = cols_map.get('macd_hist') or cols_map.get('hist')
    df_res['hist'] = df_res[hist_key] if hist_key else 0.0

    if 'volume' not in df_res.columns:
        df_res['volume'] = 0.0

    # 5. 真实回测逻辑与数据记录
    capital = initial_cash
    shares = 0
    trade_pairs = 0
    wins = 0
    total_fees = 0.0
    records = []
    marker_data = []

    for t in trades:
        is_buy = t['type'].upper() == 'BUY'
        price = float(t['price'])
        date_str = pd.to_datetime(t['date']).strftime('%Y-%m-%d')

        t_shares = t['shares']
        t_fees = t['cost']
        t_cash_left = t['cash_left']
        total_fees += t_fees

        marker_data.append({
            "time": date_str,
            "position": "belowBar" if is_buy else "aboveBar",
            "color": "#ef5350" if is_buy else "#26a69a",
            "shape": "arrowUp" if is_buy else "arrowDown",
            "text": f"{price:.2f}"
        })

        # 构建改进版的单笔账单流水
        record = {
            '股票代码': target_code,
            '股票名称': stock_name,
            '交易日期': date_str,
            '买卖方向': '买入' if is_buy else '卖出',
            '成交价格': price,
            '成交数量(股)': t_shares,
            '交易金额': t_shares * price,
            '当笔费用': t_fees,
            '买卖双边总费用': 0.0,
            '可用现金': t_cash_left,
            '持有股数': 0,
            '总资产': 0.0,
            '单笔盈亏(扣费后)': 0.0
        }

        if is_buy:
            capital = t_cash_left
            shares += t_shares

            record['持有股数'] = shares
            record['总资产'] = capital + shares * price
            records.append(record)

        else:
            revenue = t_shares * price

            # 计算盈亏与双边手续费
            last_buy_cost = records[-1]['交易金额'] if records else 0
            last_buy_fee = records[-1]['当笔费用'] if records else 0

            # 这一单的完整摩擦成本 = 买入时费用 + 卖出时费用
            round_trip_fee = last_buy_fee + t_fees

            # 真实净利润 = 卖出总额 - 买入总额 - 买卖双边总费用
            profit = revenue - last_buy_cost - round_trip_fee

            capital = t_cash_left
            shares -= t_shares

            record['持有股数'] = shares
            record['总资产'] = capital
            record['买卖双边总费用'] = round_trip_fee
            record['单笔盈亏(扣费后)'] = profit
            records.append(record)

            trade_pairs += 1
            if profit > 0:
                wins += 1

    # 6. 计算最终资产与高级指标
    final_value = capital
    if shares > 0:
        last_close = float(df_res.iloc[-1]['close'])
        final_value = capital + (shares * last_close)

    total_return = (final_value - initial_cash) / initial_cash * 100
    win_rate = (wins / trade_pairs * 100) if trade_pairs > 0 else 0.0

    max_drawdown = 0.0
    annualized_return = 0.0
    sharpe_ratio = 0.0

    if records:
        res_df = pd.DataFrame(records)
        equity_df = pd.DataFrame({'time': df_res['time'], 'close': df_res['close']})
        equity_df['capital'] = initial_cash
        equity_df['shares'] = 0

        for index, row in res_df.iterrows():
            t_date = row['交易日期']
            mask = equity_df['time'] >= t_date
            equity_df.loc[mask, 'capital'] = row['可用现金']
            equity_df.loc[mask, 'shares'] = row['持有股数']

        equity_df['total_asset'] = equity_df['capital'] + equity_df['shares'] * equity_df['close']
        equity_df['cummax'] = equity_df['total_asset'].cummax()
        equity_df['drawdown'] = (equity_df['cummax'] - equity_df['total_asset']) / equity_df['cummax']
        max_drawdown = equity_df['drawdown'].max() * 100

        trading_days = len(equity_df)
        if trading_days > 0:
            annualized_return = ((final_value / initial_cash) ** (252 / trading_days) - 1) * 100

        equity_df['daily_return'] = equity_df['total_asset'].pct_change().fillna(0)
        daily_rf = 0.03 / 252
        std_daily = equity_df['daily_return'].std()
        if std_daily > 0:
            sharpe_ratio = (equity_df['daily_return'].mean() - daily_rf) / std_daily * np.sqrt(252)

        if save_csv:
            csv_filename = f"{target_code}_trade_records.csv"
            float_cols = ['成交价格', '交易金额', '当笔费用', '买卖双边总费用', '可用现金', '总资产',
                          '单笔盈亏(扣费后)']
            for col in float_cols:
                if col in res_df.columns:
                    res_df[col] = res_df[col].round(2)

            # 清理：买入时没有双边费用和单笔盈亏，置为 NaN 让表格看起来更干净
            res_df.loc[res_df['买卖方向'] == '买入', '买卖双边总费用'] = np.nan
            res_df.loc[res_df['买卖方向'] == '买入', '单笔盈亏(扣费后)'] = np.nan

            res_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"💾 交易明细已保存至: {os.path.abspath(csv_filename)}")

    # 封装备用统计数据返回
    stats = {
        'target_code': target_code,
        'stock_name': stock_name,
        'initial_cash': initial_cash,
        'final_value': final_value,
        'total_return': total_return,
        'annualized_return': annualized_return,
        'win_rate': win_rate,
        'max_drawdown': max_drawdown,
        'sharpe_ratio': sharpe_ratio,
        'total_fees': total_fees,
        'failed_buys': strat_stats['failed_buys'],
        'failed_sells': strat_stats['failed_sells'],
        'trade_pairs': trade_pairs
    }

    print(f"✅ [{target_code}] 回测统计完成！共交易 {trade_pairs} 笔。")
    print("-" * 30)
    print(f"📊 初始资金: ¥{initial_cash:,.2f} | 最终资产: ¥{final_value:,.2f}")
    print(f"📊 策略总收益: {total_return:.2f}% | 年化收益: {annualized_return:.2f}%")
    print(f"🎯 胜率: {win_rate:.2f}% | 📉 最大回撤: {max_drawdown:.2f}%")
    print(f"⚖️ 夏普比率: {sharpe_ratio:.2f} | 💸 总交易费用: ¥{total_fees:,.2f}")
    print(f"⚠️ 挂单重试次数: 买入 {strat_stats['failed_buys']} 次 | 卖出 {strat_stats['failed_sells']} 次")
    print("-" * 30)

    return df_res, marker_data, stats


def generate_html_report(df_res, marker_data, stats):
    """
    负责生成网页可视化，接收 run_backtest 的输出结果。如果批量跑数据，不要调用它。
    """
    target_code = stats['target_code']
    stock_name = stats['stock_name']

    export_cols = ['time', 'open', 'high', 'low', 'close', 'volume', 'MA5', 'MA20', 'MA60', 'dif', 'dea', 'hist']
    chart_data_df = df_res[export_cols].replace({np.nan: None})
    full_data_json = chart_data_df.to_json(orient='records')

    return_color = "#ef5350" if stats['total_return'] >= 0 else "#26a69a"
    backtest_html = (
        f"资产: <b style='color:{return_color}'>{stats['final_value']:,.0f}</b> | "
        f"收益: <b style='color:{return_color}'>{stats['total_return']:.1f}%</b> | "
        f"胜率: <b>{stats['win_rate']:.1f}%</b> | "
        f"回撤: <b style='color:#26a69a'>{stats['max_drawdown']:.1f}%</b> | "
        f"费用: <b style='color:#FFB74D'>{stats['total_fees']:,.0f}</b> | "
        f"重试: <b style='color:#FF5252'>买{stats['failed_buys']}/卖{stats['failed_sells']}</b>"
    )

    output_filename = f"viz_{target_code}.html"

    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>[{target_code}] 复盘</title>
        <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            body {{ margin: 0; padding: 0; background: #131722; color: #d1d4dc; font-family: sans-serif; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }}
            .header {{ height: 45px; line-height: 45px; padding: 0 20px; background: #1e222d; border-bottom: 1px solid #2B3139; display: flex; justify-content: space-between; font-size: 14px; white-space: nowrap; }}
            .legend {{ color: #d1d4dc; font-family: monospace; }}
            .legend b {{ margin-left: 5px; margin-right: 10px; font-weight: normal; }}
            .up {{ color: #ef5350; }} .down {{ color: #26a69a; }}
            .chart-wrapper {{ flex: 1; display: flex; flex-direction: column; }}
            .chart-pane {{ width: 100%; position: relative; border-bottom: 1px solid #2B3139; flex: 1.5; }}
            #main-pane {{ flex: 4.5; }}
            .pane-title {{ position: absolute; left: 10px; top: 5px; z-index: 10; font-size: 11px; color: #848e9c; background: rgba(30, 34, 45, 0.6); padding: 2px 6px; border-radius: 3px; pointer-events: none; }}
            #error-box {{ position: fixed; top: 0; width: 100%; background: #600; color: #fff; padding: 10px; z-index: 9999; display: none; font-family: monospace; }}
        </style>
    </head>
    <body>
        <div id="error-box"></div>
        <div class="header">
            <div>
                <b style="font-size:16px;">{stock_name} ({target_code})</b> 
                <span id="time-display" style="margin-left:15px; color:#848e9c;">--</span>
                <span style="margin-left: 20px; color: #d1d4dc; font-size: 13px;">
                    {backtest_html}
                </span>
            </div>
            <div id="legend-box" class="legend">请移动鼠标查看数据</div>
        </div>

        <div class="chart-wrapper">
            <div id="main-pane" class="chart-pane"><div class="pane-title">K 线 & 信号</div></div>
            <div id="macd-pane" class="chart-pane"><div class="pane-title">MACD</div></div>
        </div>

        <script>
            function showErr(msg) {{ document.getElementById('error-box').style.display = 'block'; document.getElementById('error-box').innerText = "❌ JS 报错: " + msg; console.error(msg); }}
            window.onload = function() {{
                try {{
                    const {{ createChart }} = LightweightCharts;
                    const rawData = {full_data_json};
                    const markers = {json.dumps(marker_data)};
                    const dataMap = new Map(rawData.map(obj => [obj.time, obj]));
                    const legendBox = document.getElementById('legend-box');
                    const timeDisplay = document.getElementById('time-display');

                    const opt = {{ layout: {{ background: {{ color: '#131722' }}, textColor: '#d1d4dc' }}, grid: {{ vertLines: {{ color: '#1f222d' }}, horzLines: {{ color: '#1f222d' }} }}, crosshair: {{ mode: 0 }}, timeScale: {{ borderColor: '#2B3139', fixRightEdge: true }}, localization: {{ dateFormat: 'yyyy-MM-dd' }} }};

                    const charts = {{ main: createChart(document.getElementById('main-pane'), opt), macd: createChart(document.getElementById('macd-pane'), opt) }};
                    const series = {{}};
                    series.candle = charts.main.addCandlestickSeries({{ upColor:'#ef5350', downColor:'#26a69a', borderVisible:false, wickUpColor:'#ef5350', wickDownColor:'#26a69a' }});
                    series.ma5 = charts.main.addLineSeries({{ color:'#2962FF', lineWidth:1, title: 'MA5' }});
                    series.ma20 = charts.main.addLineSeries({{ color:'#FF6D00', lineWidth:1, title: 'MA20' }});
                    series.ma60 = charts.main.addLineSeries({{ color:'#9C27B0', lineWidth:1, title: 'MA60' }});
                    series.macdH = charts.macd.addHistogramSeries({{ title: 'HIST' }});
                    series.dif = charts.macd.addLineSeries({{ color:'#FF6D00', lineWidth:1, title: 'DIF' }});
                    series.dea = charts.macd.addLineSeries({{ color:'#2962FF', lineWidth:1, title: 'DEA' }});

                    series.candle.setData(rawData.map(d => ({{ time: d.time, open: d.open, high: d.high, low: d.low, close: d.close }})));
                    series.ma5.setData(rawData.filter(d => d.MA5 !== null).map(d => ({{ time: d.time, value: d.MA5 }})));
                    series.ma20.setData(rawData.filter(d => d.MA20 !== null).map(d => ({{ time: d.time, value: d.MA20 }})));
                    series.ma60.setData(rawData.filter(d => d.MA60 !== null).map(d => ({{ time: d.time, value: d.MA60 }})));
                    series.macdH.setData(rawData.map(d => ({{ time: d.time, value: d.hist, color: d.hist >= 0 ? 'rgba(239, 83, 80, 0.8)' : 'rgba(38, 166, 154, 0.8)' }})));
                    series.dif.setData(rawData.map(d => ({{ time: d.time, value: d.dif }})));
                    series.dea.setData(rawData.map(d => ({{ time: d.time, value: d.dea }})));

                    if (markers.length > 0) series.candle.setMarkers(markers);

                    const chartList = Object.values(charts);
                    chartList.forEach(chart => {{
                        chart.timeScale().subscribeVisibleTimeRangeChange(range => {{ chartList.forEach(c => {{ if(c !== chart) c.timeScale().setVisibleRange(range); }}); }});
                        chart.subscribeCrosshairMove(param => {{
                            if (param.time) {{
                                const d = dataMap.get(param.time);
                                if (d) {{
                                    charts.main.setCrosshairPosition(d.close, param.time, series.candle);
                                    charts.macd.setCrosshairPosition(d.dif, param.time, series.dif);
                                    const change = ((d.close - d.open) / d.open * 100).toFixed(2);
                                    const cls = d.close >= d.open ? 'up' : 'down';
                                    timeDisplay.innerText = d.time;
                                    legendBox.innerHTML = '开<b class="' + cls + '">' + d.open.toFixed(2) + '</b> 高<b class="' + cls + '">' + d.high.toFixed(2) + '</b> 低<b class="' + cls + '">' + d.low.toFixed(2) + '</b> 收<b class="' + cls + '">' + d.close.toFixed(2) + '</b> 幅<b class="' + cls + '">' + change + '%</b>';
                                }}
                            }} else {{
                                chartList.forEach(c => c.clearCrosshairPosition());
                                legendBox.innerHTML = "请移动鼠标查看数据";
                                timeDisplay.innerText = "--";
                            }}
                        }});
                    }});

                    const viewLength = 120;
                    const startIndex = Math.max(0, rawData.length - viewLength);
                    charts.main.timeScale().setVisibleRange({{ from: rawData[startIndex].time, to: rawData[rawData.length - 1].time }});

                    window.addEventListener('resize', () => {{ chartList.forEach(c => {{ c.resize(c.chartElement().parentElement.clientWidth, c.chartElement().parentElement.clientHeight); }}); }});
                }} catch (e) {{ showErr(e.message); }}
            }};
        </script>
    </body>
    </html>
    """

    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(html_template)

    print(f"✅ HTML 图表已生成: {os.path.abspath(output_filename)}")
    webbrowser.open(f"file://{os.path.realpath(output_filename)}")


if __name__ == "__main__":
    # 第一步：运行回测逻辑并获取返回数据 (如果要做批量回测，把这部分放进循环里即可)
    result = run_backtest("000503", initial_cash=100000.0, save_csv=True)

    # 第二步：生成网页分析报告 (在批量跑全市场数据时，可以直接注释掉这部分，只收集 stats)
    if result:
        df_res, marker_data, stats = result
        generate_html_report(df_res, marker_data, stats)