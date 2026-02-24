import pandas as pd
import webbrowser
import os
import numpy as np
import json


def generate_pro_quant_terminal(target_stock_code="000029"):
    # 数据集路径
    parquet_file = "A_share_all_history.parquet"
    output_filename = f"A_share_{target_stock_code}_pro_terminal.html"

    if not os.path.exists(parquet_file):
        parquet_file = "stock_000029_sample.parquet"
        if not os.path.exists(parquet_file):
            print(f"❌ 找不到数据文件")
            return

    # 1. 读取并筛选数据
    try:
        df = pd.read_parquet(parquet_file, engine='pyarrow', filters=[('股票代码', '=', str(target_stock_code))])
    except Exception as e:
        print(f"❌ 读取错误: {e}")
        return

    if df.empty:
        print(f"❌ 未找到股票 {target_stock_code} 的数据")
        return

    # 2. 字段清洗
    rename_map = {'日期': 'time', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'}
    df = df.rename(columns=rename_map)
    stock_name = df['股票名称'].iloc[-1] if '股票名称' in df.columns else "未知股票"

    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
    df = df.drop_duplicates(subset=['time']).sort_values('time').reset_index(drop=True)

    # 3. 计算指标
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA60'] = df['close'].rolling(60).mean()

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = ema12 - ema26
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_HIST'] = (df['DIF'] - df['DEA']) * 2
    l9 = df['low'].rolling(9).min()
    h9 = df['high'].rolling(9).max()
    rsv = 100 * (df['close'] - l9) / (h9 - l9 + 1e-8)
    df['K'] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df['D'] = df['K'].ewm(alpha=1 / 3, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    diff = df['close'].diff()
    df['RSI'] = 100 - (100 / (
                1 + (diff.clip(lower=0).ewm(com=13).mean() / (-1 * diff.clip(upper=0)).ewm(com=13).mean() + 1e-8)))

    # 4. 数据转换为 JSON (替换 NaN 防止 JS 报错)
    full_data_json = df.replace({np.nan: None}).to_json(orient='records')

    # 5. HTML 生成
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>[{target_stock_code}] {stock_name} - 量化复盘</title>
        <script src="https://cdn.jsdelivr.net/npm/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            body {{ margin: 0; padding: 0; background: #131722; color: #d1d4dc; font-family: sans-serif; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }}
            .header {{ height: 45px; line-height: 45px; padding: 0 20px; background: #1e222d; border-bottom: 1px solid #2B3139; display: flex; justify-content: space-between; font-size: 14px; }}
            .legend {{ color: #d1d4dc; font-family: monospace; }}
            .legend b {{ margin-left: 8px; font-weight: normal; }}
            .up {{ color: #ef5350; }} .down {{ color: #26a69a; }}
            .chart-wrapper {{ flex: 1; display: flex; flex-direction: column; }}
            .chart-pane {{ width: 100%; position: relative; border-bottom: 1px solid #2B3139; flex: 1.5; }}
            #main-pane {{ flex: 4.5; }}
            .pane-title {{
                position: absolute; left: 10px; top: 5px; z-index: 10;
                font-size: 11px; color: #848e9c; background: rgba(30, 34, 45, 0.4);
                padding: 1px 5px; border-radius: 2px; pointer-events: none;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <div><b style="font-size:16px;">[{target_stock_code}] {stock_name}</b> <span id="time-display" style="margin-left:15px; color:#848e9c;">--</span></div>
            <div id="legend-box" class="legend">请移动鼠标查看数据</div>
        </div>
        <div class="chart-wrapper">
            <div id="main-pane" class="chart-pane"><div class="pane-title">MA Indicators</div></div>
            <div id="vol-pane" class="chart-pane"><div class="pane-title">VOLUME</div></div>
            <div id="macd-pane" class="chart-pane"><div class="pane-title">MACD</div></div>
            <div id="kdj-pane" class="chart-pane"><div class="pane-title">KDJ</div></div>
            <div id="rsi-pane" class="chart-pane"><div class="pane-title">RSI</div></div>
        </div>

        <script>
            const {{ createChart, CandlestickSeries, LineSeries, HistogramSeries }} = LightweightCharts;
            const rawData = {full_data_json};

            // 使用 Map 优化查询效率
            const dataMap = new Map(rawData.map(obj => [obj.time, obj]));

            const legendBox = document.getElementById('legend-box');
            const timeDisplay = document.getElementById('time-display');

            const opt = {{
                layout: {{ background: {{ color: '#131722' }}, textColor: '#d1d4dc' }},
                grid: {{ vertLines: {{ color: '#1f222d' }}, horzLines: {{ color: '#1f222d' }} }},
                crosshair: {{ mode: 0 }}, // 使用 Normal 模式，十字线随鼠标走
                timeScale: {{ borderColor: '#2B3139', fixRightEdge: true, minBarSpacing: 0.5 }},
                handleScale: {{ axisPressedMouseMove: {{ time: false, price: false }}, mouseWheel: true }},
                localization: {{ dateFormat: 'yyyy-MM-dd' }}
            }};

            // 初始化图表
            const charts = {{
                main: createChart(document.getElementById('main-pane'), opt),
                vol: createChart(document.getElementById('vol-pane'), opt),
                macd: createChart(document.getElementById('macd-pane'), opt),
                kdj: createChart(document.getElementById('kdj-pane'), opt),
                rsi: createChart(document.getElementById('rsi-pane'), opt)
            }};

            // 定义系列 (用于 Y 轴标签定位)
            const series = {{}};
            series.candle = charts.main.addSeries(CandlestickSeries, {{ upColor:'#ef5350', downColor:'#26a69a', borderVisible:false, wickUpColor:'#ef5350', wickDownColor:'#26a69a' }});
            series.ma5 = charts.main.addSeries(LineSeries, {{ color:'#2962FF', lineWidth:1 }});
            series.ma20 = charts.main.addSeries(LineSeries, {{ color:'#FF6D00', lineWidth:1 }});
            series.ma60 = charts.main.addSeries(LineSeries, {{ color:'#9C27B0', lineWidth:1 }});

            series.vol = charts.vol.addSeries(HistogramSeries, {{ priceFormat:{{type:'volume'}} }});

            series.macdH = charts.macd.addSeries(HistogramSeries, {{ priceFormat:{{type:'volume'}} }});
            series.dif = charts.macd.addSeries(LineSeries, {{ color:'#FF6D00', lineWidth:1 }});
            series.dea = charts.macd.addSeries(LineSeries, {{ color:'#2962FF', lineWidth:1 }});

            series.k = charts.kdj.addSeries(LineSeries, {{ color:'#FF6D00', lineWidth:1 }});
            series.d = charts.kdj.addSeries(LineSeries, {{ color:'#2962FF', lineWidth:1 }});
            series.j = charts.kdj.addSeries(LineSeries, {{ color:'#9C27B0', lineWidth:1 }});

            // RSI 颜色变回去 (亮蓝)，警示线用偏白
            series.rsi = charts.rsi.addSeries(LineSeries, {{ color:'#2962FF', lineWidth:1.5 }});

            // 填充数据
            series.candle.setData(rawData.map(d => ({{ time: d.time, open: d.open, high: d.high, low: d.low, close: d.close }})));
            series.ma5.setData(rawData.map(d => ({{ time: d.time, value: d.MA5 }})));
            series.ma20.setData(rawData.map(d => ({{ time: d.time, value: d.MA20 }})));
            series.ma60.setData(rawData.map(d => ({{ time: d.time, value: d.MA60 }})));
            series.vol.setData(rawData.map(d => ({{ time: d.time, value: d.volume, color: d.close >= d.open ? 'rgba(239, 83, 80, 0.5)' : 'rgba(38, 166, 154, 0.5)' }})));
            series.macdH.setData(rawData.map(d => ({{ time: d.time, value: d.MACD_HIST, color: d.MACD_HIST > 0 ? '#ef5350' : '#26a69a' }})));
            series.dif.setData(rawData.map(d => ({{ time: d.time, value: d.DIF }})));
            series.dea.setData(rawData.map(d => ({{ time: d.time, value: d.DEA }})));
            series.k.setData(rawData.map(d => ({{ time: d.time, value: d.K }})));
            series.d.setData(rawData.map(d => ({{ time: d.time, value: d.D }})));
            series.j.setData(rawData.map(d => ({{ time: d.time, value: d.J }})));
            series.rsi.setData(rawData.map(d => ({{ time: d.time, value: d.RSI }})));

            // RSI 警示线：颜色改为 #F5F5F5
            series.rsi.createPriceLine({{ price: 70, color: '#F5F5F5', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '' }});
            series.rsi.createPriceLine({{ price: 30, color: '#F5F5F5', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '' }});

            // 核心功能：全自动 Y 轴标签同步
            const chartList = Object.values(charts);

            chartList.forEach(chart => {{
                // 同步缩放
                chart.timeScale().subscribeVisibleTimeRangeChange(range => {{
                    chartList.forEach(c => {{ if(c !== chart) c.timeScale().setVisibleRange(range); }});
                }});

                // 同步十字线与 Y 轴气泡
                chart.subscribeCrosshairMove(param => {{
                    if (param.time) {{
                        const d = dataMap.get(param.time);
                        if (d) {{
                            // 为每个子图强制设置 Crosshair 位置，从而触发 Y 轴气泡
                            charts.main.setCrosshairPosition(d.close, param.time, series.candle);
                            charts.vol.setCrosshairPosition(d.volume, param.time, series.vol);
                            charts.macd.setCrosshairPosition(d.DIF, param.time, series.dif);
                            charts.kdj.setCrosshairPosition(d.K, param.time, series.k);
                            charts.rsi.setCrosshairPosition(d.RSI, param.time, series.rsi);

                            // 更新顶部 Legend
                            const change = ((d.close - d.open) / d.open * 100).toFixed(2);
                            const cls = d.close >= d.open ? 'up' : 'down';
                            timeDisplay.innerText = d.time;
                            legendBox.innerHTML = '开<b class="' + cls + '">' + d.open.toFixed(2) + '</b> ' +
                                                  '高<b class="' + cls + '">' + d.high.toFixed(2) + '</b> ' +
                                                  '低<b class="' + cls + '">' + d.low.toFixed(2) + '</b> ' +
                                                  '收<b class="' + cls + '">' + d.close.toFixed(2) + '</b> ' +
                                                  '幅<b class="' + cls + '">' + change + '%</b> ' +
                                                  '量<b>' + (d.volume/10000).toFixed(2) + '万</b>';
                        }}
                    }} else {{
                        chartList.forEach(c => c.clearCrosshairPosition());
                        legendBox.innerHTML = "请移动鼠标查看数据";
                        timeDisplay.innerText = "--";
                    }}
                }});
            }});

            // 默认显示半年
            charts.main.timeScale().setVisibleRange({{ 
                from: '{df['time'].iloc[-120] if len(df) > 120 else df['time'].iloc[0]}', 
                to: '{df['time'].iloc[-1]}' 
            }});

            window.addEventListener('resize', () => {{
                chartList.forEach(c => {{
                    c.resize(c.chartElement().parentElement.clientWidth, c.chartElement().parentElement.clientHeight);
                }});
            }});
        </script>
    </body>
    </html>
    """

    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"✅ 生成成功: {output_filename}")
    webbrowser.open(f"file://{os.path.realpath(output_filename)}")


if __name__ == "__main__":
    generate_pro_quant_terminal("000029")