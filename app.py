from flask import Flask, request, jsonify, send_file
import akshare as ak
import json
import os
import re
import time as _time
from datetime import datetime, timedelta
import pandas as pd
import traceback

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE        = os.path.join(BASE_DIR, 'data.json')
CACHE_FILE_A     = os.path.join(BASE_DIR, 'cache_a_stocks.json')
CACHE_FILE_HK    = os.path.join(BASE_DIR, 'cache_hk_stocks.json')
CACHE_TTL        = 24 * 3600  # 缓存24小时


# ─────────────────── 数据持久化 ───────────────────

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'stocks': []}


def save_data(data):
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─────────────────── 汇率 ───────────────────

def get_hkd_cny_rate():
    """获取港元兑人民币汇率，失败时返回默认值 0.924"""
    # 方法1: 东方财富外汇报价
    try:
        df = ak.fx_spot_quote()
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                row_str = ' '.join(str(v) for v in row.values)
                if 'HKD' in row_str or '港元' in row_str or '港币' in row_str:
                    for v in row.values:
                        try:
                            r = float(v)
                            if 0.5 <= r <= 1.5:
                                return round(r, 6)
                        except Exception:
                            pass
    except Exception:
        pass

    # 方法2: 中国银行汇率
    try:
        df = ak.currency_boc_sina(currency="港币")
        if df is not None and not df.empty:
            for col in df.columns:
                if '中间价' in col or '基准价' in col:
                    val = pd.to_numeric(df.iloc[-1][col], errors='coerce')
                    if pd.notna(val) and val > 10:
                        return round(float(val) / 100, 6)
    except Exception:
        pass

    return 0.924  # 兜底默认值


# ─────────────────── A 股搜索 ───────────────────

def _load_a_cache():
    """从磁盘加载 A 股代码-名称缓存，过期则重新抓取（首次约70秒）"""
    if os.path.exists(CACHE_FILE_A):
        age = _time.time() - os.path.getmtime(CACHE_FILE_A)
        if age < CACHE_TTL:
            with open(CACHE_FILE_A, 'r', encoding='utf-8') as f:
                return pd.DataFrame(json.load(f))
    print("[cache] A股列表缓存过期或不存在，重新抓取（约1分钟）...")
    df = ak.stock_zh_a_spot_em()
    subset = df[['代码', '名称']].copy()
    subset['代码'] = subset['代码'].astype(str)
    with open(CACHE_FILE_A, 'w', encoding='utf-8') as f:
        json.dump(subset.to_dict('records'), f, ensure_ascii=False)
    print("[cache] A股列表缓存完成")
    return subset


def _get_a_stock_by_code(code):
    """通过代码直接查询单股信息（极快，约0.1秒）"""
    info = ak.stock_individual_info_em(symbol=code)
    # info 结构固定：行0=最新价, 行1=股票代码, 行2=股票简称
    price = pd.to_numeric(info.iloc[0]['value'], errors='coerce')
    name  = str(info.iloc[2]['value'])
    return {
        'symbol':        code,
        'name':          name,
        'current_price': float(price) if pd.notna(price) else None,
    }


def search_a_stock(query):
    """
    搜索 A 股。
    - 6位纯数字：直接走单股接口（快）
    - 名称：走本地缓存列表（首次慢，后续快）
    """
    # 快速路径：输入看起来是代码
    if query.isdigit():
        try:
            return _get_a_stock_by_code(query)
        except Exception as e:
            print(f"[search_a_stock] 按代码查询失败: {e}")

    # 名称搜索：走本地缓存
    try:
        stock_list = _load_a_cache()
        stock_list['代码'] = stock_list['代码'].astype(str)
        matched = stock_list[
            (stock_list['代码'] == query) |
            (stock_list['名称'].str.contains(query, na=False, regex=False))
        ]
        if not matched.empty:
            code = str(matched.iloc[0]['代码'])
            name = str(matched.iloc[0]['名称'])
            try:
                info = _get_a_stock_by_code(code)
                info['name'] = name
                return info
            except Exception:
                return {'symbol': code, 'name': name, 'current_price': None}
    except Exception as e:
        print(f"[search_a_stock] 名称搜索失败: {e}")

    return None


# ─────────────────── 港股搜索 ───────────────────

def _get_hk_stock_by_code(code):
    """
    通过代码直接查询港股信息（名称 + 最新收盘价）。
    - stock_hk_security_profile_em → 证券简称（col[1]）
    - stock_hk_hist                → 最新收盘价（col[2]）
    """
    code = code.zfill(5)

    # 获取股票简称
    name = code
    try:
        prof = ak.stock_hk_security_profile_em(symbol=code)
        if not prof.empty:
            name = str(prof.iloc[0, 1])   # 证券简称
    except Exception as e:
        print(f"[hk_name] {e}")

    # 获取最新收盘价（优先 stock_hk_daily，备用 stock_hk_hist）
    current_price = None
    try:
        hist = ak.stock_hk_daily(symbol=code, adjust='')
        if not hist.empty:
            current_price = float(hist.iloc[-1]['close'])
    except Exception:
        try:
            end   = datetime.now().strftime('%Y%m%d')
            start = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
            hist  = ak.stock_hk_hist(symbol=code, period='daily',
                                      start_date=start, end_date=end, adjust='')
            if not hist.empty:
                current_price = float(hist.iloc[-1, 2])
        except Exception as e:
            print(f"[hk_price] {e}")

    return {'symbol': code, 'name': name, 'current_price': current_price}


def _load_hk_cache():
    """
    从磁盘加载港股代码-名称缓存；
    首次加载用 stock_hk_spot_em()，失败时返回空表（代码搜索仍可用）。
    """
    if os.path.exists(CACHE_FILE_HK):
        age = _time.time() - os.path.getmtime(CACHE_FILE_HK)
        if age < CACHE_TTL:
            with open(CACHE_FILE_HK, 'r', encoding='utf-8') as f:
                return pd.DataFrame(json.load(f))

    print("[cache] 港股列表缓存过期或不存在，重新抓取...")
    for fn in ['stock_hk_spot_em', 'stock_hk_main_board_spot_em']:
        try:
            df = getattr(ak, fn)()
            if df is not None and not df.empty and '代码' in df.columns:
                subset = df[['代码', '名称']].copy()
                subset['代码'] = subset['代码'].astype(str)
                with open(CACHE_FILE_HK, 'w', encoding='utf-8') as f:
                    json.dump(subset.to_dict('records'), f, ensure_ascii=False)
                print(f"[cache] 港股列表缓存完成（来源: {fn}）")
                return subset
        except Exception as e:
            print(f"[cache] {fn} 失败: {e}")

    print("[cache] 港股列表获取失败，名称搜索不可用，请直接输入代码")
    return pd.DataFrame(columns=['代码', '名称'])


def search_hk_stock(query):
    """
    搜索港股。
    - 纯数字（代码）：直接走单股接口（快）
    - 名称：走本地缓存列表
    """
    if query.isdigit():
        try:
            return _get_hk_stock_by_code(query)
        except Exception as e:
            print(f"[search_hk_stock] 按代码查询失败: {e}")

    # 名称搜索：走本地缓存
    try:
        stock_list = _load_hk_cache()
        if not stock_list.empty:
            stock_list['代码'] = stock_list['代码'].astype(str)
            matched = stock_list[
                stock_list['名称'].str.contains(query, na=False, regex=False)
            ]
            if not matched.empty:
                code = str(matched.iloc[0]['代码'])
                try:
                    return _get_hk_stock_by_code(code)
                except Exception:
                    return {'symbol': code,
                            'name':   str(matched.iloc[0]['名称']),
                            'current_price': None}
    except Exception as e:
        print(f"[search_hk_stock] 名称搜索失败: {e}")

    return None


# ─────────────────── A 股分红 ───────────────────

def _find_col(df, keywords):
    """在 DataFrame 列名中匹配关键词，返回第一个命中的列名"""
    for col in df.columns:
        if all(kw in col for kw in keywords):
            return col
    for col in df.columns:
        if any(kw in col for kw in keywords):
            return col
    return None


def get_a_dividend(symbol):
    """
    获取 A 股近一年合计每股税前分红（元）及最近两次派息日。
    stock_dividend_cninfo 返回结构：
      [0] 实施方案公告日期  [1] 分红类型  [2] 送股比例
      [3] 转增比例          [4] 派息比例(每10股)  [5] 股权登记日
      [6] 除权日            [7] 派息日   ...      [10] 报告时间
    返回 (float, dates, None) 或 (None, [], error_str)
    """
    try:
        df = ak.stock_dividend_cninfo(symbol=symbol)
        if df is None or df.empty:
            return None, [], "无分红记录"

        cols = df.columns.tolist()
        if len(cols) < 5:
            return None, [], f"数据列数异常: {cols}"

        div_col      = cols[4]   # 派息比例（每10股）
        date_col     = cols[0]   # 实施方案公告日期
        pay_date_col = cols[7] if len(cols) > 7 else None   # 派息日

        df[div_col]  = pd.to_numeric(df[div_col],  errors='coerce').fillna(0)
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        # 提取最近两次派息日（只取有现金分红的行，取末尾最新的两条）
        dates = []
        if pay_date_col:
            cash_rows = df[df[div_col] > 0].copy()
            cash_rows[pay_date_col] = pd.to_datetime(cash_rows[pay_date_col], errors='coerce')
            for v in cash_rows[pay_date_col].dropna().tail(2)[::-1]:
                try:
                    dates.append(pd.Timestamp(v).strftime('%Y-%m-%d'))
                except Exception:
                    pass

        one_year_ago = datetime.now() - timedelta(days=365)
        df_recent = df[df[date_col] >= one_year_ago]

        if not df_recent.empty:
            total_per10 = float(df_recent[div_col].sum())
            if total_per10 > 0:
                return round(total_per10 / 10, 6), dates, None

        # 回退：取最新两条记录
        total_per10 = float(df.head(2)[div_col].sum())
        if total_per10 > 0:
            return round(total_per10 / 10, 6), dates, None

        return None, dates, "近期现金分红金额为 0"

    except Exception as e:
        traceback.print_exc()
        return None, [], str(e)


# ─────────────────── 港股分红 ───────────────────

def _parse_hkd(text):
    """从分红文案中提取港元金额。
    格式示例：每股派息1.013元(相当于港币1.118595元)
    """
    m = re.search(r'港[币幣]([\d.]+)', str(text))
    if m:
        return float(m.group(1))
    # 没有港币换算，直接取派息金额
    m2 = re.search(r'派息([\d.]+)', str(text))
    if m2:
        return float(m2.group(1))
    return 0.0


def get_hk_dividend(symbol):
    """
    获取港股近一年合计每股分红（港元）及最近两次除净日。
    使用 stock_hk_dividend_payout_em，列结构（降序）：
      [0] 最新公告日期  [1] 财政年度  [2] 分红方案（含港元金额）
      [3] 分配类型      [4] 除净日    [5] 截至过户日  [6] 发放日
    返回 (float, dates, None) 或 (None, [], error_str)
    """
    try:
        code = symbol.zfill(5)
        df = ak.stock_hk_dividend_payout_em(symbol=code)
        if df is None or df.empty:
            return None, [], "无港股分红记录"

        cols = df.columns.tolist()
        announce_col = cols[0]   # 最新公告日期
        method_col   = cols[2]   # 分红方案
        exdiv_col    = cols[4]   # 除净日

        df[announce_col] = pd.to_datetime(df[announce_col], errors='coerce')
        df['_hkd']       = df[method_col].apply(_parse_hkd)
        df[exdiv_col]    = pd.to_datetime(df[exdiv_col],    errors='coerce')

        # 提取最近两次除净日（数据降序，取前两行中有有效日期的）
        dates = []
        for v in df[df['_hkd'] > 0][exdiv_col].dropna().head(2):
            dates.append(pd.Timestamp(v).strftime('%Y-%m-%d'))

        # 近一年合计分红
        one_year_ago = datetime.now() - timedelta(days=365)
        df_recent    = df[df[announce_col] >= one_year_ago]

        if not df_recent.empty:
            total = float(df_recent['_hkd'].sum())
            if total > 0:
                return round(total, 6), dates, None

        # 回退：取最近两行
        total = float(df.head(2)['_hkd'].sum())
        if total > 0:
            return round(total, 6), dates, None

        return None, dates, "分红金额解析为 0"

    except Exception as e:
        traceback.print_exc()
        return None, [], f"港股分红获取失败: {e}"


# ─────────────────── Flask 路由 ───────────────────

@app.route('/')
def index():
    return send_file(os.path.join(BASE_DIR, 'index.html'))


@app.route('/api/hkd_rate')
def hkd_rate():
    rate = get_hkd_cny_rate()
    return jsonify({'rate': rate})


@app.route('/api/search', methods=['POST'])
def search_stock():
    req = request.json or {}
    query  = req.get('query', '').strip()
    market = req.get('market', 'A')

    if not query:
        return jsonify({'error': '请输入股票名称或代码'}), 400

    result = {
        'symbol':            query,
        'name':              '',
        'market':            market,
        'current_price':     None,
        'dividend':          None,
        'dividend_dates':    [],
        'dividend_currency': 'CNY' if market == 'A' else 'HKD',
        'hkd_rate':          None,
        'errors':            [],
    }

    try:
        if market == 'A':
            info = search_a_stock(query)
            if info:
                result.update(info)
            else:
                result['errors'].append(f'未找到 A 股: {query}，将以代码方式尝试获取分红')

            div, dates, err = get_a_dividend(result['symbol'])
            if div is not None:
                result['dividend']       = div
                result['dividend_dates'] = dates
            else:
                result['dividend_dates'] = dates
                result['errors'].append(f'分红获取失败: {err}')

        else:  # HK
            info = search_hk_stock(query)
            if not info:
                # 搜索失败：直接返回错误，不保存无效记录
                return jsonify({
                    'error': f'未找到港股「{query}」，请输入股票代码（如 03968）',
                    'errors': [],
                })

            result.update(info)

            div, dates, err = get_hk_dividend(result['symbol'])
            if div is not None:
                result['dividend']       = div
                result['dividend_dates'] = dates
            else:
                result['dividend_dates'] = dates
                result['errors'].append(f'港股分红获取失败: {err}')

            result['hkd_rate'] = get_hkd_cny_rate()

    except Exception as e:
        traceback.print_exc()
        result['errors'].append(str(e))

    return jsonify(result)


@app.route('/api/stocks', methods=['GET'])
def get_stocks():
    data = load_data()
    return jsonify(data['stocks'])


@app.route('/api/stocks', methods=['POST'])
def add_stock():
    stock = request.json or {}
    data  = load_data()
    stock['id']         = str(int(datetime.now().timestamp() * 1000))
    stock['created_at'] = datetime.now().isoformat()
    data['stocks'].append(stock)
    save_data(data)
    return jsonify(stock)


@app.route('/api/stocks/<stock_id>', methods=['PUT'])
def update_stock(stock_id):
    updates = request.json or {}
    data    = load_data()
    for s in data['stocks']:
        if s['id'] == stock_id:
            s.update(updates)
            s['updated_at'] = datetime.now().isoformat()
            break
    save_data(data)
    return jsonify({'success': True})


@app.route('/api/stocks/<stock_id>', methods=['DELETE'])
def delete_stock(stock_id):
    data = load_data()
    data['stocks'] = [s for s in data['stocks'] if s['id'] != stock_id]
    save_data(data)
    return jsonify({'success': True})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 50)
    print("  股息率管理工具已启动")
    print(f"  请用浏览器打开: http://localhost:{port}")
    print("=" * 50)
    app.run(debug=False, port=port, host='0.0.0.0')
