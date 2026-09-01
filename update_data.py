#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 专用：每日更新基金数据并生成 web_fund_data.json
不依赖本地 Excel 文件，从 fund_list.json 读取基金代码和分类。
不依赖 openpyxl，仅使用标准库。
"""
import json, os, ssl, urllib.request, time
from datetime import datetime, timedelta

ssl._create_default_https_context = ssl._create_unverified_context

MIN_INTERVAL = 0.4
RETRY_TIMES = 3
RETRY_BACKOFF_BASE = 1.5

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_MOBILE_UA = 'Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 Mobile Safari/537.36'
_MOBILE_REFERER = 'https://fundmobapi.eastmoney.com/'

# 天天基金 API title → dashboard 字段映射
TITLE_MAP = {
    'JN': 'ytd',    # 今年以来
    'Y':  '1m',     # 近1月
    '3Y': '3m',     # 近3月
    '6Y': '6m',     # 近6月
    '1N': '1y',     # 近1年
    '3N': '3y',     # 近3年
}


def _http_get_json(url):
    req = urllib.request.Request(
        url,
        headers={
            'User-Agent': _MOBILE_UA,
            'Referer': _MOBILE_REFERER,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode('utf-8')
    return json.loads(text)


def fetch_fund_nav(code):
    """拉取一只基金的全部历史净值，返回正序 records。"""
    url = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList?'
        f'FCODE={code}&pageIndex=1&pageSize=5000&deviceid=test&plat=Android'
        '&product=EFund&version=4.5'
    )
    try:
        data = _http_get_json(url)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    items = data.get('Datas')
    if not isinstance(items, list) or not items:
        return None
    records = []
    name = ''
    for item in reversed(items):
        nav = item.get('DWJZ')
        fsrq = item.get('FSRQ')
        if nav and nav != '' and fsrq:
            records.append({'date': fsrq, 'nav': float(nav)})
        if not name and item.get('SHORTNAME'):
            name = item.get('SHORTNAME')
    if not records:
        return None
    return {'name': name, 'records': records}


def fetch_period_increase(code):
    """返回 {title: syl_float}。"""
    url = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNPeriodIncrease?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    try:
        data = _http_get_json(url)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    result = {}
    for item in data.get('Datas', []):
        title = item.get('title', '')
        syl = item.get('syl', '')
        if syl and str(syl).strip() and str(syl).strip() != '-':
            try:
                result[title] = float(str(syl).strip())
            except (ValueError, TypeError):
                pass
    return result if result else None


def get_nav_on_or_before(records, target):
    from datetime import datetime as dt
    target_dt = dt.strptime(target, '%Y-%m-%d') if isinstance(target, str) else target
    best = None
    for r in records:
        d = dt.strptime(r['date'], '%Y-%m-%d')
        if d <= target_dt:
            best = r
    return best


def calc_return(records, start_date, end_date):
    start = get_nav_on_or_before(records, start_date)
    end = get_nav_on_or_before(records, end_date)
    if start is None or end is None:
        return None
    s, e = float(start['nav']), float(end['nav'])
    if s == 0:
        return None
    return (e - s) / s


def calc_max_drawdown(records, start_date, end_date):
    start_dt = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date
    prices = [float(r['nav']) for r in records if start_dt <= datetime.strptime(r['date'], '%Y-%m-%d') <= end_dt]
    if not prices:
        return None
    peak = prices[0]
    mdd = 0
    for p in prices:
        if p > peak:
            peak = p
        dd = (peak - p) / peak
        if dd > mdd:
            mdd = dd
    return mdd


def _pct(cr):
    return round(cr * 100, 2) if cr is not None else None


def main():
    print(f'[{datetime.now()}] 开始更新基金数据...')

    # 从 fund_list.json 读取基金代码和分类
    with open(os.path.join(BASE_DIR, 'fund_list.json'), 'r', encoding='utf-8') as f:
        fund_list = json.load(f)

    codes = [f['code'] for f in fund_list]
    fund_info = {f['code']: f for f in fund_list}
    print(f'基金数量: {len(codes)}')

    # 抓取净值数据
    print('抓取净值数据...')
    nav_data = {}
    nav_failed = []
    last_req = [0.0]
    for i, code in enumerate(codes):
        elapsed = time.time() - last_req[0]
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        last_req[0] = time.time()

        result = None
        for attempt in range(1, RETRY_TIMES + 1):
            result = fetch_fund_nav(code)
            if result is not None:
                break
            if attempt < RETRY_TIMES:
                time.sleep(RETRY_BACKOFF_BASE * attempt)

        if result:
            nav_data[code] = result
        else:
            nav_failed.append(code)

        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'  净值进度: {i + 1}/{len(codes)}')

    print(f'净值成功: {len(nav_data)}/{len(codes)}, 失败: {len(nav_failed)}')

    # 抓取阶段涨幅
    print('抓取阶段涨幅数据...')
    period_data = {}
    pi_failed = []
    for i, code in enumerate(codes):
        elapsed = time.time() - last_req[0]
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        last_req[0] = time.time()

        pi = fetch_period_increase(code)
        if pi is not None:
            period_data[code] = pi
        else:
            pi_failed.append(code)

        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'  阶段涨幅进度: {i + 1}/{len(codes)}')

    print(f'阶段涨幅成功: {len(period_data)}/{len(codes)}, 失败: {len(pi_failed)}')

    # 构建网页数据
    print('构建网页数据...')
    combined = []
    for code in codes:
        if code not in nav_data:
            continue
        records = sorted(nav_data[code]['records'], key=lambda x: x['date'])
        if not records:
            continue
        latest = records[-1]['date']
        end_dt = datetime.strptime(latest, '%Y-%m-%d')

        info = fund_info.get(code, {})
        pi = period_data.get(code, {})

        # 优先使用 API 阶段涨幅数据
        returns = {
            'ytd': pi.get('JN'),
            '1m': pi.get('Y'),
            '3m': pi.get('3Y'),
            '6m': pi.get('6Y'),
            '1y': pi.get('1N'),
            '3y': pi.get('3N'),
        }
        # API 缺失时用 NAV 计算回退
        if returns['ytd'] is None:
            ytd = calc_return(records, f'{end_dt.year}-01-01', latest)
            returns['ytd'] = _pct(ytd)
        if returns['1m'] is None:
            r1m = calc_return(records, (end_dt - timedelta(days=29)).strftime('%Y-%m-%d'), latest)
            returns['1m'] = _pct(r1m)
        if returns['3m'] is None:
            r3m = calc_return(records, (end_dt - timedelta(days=92)).strftime('%Y-%m-%d'), latest)
            returns['3m'] = _pct(r3m)
        if returns['6m'] is None:
            r6m = calc_return(records, (end_dt - timedelta(days=183)).strftime('%Y-%m-%d'), latest)
            returns['6m'] = _pct(r6m)
        if returns['1y'] is None:
            r1y = calc_return(records, (end_dt - timedelta(days=365)).strftime('%Y-%m-%d'), latest)
            returns['1y'] = _pct(r1y)
        if returns['3y'] is None:
            r3y = calc_return(records, (end_dt - timedelta(days=1096)).strftime('%Y-%m-%d'), latest)
            returns['3y'] = _pct(r3y)

        mdd_3y = calc_max_drawdown(records, (end_dt - timedelta(days=1095)).strftime('%Y-%m-%d'), latest)

        combined.append({
            'code': code,
            'name': info.get('name', nav_data[code].get('name', '')),
            'category1': info.get('category1', ''),
            'category2': info.get('category2', ''),
            'category3': info.get('category3', ''),
            'initial_share': info.get('initial_share', ''),
            'returns': returns,
            'max_drawdown_3y': round(mdd_3y * 100, 2) if mdd_3y is not None else None,
            'investment_style': '',
            'pros': [],
            'cons': [],
            'sector': '',
            'morningstar_3y': None,
            'morningstar_5y': None,
            'holdings': [],
            'holding_type': '',
            'is_september_focus': False,
            'nav_history': [[r['date'], r['nav']] for r in records]
        })

    cat2s = sorted(set(f['category2'] for f in combined if f['category2']))
    cat3s = sorted(set(f['category3'] for f in combined if f['category3']))
    categories = {'level2': cat2s, 'level3': cat3s}

    data = {'funds': combined, 'categories': categories}
    json_path = os.path.join(BASE_DIR, 'web_fund_data.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    print(f'生成数据: {json_path}')
    print(f'[{datetime.now()}] 更新完成，共 {len(combined)} 只基金')


if __name__ == '__main__':
    main()
