#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 专用：每日更新基金数据并生成 web_fund_data.json
不依赖本地 Excel 文件，从 fund_list.json 读取基金代码和分类。
不依赖 openpyxl，仅使用标准库。
"""
import json, os, ssl, urllib.request, time, re
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


def fetch_fund_basic_info(code):
    """拉取基金基础信息：规模、申赎状态、费率、限额等。"""
    url = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNBasicInformation?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    try:
        data = _http_get_json(url)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    d = data.get('Datas')
    if not isinstance(d, dict):
        return None
    return {
        'scale': d.get('ENDNAV') or d.get('FEGM') or None,
        'purchase_status': d.get('SGZT', ''),
        'redeem_status': d.get('SHZT', ''),
        'min_purchase': d.get('MINSG'),
        'max_purchase': d.get('MAXSG'),
        'source_rate': d.get('SOURCERATE', ''),
        'actual_rate': d.get('RATE', ''),
        'fund_type': d.get('FTYPE', ''),
        'risk_level': d.get('RISKLEVEL', ''),
    }


def fetch_fund_holdings(code):
    """拉取基金最新持仓，返回 {stocks:[...], bonds:[...], fofs:[...], date: str}。
    股票字段：GPDM(代码), GPJC(名称), JZBL(占净值比), INDEXNAME(行业)
    债券字段：ZQDM(代码), ZQMC(名称), ZJZBL(占净值比)
    """
    url = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNInverstPosition?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    try:
        data = _http_get_json(url)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    d = data.get('Datas')
    if not isinstance(d, dict):
        return None

    def _num(v):
        try:
            return float(str(v).replace(',', '')) if v not in (None, '', '--') else None
        except (ValueError, TypeError):
            return None

    stocks = []
    for item in d.get('fundStocks', []) or []:
        ratio = _num(item.get('JZBL'))
        if ratio is None:
            continue
        stocks.append({
            'code': item.get('GPDM', ''),
            'name': item.get('GPJC', ''),
            'ratio': ratio,
            'sector': item.get('INDEXNAME', ''),
            'change_type': item.get('PCTNVCHGTYPE', ''),
        })

    bonds = []
    for item in d.get('fundboods', []) or []:
        ratio = _num(item.get('ZJZBL'))
        if ratio is None:
            continue
        bonds.append({
            'code': item.get('ZQDM', ''),
            'name': item.get('ZQMC', ''),
            'ratio': ratio,
        })

    fofs = []
    for item in d.get('fundfofs', []) or []:
        ratio = _num(item.get('JZBL'))
        if ratio is None:
            continue
        fofs.append({
            'code': item.get('FCODE', ''),
            'name': item.get('SHORTNAME', ''),
            'ratio': ratio,
        })

    # 按占净值比降序
    stocks.sort(key=lambda x: x['ratio'], reverse=True)
    bonds.sort(key=lambda x: x['ratio'], reverse=True)
    fofs.sort(key=lambda x: x['ratio'], reverse=True)
    return {'stocks': stocks, 'bonds': bonds, 'fofs': fofs}


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


def _fmt_scale(v):
    """将规模数值格式化为亿元。"""
    if v is None:
        return None
    try:
        return round(float(v) / 100000000, 2)
    except (ValueError, TypeError):
        return None


def load_september_focus():
    """从 september_focus.json 读取9月重点资产名单"""
    path = os.path.join(BASE_DIR, 'september_focus.json')
    if not os.path.exists(path):
        print('  september_focus.json 不存在，跳过9月重点标记')
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_base(name):
    """归一化基金名称用于模糊匹配"""
    n = name.replace(' ', '').replace('证券投资基金', '').replace('型', '')
    n = re.sub(r'[A-FC]+(?:类|份额)?$', '', n)
    return n


def get_share_class(name):
    """提取份额类别（A/B/C/D/E等）"""
    m = re.search(r'([A-FC]+)(?:类|份额)?$', name.replace(' ', ''))
    return m.group(1) if m else 'A'


def mark_september_focus(combined, sep_names):
    """标记9月重点资产"""
    if not sep_names:
        return combined
    matched_count = 0
    for fund in combined:
        fb = normalize_base(fund['name'])
        fsc = get_share_class(fund['name'])
        matched = False
        for sn in sep_names:
            sb = normalize_base(sn)
            ssc = get_share_class(sn)
            if (fb in sb or sb in fb) and fsc == ssc:
                matched = True
                break
        fund['is_september_focus'] = matched
        if matched:
            matched_count += 1
    print(f'  9月重点资产标记: {matched_count}/{len(combined)}')
    return combined


def main():
    print(f'[{datetime.now()}] 开始更新基金数据...')

    # 从 fund_list.json 读取基金代码和分类
    with open(os.path.join(BASE_DIR, 'fund_list.json'), 'r', encoding='utf-8') as f:
        fund_list = json.load(f)

    codes = [f['code'] for f in fund_list]
    fund_info = {f['code']: f for f in fund_list}
    print(f'基金数量: {len(codes)}')

    # 加载静态数据（晨星评级、赛道、投资风格、优缺点、持仓）
    def _load_json(filename):
        path = os.path.join(BASE_DIR, filename)
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    extra_data = _load_json('fund_extra_data.json')
    profiles = _load_json('fund_profiles.json')
    holdings_data = _load_json('fund_holdings.json')

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

    # 抓取基金规模与买卖规则
    print('抓取基金规模与买卖规则...')
    basic_data = {}
    basic_failed = []
    for i, code in enumerate(codes):
        elapsed = time.time() - last_req[0]
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        last_req[0] = time.time()

        bi = fetch_fund_basic_info(code)
        if bi is not None:
            basic_data[code] = bi
        else:
            basic_failed.append(code)

        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'  规模规则进度: {i + 1}/{len(codes)}')

    print(f'规模规则成功: {len(basic_data)}/{len(codes)}, 失败: {len(basic_failed)}')

    # 抓取最新持仓（股票+债券）
    print('抓取最新持仓数据...')
    holdings_data_live = {}
    holdings_failed = []
    for i, code in enumerate(codes):
        elapsed = time.time() - last_req[0]
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        last_req[0] = time.time()

        hd = fetch_fund_holdings(code)
        if hd is not None:
            holdings_data_live[code] = hd
        else:
            holdings_failed.append(code)

        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'  持仓进度: {i + 1}/{len(codes)}')

    print(f'持仓成功: {len(holdings_data_live)}/{len(codes)}, 失败: {len(holdings_failed)}')

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
        e = extra_data.get(code, {})
        profile = profiles.get(code, {})
        # 优先使用实时抓取的持仓；失败时回退到静态 fund_holdings.json
        h = holdings_data_live.get(code, holdings_data.get(code, {}))
        bi = basic_data.get(code, {})

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
            'investment_style': profile.get('investment_style', ''),
            'pros': profile.get('pros', []),
            'cons': profile.get('cons', []),
            'sector': h.get('sector', e.get('sector', '')),
            'morningstar_3y': e.get('morningstar_3y'),
            'morningstar_5y': e.get('morningstar_5y'),
            'holdings': h.get('holdings', []),
            'holding_type': h.get('holding_type', ''),
            'stocks': h.get('stocks', []),
            'bonds': h.get('bonds', []),
            'fofs': h.get('fofs', []),
            'scale': _fmt_scale(bi.get('scale')),
            'purchase_status': bi.get('purchase_status', ''),
            'redeem_status': bi.get('redeem_status', ''),
            'min_purchase': bi.get('min_purchase'),
            'max_purchase': bi.get('max_purchase'),
            'source_rate': bi.get('source_rate', ''),
            'actual_rate': bi.get('actual_rate', ''),
            'fund_type': bi.get('fund_type', ''),
            'risk_level': bi.get('risk_level', ''),
            'is_september_focus': False,
        })

    cat2s = sorted(set(f['category2'] for f in combined if f['category2']))
    cat3s = sorted(set(f['category3'] for f in combined if f['category3']))
    categories = {'level2': cat2s, 'level3': cat3s}

    # 标记9月重点资产
    print('标记9月重点资产...')
    sep_names = load_september_focus()
    combined = mark_september_focus(combined, sep_names)

    # 写入主数据（不含 nav_history，减少文件体积加速加载）
    data = {'funds': combined, 'categories': categories}
    json_path = os.path.join(BASE_DIR, 'web_fund_data.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    # 写入净值历史独立文件（详情页按需加载）
    nav_history_map = {}
    for code in codes:
        if code in nav_data:
            records = sorted(nav_data[code]['records'], key=lambda x: x['date'])
            if records:
                nav_history_map[code] = [[r['date'], r['nav']] for r in records]
    nav_path = os.path.join(BASE_DIR, 'fund_nav_history.json')
    with open(nav_path, 'w', encoding='utf-8') as f:
        json.dump(nav_history_map, f, ensure_ascii=False)

    print(f'生成主数据: {json_path}')
    print(f'生成净值历史: {nav_path}')
    print(f'[{datetime.now()}] 更新完成，共 {len(combined)} 只基金')


if __name__ == '__main__':
    main()
