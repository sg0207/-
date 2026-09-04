#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py — GitHub Actions 专用全量数据抓取脚本

Step 1: fetch_extra_data()  — 并行抓取收益/赛道/晨星评级 -> fund_extra_data.json
Step 2: fetch_holdings()    — 抓取前十大持仓                     -> fund_holdings.json
Step 3: generate_profiles() — 根据模板生成投资风格/优缺点       -> fund_profiles.json
Step 4: merge_all_data()    — 合并全部数据 + NAV + 阶段涨幅     -> web_fund_data.json

仅使用 Python 标准库，路径均基于 BASE_DIR 相对路径。
"""

import json, os, ssl, urllib.request, time, re, random, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

ssl._create_default_https_context = ssl._create_unverified_context

# ── 常量 ──────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MIN_INTERVAL = 0.4
RETRY_TIMES = 3
RETRY_BACKOFF_BASE = 1.5
MAX_WORKERS = 8

_MOBILE_UA = 'Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 Mobile Safari/537.36'
_MOBILE_REFERER = 'https://fundmobapi.eastmoney.com/'
_DESKTOP_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
_DESKTOP_REFERER = 'https://fund.eastmoney.com/'

# ── 全局速率锁 ─────────────────────────────────────────────────────

_rate_lock = threading.Lock()
_last_req_time = [0.0]


def _rate_limit():
    """全局限速：相邻请求间隔至少 MIN_INTERVAL 秒。"""
    with _rate_lock:
        elapsed = time.time() - _last_req_time[0]
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        _last_req_time[0] = time.time()


# ── HTTP 工具 ─────────────────────────────────────────────────────

def _http_get(url, ua=None, referer=None, timeout=30):
    """带重试的 GET，返回 response text。"""
    if ua is None:
        ua = _MOBILE_UA
    if referer is None:
        referer = _MOBILE_REFERER
    headers = {
        'User-Agent': ua,
        'Referer': referer,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
    }
    backoff = 0
    for attempt in range(RETRY_TIMES):
        try:
            _rate_limit()
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8', errors='replace')
        except Exception as exc:
            backoff = RETRY_BACKOFF_BASE ** (attempt + 1)
            if attempt < RETRY_TIMES - 1:
                time.sleep(backoff)
    return None


def _http_get_json(url, ua=None, referer=None):
    """GET + json.loads，失败返回 None。"""
    text = _http_get(url, ua=ua, referer=referer)
    if text is None:
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


# ── Step 1: fetch_extra_data ─────────────────────────────────────

# 基金管理人前缀 → 公司简称映射（按长度降序排列，避免短前缀误匹配）
COMPANY_PREFIXES = sorted([
    '华泰柏瑞', '汇添富', '易方达', '富国', '博时', '大成', '华夏', '南方', '中欧',
], key=len, reverse=True)


def _extract_company(name):
    """从基金名称前缀提取基金公司简称。"""
    for prefix in COMPANY_PREFIXES:
        if name.startswith(prefix):
            return prefix
    return ''


# ── pingzhongdata.js 收益解析 ────────────────────────────────────

_SYL_MAP = {
    'syl_3y': '3m',   # 近3月
    'syl_6y': '6m',   # 近6月
    'syl_1n': '1y',   # 近1年
    'syl_1y': '1m',   # 近1月
}


def _parse_pingzhong_returns(text):
    """从 pingzhongdata.js 内容解析收益。返回 {3m, 6m, 1y, 1m}。"""
    result = {}
    # 解析 syl_* 变量（双引号包裹的字符串）
    for var_name, field in _SYL_MAP.items():
        pat = re.compile(r'var\s+' + re.escape(var_name) + r'\s*=\s*"([^"]*)"\s*;')
        m = pat.search(text)
        if m:
            try:
                result[field] = float(m.group(1))
            except (ValueError, TypeError):
                pass
    return result


def _calc_3y_return(text):
    """从 Data_netWorthTrend 计算近3年收益。"""
    arr_pattern = re.compile(r'var\s+Data_netWorthTrend\s*=\s*\[(.+?)\]\s*;')
    m = arr_pattern.search(text)
    if not m:
        return None
    arr_str = m.group(1)
    items = re.findall(r'\{x:\s*(\d+)[,\s]+y:\s*([\d.]+)', arr_str)
    if len(items) < 2:
        return None
    # 转换为正序列表 (date_ms, nav)
    records = []
    for x_ms, y_nav in items:
        records.append((int(x_ms), float(y_nav)))
    records.sort(key=lambda r: r[0])

    latest_ms = records[-1][0]
    three_years_ago_ms = latest_ms - 3 * 365.25 * 24 * 3600 * 1000
    # 找三年前的NAV
    nav_3y_ago = None
    nav_latest = records[-1][1]
    for ms, nav in records:
        if ms <= three_years_ago_ms:
            nav_3y_ago = nav
    if nav_3y_ago is None or nav_3y_ago <= 0 or nav_latest <= 0:
        return None
    return round((nav_latest / nav_3y_ago - 1) * 100, 2)


# ── 晨星评级解析 ──────────────────────────────────────────────────

def _parse_morningstar(html):
    """从基金详情页提取晨星评级，返回 {3y, 5y} 或 None。"""
    result = {}
    # 匹配"晨星评级"后的星级，例如 ★★★★★ 或 ★★★★☆
    pattern = re.compile(r'晨星评级[^★]*([★☆·]{5})')
    for m in pattern.finditer(html):
        stars_str = m.group(1)
        count = stars_str.count('★')
        # 尝试匹配是哪一年级的评级
        # 先往前找年份标记
        ctx_start = max(0, m.start() - 200)
        ctx = html[ctx_start:m.start()]
        year_3y = re.search(r'(3[^0-9]*年)', ctx)
        year_5y = re.search(r'(5[^0-9]*年)', ctx)
        if year_3y and '3' in ctx:
            result['3y'] = count
        elif year_5y and '5' in ctx:
            result['5y'] = count
        elif not result:
            # 默认第一个匹配为3年评级
            result['3y'] = count
        # 只取前两个评级
        if len(result) >= 2:
            break
    if not result:
        return None
    return result


# ── howbuy 赛道解析 ───────────────────────────────────────────────

def _parse_howbuy_sector(html):
    """从 howbuy JS 提取行业配置（top 3 行业）。返回行业字符串或 None。"""
    # hyPieData = { ... 日期 {行业: 百分比, ...}, ... }
    # 尝试解析为 JS 对象
    pat = re.compile(r'var\s+hyPieData\s*=\s*(\{.*?\})\s*;')
    m = pat.search(html)
    if not m:
        return None
    obj_str = m.group(1)

    # 提取所有带日期的行业配置
    # 格式: "2024Q1": {"行业A": 12.3, "行业B": 8.7}
    dated_pattern = re.compile(
        r'["\'](\d{4}[Qq]\d|"\d{4}-\d{2}-\d{2}"|\d{4}-\d{2})["\']'
        r'\s*:\s*\{([^}]*)\}'
    )
    all_industries = {}
    for dm in dated_pattern.finditer(obj_str):
        period_key = dm.group(1).strip('"\' ')
        inner = dm.group(2)
        # 解析行业: "行业名": 比例
        entries = re.findall(r'["\']([^"\']+)["\']\s*:\s*([\d.]+)', inner)
        if entries:
            total = sum(float(v) for _, v in entries)
            if total > 0:
                sorted_entries = sorted(entries, key=lambda e: float(e[1]), reverse=True)
                top = [e[0] for e in sorted_entries[:3]]
                all_industries[period_key] = top

    if not all_industries:
        return None
    # 取最新季度的行业
    latest_key = max(all_industries.keys())
    return '、'.join(all_industries[latest_key])


def _infer_sector_from_name_and_type(name, cat1, cat2, cat3):
    """当 howbuy API 失败时，根据基金名称和分类推断行业/赛道。"""
    # 关键词 → 赛道映射
    SECTOR_HINTS = {
        # 科技/TMT
        '科技': 'TMT、科技', '半导体': '半导体', '芯片': '半导体',
        '信息': 'TMT、科技', '互联网': 'TMT', '数字经济': '数字经济、TMT',
        '人工智能': '人工智能、TMT', '5G': '通信、TMT', '科创板': '科创板、科技',
        '制造': '先进制造、新能源', '环保': '环保、新能源', '新能源': '新能源',
        '医药': '医药、医疗健康', '医疗保健': '医药、医疗健康',
        '消费': '消费、食品', '品牌': '消费',
        '金融': '金融、银行', '红利': '红利、金融',
        '量化': '量化', '指数': '指数',
        '黄金': '黄金、大宗商品',
        '港股': '港股', '沪深港': '港股、A股',
        # 固收+
        '固收': '债券、固收+', '债券': '债券',
        'FOF': 'FOF、基金',
    }

    sector_parts = []

    # 从名称关键词匹配
    for keyword, sector in SECTOR_HINTS.items():
        if keyword in name:
            sector_parts.append(sector)
            break

    # 从分类补充
    if cat2 == '被动权益':
        if cat3 == '指数增强':
            sector_parts.append('指数增强')
        elif cat3 == '宽基':
            sector_parts.append('宽基指数')
        elif cat3 == '行业':
            # 从名称提取行业
            industry_keywords = ['半导体', '芯片', '医疗', '消费', '金融', '能源', '科技', '通信']
            matched = [kw for kw in industry_keywords if kw in name]
            sector_parts.append(matched[0] if matched else '行业指数')
        elif cat3 == '黄金':
            sector_parts.append('黄金')
    elif cat2 == '主动权益':
        if cat3 in ('科技', '消费', '医药', '红利', '价值', '新能源', '制造'):
            sector_parts.append(cat3)
        elif cat3 == '泛周期':
            sector_parts.append('周期、原材料')
        elif cat3 in ('均衡', '权益低波'):
            sector_parts.append('均衡配置')
        elif cat3 == '成长':
            sector_parts.append('成长风格')
    elif cat2 == '固收+':
        if cat3 == '低波固收+':
            sector_parts.append('中短债、低风险')
        elif cat3 == '中波固收+':
            sector_parts.append('中高等级信用债、可转债')
        elif cat3 == '高波固收+':
            sector_parts.append('可转债、偏债混合')
        if 'FOF' in name:
            sector_parts.append('FOF')

    # 去重 + 拼接
    seen = set()
    unique = []
    for s in sector_parts:
        for part in s.split('、'):
            part = part.strip()
            if part and part not in seen:
                seen.add(part)
                unique.append(part)

    return '、'.join(unique) if unique else ''


# ── 单基金额外数据抓取 ────────────────────────────────────────────

def _fetch_extra_for_one(code, name, cat1, cat2, cat3):
    """抓取一只基金的额外数据（收益、赛道、晨星评级）。"""
    extra = {}

    # 1) 收益 — pingzhongdata.js
    pz_url = f'https://fund.eastmoney.com/pingzhongdata/{code}.js'
    pz_text = _http_get(pz_url, ua=_DESKTOP_UA, referer=_DESKTOP_REFERER)
    returns_eastmoney = {}
    if pz_text:
        returns_eastmoney = _parse_pingzhong_returns(pz_text)
        # 3y 从 Data_netWorthTrend 计算
        if '3y' not in returns_eastmoney:
            three_y = _calc_3y_return(pz_text)
            if three_y is not None:
                returns_eastmoney['3y'] = three_y
    extra['returns_eastmoney'] = returns_eastmoney if returns_eastmoney else None

    # 2) 赛道 — howbuy JS
    sector = ''
    howbuy_url = f'https://static.howbuy.com/upload/auto/script/fund/data_{code}.js'
    howbuy_text = _http_get(howbuy_url, ua=_DESKTOP_UA, referer=_DESKTOP_REFERER)
    if howbuy_text:
        sector = _parse_howbuy_sector(howbuy_text)
    if not sector:
        sector = _infer_sector_from_name_and_type(name, cat1, cat2, cat3)
    extra['sector'] = sector

    # 3) 晨星评级 — 基金详情页
    ms_url = f'https://fund.eastmoney.com/{code}.html'
    ms_html = _http_get(ms_url, ua=_DESKTOP_UA, referer=_DESKTOP_REFERER)
    if ms_html:
        ms_data = _parse_morningstar(ms_html)
        if ms_data:
            extra['morningstar_3y'] = ms_data.get('3y')
            extra['morningstar_5y'] = ms_data.get('5y')
        else:
            extra['morningstar_3y'] = None
            extra['morningstar_5y'] = None
    else:
        extra['morningstar_3y'] = None
        extra['morningstar_5y'] = None

    return extra


def fetch_extra_data(fund_list):
    """Step 1: 并行抓取所有基金的额外数据。返回 {code: extra}。"""
    print('Step 1: 抓取额外数据（收益/赛道/晨星评级）...')
    results = {}
    codes = [f['code'] for f in fund_list]
    fund_map = {f['code']: f for f in fund_list}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut_to_code = {}
        for fund in fund_list:
            fut = pool.submit(
                _fetch_extra_for_one,
                fund['code'], fund['name'],
                fund.get('category1', ''),
                fund.get('category2', ''),
                fund.get('category3', ''),
            )
            fut_to_code[fut] = fund['code']

        done = 0
        for fut in as_completed(fut_to_code):
            code = fut_to_code[fut]
            done += 1
            try:
                extra = fut.result()
                results[code] = extra
                if done % 10 == 0 or done == len(codes):
                    print(f'  进度: {done}/{len(codes)}')
            except Exception as exc:
                print(f'  抓取 {code} 失败: {exc}')
                results[code] = {
                    'returns_eastmoney': None,
                    'sector': '',
                    'morningstar_3y': None,
                    'morningstar_5y': None,
                }

    out_path = os.path.join(BASE_DIR, 'fund_extra_data.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)
    print(f'  已写入 {out_path}')
    return results


# ── 基金基础信息与实时持仓（规模、买卖规则、股票+债券） ─────────────

def fetch_fund_basic_info(code):
    """拉取基金基础信息：规模、申赎状态、费率、限额等。"""
    # 基础信息（规模、原始/实际费率等）
    url1 = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNBasicInformation?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    data1 = _http_get_json(url1)
    if not isinstance(data1, dict):
        return None
    d1 = data1.get('Datas')
    if not isinstance(d1, dict):
        return None

    # 完整费率信息（申购分档 + 赎回费率）
    url2 = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNRateInfo?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    try:
        data2 = _http_get_json(url2)
    except Exception:
        data2 = None
    d2 = data2.get('Datas') if isinstance(data2, dict) else None

    def _parse_rate_list(items):
        return [{'money': i.get('money', ''), 'time': i.get('time', ''), 'rate': i.get('rate', '')} for i in items if i.get('rate')]

    purchase_rate_tiers = _parse_rate_list(d2.get('rg', [])) if isinstance(d2, dict) else []
    redeem_rate_tiers = _parse_rate_list(d2.get('sh', [])) if isinstance(d2, dict) else []

    return {
        'scale': d1.get('ENDNAV') or d1.get('FEGM') or None,
        'purchase_status': d1.get('SGZT', ''),
        'redeem_status': d1.get('SHZT', ''),
        'min_purchase': d1.get('MINSG'),
        'max_purchase': d1.get('MAXSG'),
        'source_rate': d1.get('SOURCERATE', ''),
        'actual_rate': d1.get('RATE', ''),
        'purchase_rate_tiers': purchase_rate_tiers,
        'redeem_rate_tiers': redeem_rate_tiers,
        'fund_type': d1.get('FTYPE', ''),
        'risk_level': d1.get('RISKLEVEL', ''),
    }


def fetch_fund_holdings_live(code):
    """拉取基金最新持仓（股票 fundStocks + 债券 fundboods + FOF fundfofs）。
    返回 {stocks:[...], bonds:[...], fofs:[...]} 或 None。
    """
    url = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNInverstPosition?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    data = _http_get_json(url)
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

    stocks.sort(key=lambda x: x['ratio'], reverse=True)
    bonds.sort(key=lambda x: x['ratio'], reverse=True)
    fofs.sort(key=lambda x: x['ratio'], reverse=True)
    return {'stocks': stocks, 'bonds': bonds, 'fofs': fofs}


# ── Step 2: fetch_holdings ────────────────────────────────────────

def _parse_holdings_from_jsonp(text):
    """从 FundArchivesDatas.aspx JSONP 响应解析前十大持仓。"""
    idx_open = text.find('content:"')
    if idx_open < 0:
        return None, ''
    val_start = idx_open + len('content:"')
    close_marker = '",arryear'
    idx_close = text.find(close_marker, val_start)
    if idx_close < 0:
        return None, ''

    content = text[val_start:idx_close]
    if not content.strip():
        return [], ''

    # 解析 HTML 表格：<tr>...<td class='tol'><a>name</a></td>...<td class='tor'>X.XX%</td></tr>
    holdings = []
    tr_pattern = re.compile(r'<tr\b[^>]*>(.*?)</tr>', re.DOTALL)
    for tr_match in tr_pattern.finditer(content):
        tr_body = tr_match.group(1)
        # 跳过表头行
        if '<th' in tr_body:
            continue
        # 股票名: <td class='tol'> 或 <td ... tol ...>
        name_m = re.search(
            r"<td\b[^>]*class=['\"]?[^'\"]*tol[^'\"]*['\"]?[^>]*>(.*?)</td>",
            tr_body, re.DOTALL,
        )
        if not name_m:
            continue
        name = re.sub(r'<[^>]*>', '', name_m.group(1)).strip()
        if not name:
            continue
        # 比例: <td class='tor'>X.XX%</td>
        ratio_m = re.search(
            r"<td\b[^>]*class=['\"]?[^'\"]*tor[^'\"]*['\"]?[^>]*>(.*?)</td>",
            tr_body, re.DOTALL,
        )
        if not ratio_m:
            continue
        ratio_str = re.sub(r'<[^>]*>', '', ratio_m.group(1)).strip()
        pct_m = re.search(r'([\d.]+)\s*%', ratio_str)
        if not pct_m:
            continue
        try:
            ratio = round(float(pct_m.group(1)), 2)
        except ValueError:
            continue
        holdings.append({'name': name, 'ratio': ratio})

    return holdings, content


def _fetch_holding_type(code):
    """通过 FundMNAssetAllocation API 判断持仓类型（stock / bond / mixed）。"""
    url = (
        'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNAssetAllocation?'
        f'FCODE={code}&deviceid=test&plat=Android&product=EFund&version=4.5'
    )
    data = _http_get_json(url)
    if not data or not data.get('Datas') or not data['Datas']:
        return 'stock'  # 默认
    # 取最新一期（Datas 按时间倒序）
    latest = data['Datas'][0]

    def safe_float(v):
        if v is None or v == '' or v == '--':
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    gp = safe_float(latest.get('GP'))
    zq = safe_float(latest.get('ZQ'))
    hb = safe_float(latest.get('HB'))

    # GP > ZQ → 股票型; ZQ > GP → 债券型; 否则混合型
    if gp > zq:
        return 'stock'
    elif zq > gp:
        return 'bond'
    elif hb > gp and hb > zq:
        return 'bond'  # 混合基金中偏债
    else:
        return 'stock'


def fetch_holdings(fund_list):
    """Step 2: 抓取所有基金的最新持仓（股票+债券+FOF）。返回 {code: holdings_data}。"""
    print('Step 2: 抓取最新持仓（股票+债券+FOF）...')
    results = {}
    codes = [f['code'] for f in fund_list]

    for i, fund in enumerate(fund_list):
        code = fund['code']

        # 优先使用实时持仓接口（股票+债券+FOF）
        live = fetch_fund_holdings_live(code)
        if live is not None and (live['stocks'] or live['bonds'] or live['fofs']):
            holdings = [{'name': s['name'], 'ratio': s['ratio']} for s in live['stocks']]
            holding_type = 'stock' if live['stocks'] else ('bond' if live['bonds'] else 'fof')
            results[code] = {
                'holdings': holdings,
                'holding_type': holding_type,
                'sector': '',  # placeholder, filled in Step 4
                'stocks': live['stocks'],
                'bonds': live['bonds'],
                'fofs': live['fofs'],
            }
        else:
            # 回退：旧接口拉股票持仓 + AssetAllocation 判断类型
            rt = random.random()
            url = (
                f'https://fundf10.eastmoney.com/FundArchivesDatas.aspx?'
                f'code={code}&type=jjcc&topline=10&year=0&month=0&rt={rt}'
            )
            text = _http_get(url, ua=_DESKTOP_UA, referer=f'https://fundf10.eastmoney.com/')
            holdings, content = [], ''
            if text:
                holdings, content = _parse_holdings_from_jsonp(text)
                if holdings is None:
                    holdings = []
            holding_type = _fetch_holding_type(code)
            results[code] = {
                'holdings': holdings,
                'holding_type': holding_type,
                'sector': '',
                'stocks': holdings,
                'bonds': [],
                'fofs': [],
            }

        if (i + 1) % 10 == 0 or (i + 1) == len(codes):
            s = results[code]
            n_stock = len(s['stocks'])
            n_bond = len(s['bonds'])
            n_fof = len(s['fofs'])
            print(f'  进度: {i + 1}/{len(codes)}, {code}: 股票{n_stock} 债券{n_bond} FOF{n_fof}')

    out_path = os.path.join(BASE_DIR, 'fund_holdings.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)
    print(f'  已写入 {out_path}')
    return results


# ── Step 3: generate_profiles ─────────────────────────────────────

# ── 持有期/份额提取 ───────────────────────────────────────────────

_PERIOD_MAP = {
    '九个月': '九个月持有期',
    '6个月': '六个月持有期',
    '三个月': '三个月持有期',
    '12个月': '一年持有期',
    '一年': '一年持有期',
    '5个月': '五个月持有期',
    '15个月': '十五个月持有期',
    '二年': '两年持有期',
    '两年': '两年持有期',
    '三年': '三年持有期',
    '定开': '定期开放',
}


def holding_period(name):
    """从基金名提取持有期描述，返回如 '九个月持有期'。"""
    name = name.replace('(', '（').replace(')', '）')
    for ch, full in _PERIOD_MAP.items():
        if ch in name:
            return full
    # 匹配数字+月/年
    m = re.search(r'(\d+)\s*(个月|月|年)', name)
    if m:
        num, unit = m.group(1), m.group(2)
        if unit == '月' or unit == '个月':
            return f'{num}个月持有期'
        if unit == '年':
            # 避免把"年华"等误匹配
            if not re.search(r'[酸碱融]', name[max(0, m.start()-3):m.start()]):
                return f'{num}年持有期'
    return ''


def share_class(name):
    """提取份额类别 A/B/C/D/E。"""
    m = re.search(r'([A-FC]+)(?:类|份额)?$', name.replace(' ', ''))
    return m.group(1) if m else ''


def pick(pool, n=3, seed=None):
    """随机选 n 个不重复项。"""
    if seed is not None:
        random.seed(seed)
    shuffled = list(pool)
    random.shuffle(shuffled)
    return shuffled[:min(n, len(shuffled))]


def ensure_length(style, extra=''):
    """确保 style 在 100-200 字符之间。"""
    s = style + extra
    if len(s) < 100:
        s = s + '。' + extra
    if len(s) > 250:
        s = s[:245] + '...'
    return s


# ── 指数名映射 ────────────────────────────────────────────────────

_INDEX_MAP = {
    '中证1000': '中证1000',
    '中证500': '中证500',
    '沪深300': '沪深300',
    '创业板指': '创业板指',
    '上证科创板50': '科创50',
    '上证科创板综合': '科创综合',
    '中证A500': '中证A500',
    '上证50': '上证50',
    '国证2000': '国证2000',
    '中证红利': '中证红利',
    '中证医疗': '中证医疗',
    '中证消费': '中证消费',
    '中证半导体': '中证半导体',
    '中证人工智能': '中证人工智能',
    '中证新能源': '中证新能源',
    '中证军工': '中证军工',
    '中证生物科技': '中证生物科技',
    '中证酒': '中证酒',
    '中证沪港深黄金': '沪深港黄金',
    '国证半导体芯片': '半导体芯片',
    '科创板半导体材料设备': '科创板半导体材料设备',
    '中证军工': '军工',
    '中证5G': '5G通信',
}


def passive_sector(name):
    """从基金名提取被动指数的标的。"""
    for keyword, label in sorted(_INDEX_MAP.items(), key=lambda x: -len(x[0])):
        if keyword in name:
            return label
    return ''


# ── 风格描述模板 ──────────────────────────────────────────────────

_STYLE_TEMPLATES = {
    # 固收+
    '固收+': [
        '{company}管理的中波{label}（{kw}），以债券资产为核心，辅以适度股票、可转债等权益敞口，{period}，鼓励长期持有，强调风险调整后收益的稳定性。 本{share}由{company}管理（{kw}），通过债券打底叠加权益增强，希望在稳健基础上争取优于纯债的风险调整后收益。',
        '{company}管理的低波{label}（{kw}），以高等级信用债和利率债为底仓，严格控制久期和信用风险，{period}，追求本金安全基础上的稳健增值。 本{share}由{company}管理（{kw}），注重安全边际，在利率下行周期中表现出色。',
        '{company}管理的高波{label}（{kw}），在债券底仓之上积极运用可转债、信用下沉等策略增厚收益，{period}，追求超越基准的绝对收益。 本{share}由{company}管理（{kw}），权益仓位灵活调整，在股债跷跷板中捕捉套利机会。',
    ],
    # 主动权益 — 均衡
    '均衡': [
        '{company}旗下{share}为均衡风格主动权益基金（{kw}），不押注单一赛道，通过分散配置力争平滑组合波动，{period}，适合稳健型权益投资者。 本{share}由{company}管理（{kw}），在行业与风格配置上相对分散，力求在成长与价值、进攻与防守之间取得平衡。',
        '{company}旗下{share}为均衡风格主动权益基金，{period}，坚持价值与成长并重的投资理念，注重企业盈利质量和估值合理性。 本{company}{kw}基金通过深入研究驱动选股，在控制回撤的同时追求超额收益。',
    ],
    # 主动权益 — 科技
    '科技': [
        '{company}旗下{share}为科技风格主动权益基金（{kw}），重点布局半导体、人工智能、数字经济等赛道，{period}，追求科技创新带来的高成长回报。 本{share}由{company}管理（{kw}），深度挖掘 TMT 产业链中的优质标的，注重产业趋势与估值匹配。',
        '{company}旗下{share}聚焦科技创新主线，覆盖消费电子、半导体设计、AI 算力等细分领域，{period}。 本{company}{kw}基金通过产业链纵向深耕，在科技行业高弹性中捕捉结构性机会。',
    ],
    # 主动权益 — 消费
    '消费': [
        '{company}旗下{share}为消费风格主动权益基金（{kw}），重仓食品饮料、家电、医药生物等必选及可选消费板块，{period}，分享消费升级红利。 本{company}{kw}基金注重品牌壁垒和现金流质量，在消费复苏周期中表现稳健。',
    ],
    # 主动权益 — 医药
    '医药': [
        '{company}旗下{share}为医药风格主动权益基金（{kw}），聚焦创新药、医疗器械、CXO 等细分赛道，{period}，把握人口老龄化和医疗升级的长期趋势。 本{company}{kw}基金通过医药产业深度研究，在细分领域挖掘Alpha收益。',
    ],
    # 主动权益 — 红利
    '红利': [
        '{company}旗下{share}为红利风格主动权益基金（{kw}），重点配置高股息、低波动的价值型标的，{period}，注重股息回报和下行保护。 本{company}{kw}基金在震荡市中通过高股息策略提供相对稳定的收益来源。',
    ],
    # 主动权益 — 价值
    '价值': [
        '{company}旗下{share}为价值风格主动权益基金（{kw}），坚守深度价值投资框架，关注低估值、高安全边际的标的，{period}。 本{company}{kw}基金以量价指标和财务健康度为筛选标准，在价值回归过程中获取超额收益。',
    ],
    # 主动权益 — 成长
    '成长': [
        '{company}旗下{share}为成长风格主动权益基金（{kw}），聚焦高ROE、高营收增速的优质企业，{period}，追求长期资本增值。 本{company}{kw}基金通过商业模式分析和行业景气度跟踪，在成长赛道中挖掘十倍股潜力。',
    ],
    # 主动权益 — 泛周期
    '泛周期': [
        '{company}旗下{share}为泛周期风格主动权益基金（{kw}），覆盖有色金属、煤炭、化工等顺周期板块，{period}，把握经济复苏期的周期弹性。 本{company}{kw}基金紧密跟踪宏观周期信号，在通胀交易和复苏交易中获取收益。',
    ],
    # 主动权益 — 权益低波
    '权益低波': [
        '{company}旗下{share}为低波风格主动权益基金（{kw}），在权益仓位内优选低波动、高分红的防御型标的，{period}，追求最大回撤可控的绝对收益。 本{company}{kw}基金注重组合稳定性，在熊市中跌幅较小，在牛市中也能跟上基准。',
    ],
    # 被动权益 — 指数增强
    '被动权益_指数增强': [
        '{company}旗下{share}为指数增强型基金，跟踪{index}，通过量化模型在控制跟踪误差的前提下追求超额收益，{period}。 本{company}{kw}基金采用多因子模型，在指数复制基础上进行Smart Beta优化。',
    ],
    # 被动权益 — 宽基
    '被动权益_宽基': [
        '{company}旗下{share}为宽基指数基金，跟踪{index}，完整复制指数成分，费用低廉，流动性好，{period}。 本{company}产品是布局{index}市场的低成本工具，适合长期定投和资产配置。',
    ],
    # 被动权益 — 行业
    '被动权益_行业': [
        '{company}旗下{share}为行业主题指数基金，跟踪{index}，集中投资于{sector}产业链，{period}。 本{company}产品提供{sector}赛道的一站式配置工具，风险收益特征清晰透明。',
    ],
    # 被动权益 — 黄金
    '被动权益_黄金': [
        '{company}旗下{share}为黄金主题ETF联接基金，跟踪现货黄金价格波动，提供黄金资产配置工具，{period}。 本{company}产品是规避通胀风险和分散投资组合的有效工具。',
    ],
}

_KW_MAP = {
    '均衡': '研究精选',
    '科技': '科技成长',
    '消费': '消费升级',
    '医药': '医药健康',
    '红利': '高股息红利',
    '价值': '深度价值',
    '成长': '成长优选',
    '泛周期': '周期优选',
    '权益低波': '低波稳健',
}


def generate_style(f):
    """根据基金分类生成投资风格描述。"""
    cat2 = f.get('cat2', '')
    cat3 = f.get('cat3', '')
    name = f.get('name', '')
    company = f.get('company', _extract_company(name))
    share = share_class(name) or 'A'

    period = holding_period(name)
    period_desc = ''
    if period:
        period_desc = f'设有{period}，'

    fof = '（FOF）' in name

    # 被动权益特殊处理
    if cat2 == '被动权益':
        index_name = passive_sector(name) or ''
        sector_name = index_name
        if cat3 == '指数增强':
            tpl = _STYLE_TEMPLATES.get('被动权益_指数增强', _STYLE_TEMPLATES['被动权益_指数增强'])
        elif cat3 == '宽基':
            tpl = _STYLE_TEMPLATES.get('被动权益_宽基', _STYLE_TEMPLATES['被动权益_宽基'])
        elif cat3 == '行业':
            tpl = _STYLE_TEMPLATES.get('被动权益_行业', _STYLE_TEMPLATES['被动权益_行业'])
        elif cat3 == '黄金':
            tpl = _STYLE_TEMPLATES.get('被动权益_黄金', _STYLE_TEMPLATES['被动权益_黄金'])
        else:
            tpl = _STYLE_TEMPLATES['被动权益_宽基']

        kw = _KW_MAP.get(cat3, cat3) if cat3 in _KW_MAP else ''
        extra_kw = f'（{kw}）' if kw else ''
        idx_str = f'，标的为{index_name}' if index_name else ''
        opts = [
            tpl[0].format(
                company=company, share=share, kw=kw, period=period_desc,
                index=index_name, sector=sector_name,
            ),
        ]
        return ensure_length(random.choice(opts))

    # 固收+
    if cat2 == '固收+':
        cat3_label = cat3
        if '低波' in cat3:
            cat3_label = '低波'
        elif '高波' in cat3:
            cat3_label = '高波'
        else:
            cat3_label = '中波'
        tpl = _STYLE_TEMPLATES['固收+']
        kw = _KW_MAP.get(cat3, cat3) if cat3 in _KW_MAP else cat3
        kw = '固收+' if not kw else kw
        opts = [
            tpl[0].format(company=company, label=cat3_label, kw=kw, period=period_desc, share=share),
            tpl[1].format(company=company, label=cat3_label, kw=kw, period=period_desc, share=share)
            if '低波' in cat3 else tpl[2].format(company=company, label=cat3_label, kw=kw, period=period_desc, share=share),
        ]
        return ensure_length(random.choice(opts))

    # 主动权益
    if cat2 == '主动权益':
        template_key = cat3
        if template_key not in _STYLE_TEMPLATES:
            template_key = '均衡'  # fallback
        kw = _KW_MAP.get(cat3, cat3) if cat3 in _KW_MAP else ''
        extra_kw = f'（{kw}）' if kw else ''
        opts = []
        for tpl in _STYLE_TEMPLATES[template_key]:
            opts.append(tpl.format(
                company=company, share=share, kw=extra_kw.strip('（）') if extra_kw else kw,
                period=period_desc, index='', sector='',
            ))
        if fof:
            opts = [o.replace('主动权益基金', 'FOF基金') for o in opts]
        return ensure_length(random.choice(opts))

    # 默认兜底
    return f'{company}管理的{cat2}基金（{cat3}），通过多元化资产配置追求稳健回报。'


_PROS_POOL = {
    '固收+': [
        '以债券为底仓，收益来源较为多元',
        '股债搭配有助于平滑组合波动',
        '风险控制意识较强，回撤管理较好',
        '适合作为理财替代或稳健底仓配置',
        '持有期设计帮助避免追涨杀跌',
        '在利率下行周期中债券部分表现突出',
        '基金经理债基经验丰富，风控体系成熟',
        '可转债增强策略在牛市中可贡献超额收益',
    ],
    '均衡': [
        '行业和风格配置分散，单一阵营风险低',
        '适应不同市场环境的能力较强',
        '波动相对可控',
        '注重估值安全边际，下行风险有限',
        '基金经理风格稳定，换手率适中',
        '在熊市中跌幅通常小于偏赛道基金',
    ],
    '科技': [
        '聚焦高景气赛道，成长弹性大',
        '基金经理对产业趋势理解深入',
        '科技行业长期增长确定性高',
        '在科技牛市中具备显著超额收益',
        '注重企业核心竞争力和研发投入',
        '组合中结构性阿尔法空间较大',
    ],
    '消费': [
        '消费行业长坡厚雪，确定性高',
        '品牌护城河深，盈利稳定性强',
        '抗通胀能力较好',
        '现金流充沛，分红能力稳定',
        '在经济复苏周期中弹性明显',
        '细分龙头集中度持续提升',
    ],
    '医药': [
        '医药行业刚需属性强，长期空间大',
        '创新药和医疗器械双重驱动',
        '老龄化趋势提供长期需求支撑',
        '政策出清后行业景气度回升',
        '细分赛道龙头具备技术壁垒',
        '海外授权（License-out）带来估值提升',
    ],
    '红利': [
        '高股息提供稳定的现金流回报',
        '低估值提供安全边际',
        '防御属性强，熊市中表现优异',
        '股息再投资复利效应显著',
        '成分股盈利稳定性高',
        '估值修复空间大',
    ],
    '价值': [
        '深度价值投资框架，注重企业内在价值',
        '低估值提供足够安全边际',
        '长期看价值回归确定性高',
        '持仓多具备稳健现金流',
        '在风格切换时具备抗跌优势',
        '基金经理价值投资纪律严明',
    ],
    '成长': [
        '聚焦高成长企业，长期回报潜力大',
        '产业趋势跟踪敏锐，能在早期捕捉龙头',
        '高ROE企业盈利复利效应强',
        '在牛市和成长行情中弹性突出',
        '注重商业模式和竞争壁垒分析',
        '组合中具备十倍股潜力标的',
    ],
    '泛周期': [
        '把握宏观经济周期轮动机会',
        '在复苏和过热阶段弹性显著',
        '资源品价格上行周期中收益丰厚',
        '基金经理宏观研判能力强',
        '行业配置前瞻，切换及时',
        '通胀交易中具备天然优势',
    ],
    '权益低波': [
        '低波动策略在震荡市中表现稳健',
        '最大回撤控制较好，持有体验佳',
        '股息率较高，下行保护强',
        '适合风险偏好较低的权益投资者',
        '长期年化回报与基准接近但波动更低',
        '在市场大幅回调时跌幅明显较小',
    ],
    '被动权益_指数增强': [
        '量化模型追求超越基准的超额收益',
        '费率远低于主动管理基金',
        '跟踪误差可控，指数暴露透明',
        '多因子模型降低单一因子依赖',
        '流动性好，可随时申赎',
        '长期收益稳定跟踪标的指数',
    ],
    '被动权益_宽基': [
        '费用极低，长期持有成本低',
        '完整覆盖指数成分，分散风险',
        '流动性好，适合大额配置',
        '透明度高，持仓完全可查',
        '定投策略的理想工具',
        '无基金经理择时风险',
    ],
    '被动权益_行业': [
        '精准投资于目标行业，贝塔清晰',
        '费率低，跟踪误差小',
        '行业景气度判断明确时收益突出',
        '适合看好特定赛道的投资者',
        '成分股透明，无需研究个股',
        '在市场风格偏向该行业时弹性大',
    ],
    '被动权益_黄金': [
        '黄金避险属性强，对冲系统性风险',
        '与股票、债券相关性低，分散组合风险',
        '抗通胀工具，保护购买力',
        '费用低，流动性好',
        '在地缘政治不确定性上升时表现优异',
        '全球央行持续增持黄金提供价格支撑',
    ],
}

_CONS_POOL = {
    '固收+': [
        '持有期内流动性受限，赎回不够灵活',
        '增强收益依赖基金经理择时选股能力',
        '利率上行阶段债券部分存在回撤压力',
        '权益仓位较小时整体收益弹性有限',
        '信用下沉策略存在违约风险',
    ],
    '均衡': [
        '行业分散导致在单一赛道牛市时跑输',
        '超额收益来源不够聚焦',
        '牛市弹性不如赛道型基金',
        '均衡配置意味着永远不会大幅跑赢',
        '基金经理选股范围受限，α空间受限',
    ],
    '科技': [
        '科技行业波动大，回撤可能很深',
        '估值偏高时安全边际不足',
        '技术迭代风险大，落后即被淘汰',
        '政策监管不确定性高',
        '持仓集中度高，单只标的影响大',
    ],
    '消费': [
        '消费复苏节奏不确定，短期可能承压',
        '估值偏高时性价比下降',
        '消费降级趋势对高端品牌有压力',
        '原材料成本上涨侵蚀利润',
        '新品类培育周期长',
    ],
    '医药': [
        '集采政策持续压制板块估值',
        '研发失败风险高，管线不确定性大',
        '监管审批周期长，商业化兑现慢',
        '细分赛道间分化严重',
        '医保控费压力长期存在',
    ],
    '红利': [
        '成长行情中红利板块往往跑输大盘',
        '高股息不可持续，有"价值陷阱"风险',
        '利率上行周期中相对吸引力下降',
        '行业集中度高（金融、能源占比大），分散不足',
        '资本增值空间有限',
    ],
    '价值': [
        '价值回归需要时间，可能长期跑输成长',
        '低估值标的可能长期不反转',
        '在成长股主导的市场中表现平淡',
        '对宏观经济周期敏感',
        '选股难度大，容易陷入价值陷阱',
    ],
    '成长': [
        '高估值意味着容错率低，业绩不及预期时下跌剧烈',
        '技术路线变革可能导致原有龙头被颠覆',
        '市场风格切换时成长股往往首当其冲',
        '行业竞争激烈，利润率可能下降',
        '过度依赖少数明星基金经理的选股能力',
    ],
    '泛周期': [
        '强周期属性意味着业绩波动大',
        '经济下行周期中亏损严重',
        ' commodity 价格受全球供需影响大',
        '政策调控对周期行业影响显著',
        '难以把握周期性拐点，择时难度大',
    ],
    '权益低波': [
        '牛市弹性不足，涨幅通常落后大盘',
        '低波策略在趋势市中可能跑输',
        '选股范围受限，可能错过高弹性标的',
        '成分股流动性可能不如宽基指数',
        '在牛市中持有体验较差',
    ],
    '被动权益_指数增强': [
        '超额收益不保证，市场风格不利于模型时可能跑输指数',
        '量化模型存在过拟合风险',
        '换手率较高可能导致交易成本上升',
        '因子拥挤时超额收益衰减',
    ],
    '被动权益_宽基': [
        '完全跟随指数，无法规避系统性风险',
        '熊市无法止损，只能承受整体下跌',
        '费率虽低但长期仍是成本',
        '指数编制规则变更可能影响跟踪效果',
    ],
    '被动权益_行业': [
        '行业集中度高，单赛道风险大',
        '行业景气下行时跌幅显著大于大盘',
        '政策风险对特定行业影响大',
        '无法通过分散降低行业系统性风险',
        '买入时点选择很重要，追高风险大',
    ],
    '被动权益_黄金': [
        '黄金不产生利息或股息，持有成本隐性',
        '价格受美元走势和实际利率影响大',
        '短期波动可能较大',
        '与股票相关性在极端行情下可能上升',
    ],
}


def generate_pros_cons(f, seed=0):
    """根据基金分类生成优缺点。"""
    cat2 = f.get('cat2', '')
    cat3 = f.get('cat3', '')
    name = f.get('name', '')

    fof = 'FOF' in name or 'FOF）' in name

    # 决定 pros/cons pool key
    if cat2 == '固收+':
        pool_key = '固收+'
    elif cat2 == '被动权益':
        if cat3 == '指数增强':
            pool_key = '被动权益_指数增强'
        elif cat3 == '宽基':
            pool_key = '被动权益_宽基'
        elif cat3 == '行业':
            pool_key = '被动权益_行业'
        elif cat3 == '黄金':
            pool_key = '被动权益_黄金'
        else:
            pool_key = '被动权益_宽基'
    else:
        pool_key = cat3 if cat3 in _PROS_POOL else '均衡'

    pros_pool = _PROS_POOL.get(pool_key, _PROS_POOL['均衡'])
    cons_pool = _CONS_POOL.get(pool_key, _CONS_POOL['均衡'])

    # FOF 特殊处理：增加 FOF 相关的 pros/cons
    if fof:
        fof_pros = [
            'FOF 基金实现二次分散，风险更低',
            '可跨策略跨资产优化配置',
            '专业团队筛选子基金，降低选基难度',
        ]
        fof_cons = [
            'FOF 存在双重收费问题',
            '子基金策略不透明',
            '业绩受基金经理选基能力影响大',
        ]
        all_pros = pros_pool + fof_pros
        all_cons = cons_pool + fof_cons
    else:
        all_pros = pros_pool
        all_cons = cons_pool

    seed_val = seed if seed else abs(hash(name))
    return pick(all_pros, 3, seed=seed_val), pick(all_cons, 3, seed=seed_val + 1)


def generate_profiles(fund_list):
    """Step 3: 为所有基金生成投资风格、优缺点。返回 {code: profile}。"""
    print('Step 3: 生成投资风格/优缺点...')
    results = {}

    for i, fund in enumerate(fund_list):
        code = fund['code']
        name = fund['name']
        company = fund.get('company', _extract_company(name))
        cat1 = fund.get('category1', '')
        cat2 = fund.get('category2', '')
        cat3 = fund.get('category3', '')

        # 构造兼容 generate_style 的 dict
        f = {
            'name': name,
            'cat1': cat1,
            'cat2': cat2,
            'cat3': cat3,
            'company': company,
        }

        style = generate_style(f)
        pros, cons = generate_pros_cons(f, seed=hash(code) % 10000)

        results[code] = {
            'investment_style': style,
            'pros': pros,
            'cons': cons,
        }

        if (i + 1) % 10 == 0 or (i + 1) == len(fund_list):
            print(f'  进度: {i + 1}/{len(fund_list)}')

    out_path = os.path.join(BASE_DIR, 'fund_profiles.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)
    print(f'  已写入 {out_path}')
    return results


# ── Step 4: merge_all_data ────────────────────────────────────────

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


# NAV title → 字段名映射
_TITLE_MAP = {
    'JN': 'ytd',
    'Y':  '1m',
    '3Y': '3m',
    '6Y': '6m',
    '1N': '1y',
    '3N': '3y',
}


def get_nav_on_or_before(records, target):
    """获取 target 日期当天或之前的最新净值记录。"""
    target_dt = datetime.strptime(target, '%Y-%m-%d') if isinstance(target, str) else target
    best = None
    for r in records:
        d = datetime.strptime(r['date'], '%Y-%m-%d')
        if d <= target_dt:
            best = r
    return best


def calc_return(records, start_date, end_date):
    """计算区间收益率。"""
    start = get_nav_on_or_before(records, start_date)
    end = get_nav_on_or_before(records, end_date)
    if start is None or end is None:
        return None
    s, e = float(start['nav']), float(end['nav'])
    if s == 0:
        return None
    return (e - s) / s


def calc_max_drawdown(records, start_date, end_date):
    """计算区间最大回撤。"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date
    prices = []
    for r in records:
        d = datetime.strptime(r['date'], '%Y-%m-%d')
        if start_dt <= d <= end_dt:
            prices.append(float(r['nav']))
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


def load_september_focus():
    path = os.path.join(BASE_DIR, 'september_focus.json')
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_base(name):
    n = name.replace(' ', '').replace('证券投资基金', '').replace('型', '')
    n = re.sub(r'[A-FC]+(?:类|份额)?$', '', n)
    return n


def get_share_class(name):
    m = re.search(r'([A-FC]+)(?:类|份额)?$', name.replace(' ', ''))
    return m.group(1) if m else 'A'


def mark_september_focus(combined, sep_names):
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


def merge_all_data(fund_list, extra_data, holdings_data, profiles_data):
    """Step 4: 合并全部数据，fetch NAV + period returns，输出 web_fund_data.json。"""
    print('Step 4: 合并数据 + 抓取净值/阶段涨幅...')

    codes = [f['code'] for f in fund_list]
    fund_info = {f['code']: f for f in fund_list}
    print(f'  基金数量: {len(codes)}')

    # ── 抓取 NAV ──
    print('  抓取净值数据...')
    nav_data = {}
    nav_failed = []
    for i, code in enumerate(codes):
        result = None
        for attempt in range(RETRY_TIMES):
            result = fetch_fund_nav(code)
            if result is not None:
                break
            if attempt < RETRY_TIMES - 1:
                time.sleep(RETRY_BACKOFF_BASE * (attempt + 1))

        if result:
            nav_data[code] = result
        else:
            nav_failed.append(code)

        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'    净值进度: {i + 1}/{len(codes)}')

    print(f'  净值成功: {len(nav_data)}/{len(codes)}, 失败: {len(nav_failed)}')

    # ── 抓取阶段涨幅 ──
    print('  抓取阶段涨幅数据...')
    period_data = {}
    pi_failed = []
    for i, code in enumerate(codes):
        pi = None
        for attempt in range(RETRY_TIMES):
            pi = fetch_period_increase(code)
            if pi is not None:
                break
            if attempt < RETRY_TIMES - 1:
                time.sleep(RETRY_BACKOFF_BASE * (attempt + 1))

        if pi is not None:
            period_data[code] = pi
        else:
            pi_failed.append(code)

        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'    阶段涨幅进度: {i + 1}/{len(codes)}')

    print(f'  阶段涨幅成功: {len(period_data)}/{len(codes)}, 失败: {len(pi_failed)}')

    # ── 抓取基金规模与买卖规则 ──
    print('  抓取基金规模与买卖规则...')
    basic_data = {}
    basic_failed = []
    for i, code in enumerate(codes):
        bi = None
        for attempt in range(RETRY_TIMES):
            bi = fetch_fund_basic_info(code)
            if bi is not None:
                break
            if attempt < RETRY_TIMES - 1:
                time.sleep(RETRY_BACKOFF_BASE * (attempt + 1))
        if bi is not None:
            basic_data[code] = bi
        else:
            basic_failed.append(code)
        if (i + 1) % 20 == 0 or (i + 1) == len(codes):
            print(f'    规模规则进度: {i + 1}/{len(codes)}')
    print(f'  规模规则成功: {len(basic_data)}/{len(codes)}, 失败: {len(basic_failed)}')

    def _fmt_scale(v):
        if v is None:
            return None
        try:
            return round(float(v) / 100000000, 2)
        except (ValueError, TypeError):
            return None

    # ── 构建最终数据 ──
    print('  构建最终数据...')
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
        profile = profiles_data.get(code, {})
        h = holdings_data.get(code, {})
        bi = basic_data.get(code, {})

        # 优先级：API 阶段涨幅 > pingzhongdata.js 收益 > NAV 计算
        returns = {
            'ytd': pi.get('JN'),
            '1m': pi.get('Y'),
            '3m': pi.get('3Y'),
            '6m': pi.get('6Y'),
            '1y': pi.get('1N'),
            '3y': pi.get('3N'),
        }

        # API 缺失时用 NAV 回退计算
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
            # 优先使用 pingzhongdata.js 3y
            em = e.get('returns_eastmoney') or {}
            if '3y' in em:
                returns['3y'] = em['3y']
            else:
                r3y = calc_return(records, (end_dt - timedelta(days=1096)).strftime('%Y-%m-%d'), latest)
                returns['3y'] = _pct(r3y)

        mdd_3y = calc_max_drawdown(records, (end_dt - timedelta(days=1095)).strftime('%Y-%m-%d'), latest)

        # 赛道：优先使用 holdings 中的 sector（Step 2 产出），其次额外数据
        sector = h.get('sector', '') or e.get('sector', '')

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
            'sector': sector,
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
            'purchase_rate_tiers': bi.get('purchase_rate_tiers', []),
            'redeem_rate_tiers': bi.get('redeem_rate_tiers', []),
            'fund_type': bi.get('fund_type', ''),
            'risk_level': bi.get('risk_level', ''),
            'is_september_focus': False,
        })

    # 分类列表
    cat2s = sorted(set(f['category2'] for f in combined if f['category2']))
    cat3s = sorted(set(f['category3'] for f in combined if f['category3']))
    categories = {'level2': cat2s, 'level3': cat3s}

    # 9月重点标记
    print('  标记9月重点资产...')
    sep_names = load_september_focus()
    combined = mark_september_focus(combined, sep_names)

    # 输出主数据（不含 nav_history，减少文件体积加速加载）
    # updated_at 记录本次数据生成时间（即工作流实际更新时间），转北京时间(UTC+8)，供首页展示
    updated_at = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    data = {'funds': combined, 'categories': categories, 'updated_at': updated_at}
    out_path = os.path.join(BASE_DIR, 'web_fund_data.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    # 输出净值历史独立文件（详情页按需加载）
    nav_history_map = {}
    for code in codes:
        if code in nav_data:
            records = sorted(nav_data[code]['records'], key=lambda x: x['date'])
            if records:
                nav_history_map[code] = [[r['date'], r['nav']] for r in records]
    nav_path = os.path.join(BASE_DIR, 'fund_nav_history.json')
    with open(nav_path, 'w', encoding='utf-8') as f:
        json.dump(nav_history_map, f, ensure_ascii=False)

    print(f'  已写入主数据 {out_path}')
    print(f'  已写入净值历史 {nav_path}')
    print(f'  共 {len(combined)} 只基金')
    return data


# ── main ──────────────────────────────────────────────────────────

def main():
    print(f'[{datetime.now()}] run_all.py 开始执行')
    print()

    # 加载 fund_list.json
    fund_list_path = os.path.join(BASE_DIR, 'fund_list.json')
    with open(fund_list_path, 'r', encoding='utf-8') as f:
        fund_list = json.load(f)
    print(f'加载 {len(fund_list)} 只基金')
    print()

    # Step 1: fetch_extra_data
    extra_data = fetch_extra_data(fund_list)
    print()

    # Step 2: fetch_holdings
    holdings_data = fetch_holdings(fund_list)
    print()

    # Step 3: generate_profiles
    profiles_data = generate_profiles(fund_list)
    print()

    # Step 4: merge_all_data
    merge_all_data(fund_list, extra_data, holdings_data, profiles_data)

    print()
    print(f'[{datetime.now()}] 全部完成')


if __name__ == '__main__':
    main()