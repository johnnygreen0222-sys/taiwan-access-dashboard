"""
數據抓取模組 — GA4 / Meta Ads / GSC
支援兩種模式：
  本機：讀 config.json + ~/.claude/ 憑證檔
  雲端：讀環境變數 GOOGLE_SA_JSON / META_* / GSC_SITE_URL
"""

import json, os, re, time, urllib.request, urllib.parse
from datetime import datetime, timedelta

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')

def _load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}

CFG = _load_config()


# ══════════════════════════════════════════
#  GA4 電商數據
# ══════════════════════════════════════════

def _get_ga4_creds():
    """優先讀環境變數，fallback 到本機檔案"""
    from google.oauth2 import service_account
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    sa_json = os.environ.get('GOOGLE_SA_JSON')
    if sa_json:
        info = json.loads(sa_json)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    sa_path = os.path.expanduser('~/.claude/gcp-service-account.json')
    return service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)

GA4_PROPERTY = os.environ.get('GA4_PROPERTY_ID', '296390016')


def _ga4_report(body):
    from googleapiclient.discovery import build
    creds  = _get_ga4_creds()
    client = build('analyticsdata', 'v1beta', credentials=creds)
    return client.properties().runReport(
        property=f'properties/{GA4_PROPERTY}', body=body).execute()


def fetch_ga4_ecommerce(days=30):
    """回傳 GA4 電商整合數據"""
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    prev_end   = (datetime.today() - timedelta(days=days+1)).strftime('%Y-%m-%d')
    prev_start = (datetime.today() - timedelta(days=days*2)).strftime('%Y-%m-%d')

    def v(row, i, typ=float):
        try: return typ(row['metricValues'][i]['value'])
        except: return typ(0)

    # ── KPI（本期 vs 上期）
    kpi = _ga4_report({
        'dateRanges': [
            {'startDate': start,      'endDate': end,       'name': 'current'},
            {'startDate': prev_start, 'endDate': prev_end,  'name': 'previous'},
        ],
        'metrics': [
            {'name': 'purchaseRevenue'}, {'name': 'transactions'},
            {'name': 'averagePurchaseRevenue'}, {'name': 'sessions'},
            {'name': 'addToCarts'}, {'name': 'checkouts'},
        ],
    })
    rows = kpi.get('rows', [])
    cur = rows[0] if rows else {'metricValues': [{'value':'0'}]*6}
    prv = rows[1] if len(rows) > 1 else {'metricValues': [{'value':'0'}]*6}
    revenue_c = v(cur,0); revenue_p = v(prv,0)
    txn_c     = v(cur,1); txn_p     = v(prv,1)
    aov_c     = v(cur,2)
    sess_c    = v(cur,3)
    cart_c    = v(cur,4)
    chk_c     = v(cur,5)
    cvr_c     = txn_c / sess_c * 100 if sess_c else 0

    def chg(a, b): return round((a-b)/b*100, 1) if b else 0

    # ── 每日趨勢
    daily_raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'date'}],
        'metrics': [{'name': 'purchaseRevenue'}, {'name': 'transactions'}],
        'orderBys': [{'dimension': {'dimensionName': 'date'}}],
    })
    daily = [{'date': r['dimensionValues'][0]['value'],
              'revenue': round(v(r,0)), 'orders': int(v(r,1))}
             for r in daily_raw.get('rows', [])]

    # ── 流量來源
    src_raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'sessionDefaultChannelGrouping'}],
        'metrics': [{'name': 'purchaseRevenue'}, {'name': 'transactions'}, {'name': 'sessions'}],
        'orderBys': [{'metric': {'metricName': 'purchaseRevenue'}, 'desc': True}],
        'limit': 8,
    })
    sources = [{'channel': r['dimensionValues'][0]['value'],
                'revenue': round(v(r,0)), 'orders': int(v(r,1))}
               for r in src_raw.get('rows', [])]

    # ── 熱賣商品
    item_raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'itemName'}],
        'metrics': [{'name': 'itemRevenue'}, {'name': 'itemsPurchased'}],
        'orderBys': [{'metric': {'metricName': 'itemRevenue'}, 'desc': True}],
        'limit': 10,
    })
    products = [{'name': r['dimensionValues'][0]['value'],
                 'revenue': round(v(r,0)), 'qty': int(v(r,1))}
                for r in item_raw.get('rows', [])]

    return {
        'period': {'start': start, 'end': end, 'days': days},
        'kpi': {
            'revenue':    {'value': round(revenue_c), 'change': chg(revenue_c, revenue_p)},
            'orders':     {'value': int(txn_c),       'change': chg(txn_c, txn_p)},
            'aov':        {'value': round(aov_c),     'change': chg(aov_c, v(prv,2))},
            'cvr':        round(cvr_c, 2),
            'add_to_cart': int(cart_c),
            'checkouts':  int(chk_c),
        },
        'funnel': {
            'sessions':  int(sess_c),
            'add_cart':  int(cart_c),
            'checkout':  int(chk_c),
            'purchase':  int(txn_c),
        },
        'daily':    daily,
        'sources':  sources,
        'products': products,
    }


def fetch_ga4_extras(days=30):
    """裝置分析 + 新舊訪客"""
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')

    def iv(row, i):
        try: return float(row['metricValues'][i]['value'])
        except: return 0.0

    # 裝置類型
    dev_raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'deviceCategory'}],
        'metrics': [{'name': 'sessions'}, {'name': 'purchaseRevenue'}, {'name': 'transactions'}],
        'orderBys': [{'metric': {'metricName': 'sessions'}, 'desc': True}],
    })
    devices = [{
        'device':   r['dimensionValues'][0]['value'],
        'sessions': int(iv(r, 0)),
        'revenue':  round(iv(r, 1)),
        'orders':   int(iv(r, 2)),
    } for r in dev_raw.get('rows', [])]

    # 新訪客 vs 回訪客
    nv_raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'newVsReturning'}],
        'metrics': [{'name': 'sessions'}, {'name': 'purchaseRevenue'}, {'name': 'transactions'}],
    })
    user_types = [{
        'type':     r['dimensionValues'][0]['value'],
        'sessions': int(iv(r, 0)),
        'revenue':  round(iv(r, 1)),
        'orders':   int(iv(r, 2)),
    } for r in nv_raw.get('rows', [])]

    return {'devices': devices, 'user_types': user_types}


# ══════════════════════════════════════════
#  Meta Ads 數據
# ══════════════════════════════════════════

META_TOKEN   = os.environ.get('META_ACCESS_TOKEN') or CFG.get('meta_access_token', '')
META_ACCOUNT = os.environ.get('META_AD_ACCOUNT_ID') or CFG.get('meta_ad_account_id', '')
GRAPH_VER    = 'v21.0'


def _meta_get(path, params={}):
    if not META_TOKEN or not META_ACCOUNT:
        raise ValueError('未設定 Meta 憑證')
    # 動態讀取（可能在 server 啟動後才寫入）
    token   = os.environ.get('META_ACCESS_TOKEN') or _load_config().get('meta_access_token', META_TOKEN)
    account = os.environ.get('META_AD_ACCOUNT_ID') or _load_config().get('meta_ad_account_id', META_ACCOUNT)
    p = dict(params)
    p['access_token'] = token
    url = f'https://graph.facebook.com/{GRAPH_VER}/{path}?{urllib.parse.urlencode(p)}'
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.loads(r.read())
    if 'error' in data:
        raise RuntimeError(data['error'].get('message', str(data['error'])))
    rows, next_url = data.get('data', []), data.get('paging', {}).get('next')
    while next_url:
        with urllib.request.urlopen(next_url, timeout=30) as r:
            data = json.loads(r.read())
        rows.extend(data.get('data', []))
        next_url = data.get('paging', {}).get('next')
    return rows


def _act(key):
    return os.environ.get('META_AD_ACCOUNT_ID') or _load_config().get('meta_ad_account_id', META_ACCOUNT)


def fetch_meta_ads(days=30):
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    account = _act('META_AD_ACCOUNT_ID')

    fields = 'campaign_name,spend,impressions,clicks,ctr,cpc,actions,action_values'
    rows   = _meta_get(f'{account}/insights', {
        'level':      'campaign',
        'time_range': json.dumps({'since': start, 'until': end}),
        'fields':     fields,
        'limit':      200,
    })

    def av(lst, t):
        for a in (lst or []):
            if a.get('action_type') == t: return float(a.get('value', 0))
        return 0.0

    campaigns = []
    tot_spend = tot_rev = tot_clicks = 0
    for r in rows:
        spend = float(r.get('spend', 0))
        rev   = av(r.get('action_values', []), 'purchase')
        roas  = round(rev / spend, 2) if spend > 0 else 0
        campaigns.append({
            'name':    r.get('campaign_name', ''),
            'spend':   round(spend),
            'clicks':  int(r.get('clicks', 0)),
            'ctr':     round(float(r.get('ctr', 0)), 2),
            'revenue': round(rev),
            'roas':    roas,
        })
        tot_spend += spend; tot_rev += rev; tot_clicks += int(r.get('clicks', 0))

    campaigns.sort(key=lambda x: x['spend'], reverse=True)
    return {
        'period': {'start': start, 'end': end},
        'kpi': {
            'spend':   round(tot_spend),
            'revenue': round(tot_rev),
            'roas':    round(tot_rev / tot_spend, 2) if tot_spend else 0,
            'clicks':  tot_clicks,
        },
        'campaigns': campaigns,
    }


def fetch_meta_daily(days=30):
    """Meta Ads 每日花費 + 營收趨勢（帳號層級）"""
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    account = _act('META_AD_ACCOUNT_ID')

    rows = _meta_get(f'{account}/insights', {
        'level':          'account',
        'time_range':     json.dumps({'since': start, 'until': end}),
        'time_increment': 1,
        'fields':         'date_start,spend,impressions,clicks,actions,action_values',
        'limit':          100,
    })

    def av(lst, t):
        for a in (lst or []):
            if a.get('action_type') == t: return float(a.get('value', 0))
        return 0.0

    daily = []
    for r in sorted(rows, key=lambda x: x.get('date_start', '')):
        spend = float(r.get('spend', 0))
        rev   = av(r.get('action_values', []), 'purchase')
        daily.append({
            'date':    r.get('date_start', ''),
            'spend':   round(spend),
            'revenue': round(rev),
            'roas':    round(rev / spend, 2) if spend > 0 else 0,
            'clicks':  int(r.get('clicks', 0)),
        })

    return {'period': {'start': start, 'end': end}, 'daily': daily}


# ══════════════════════════════════════════
#  GSC 關鍵字數據
# ══════════════════════════════════════════

GSC_SITE = os.environ.get('GSC_SITE_URL', 'https://www.taiwanaccess.com.tw/')


def _gsc_creds():
    from google.oauth2 import service_account
    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
    sa_json = os.environ.get('GOOGLE_SA_JSON')
    if sa_json:
        return service_account.Credentials.from_service_account_info(
            json.loads(sa_json), scopes=SCOPES)
    sa_path = os.path.expanduser('~/.claude/gcp-service-account.json')
    return service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)


def fetch_gsc_keywords(days=30):
    from googleapiclient.discovery import build
    svc   = build('searchconsole', 'v1', credentials=_gsc_creds())
    end   = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days+3)).strftime('%Y-%m-%d')

    resp = svc.searchanalytics().query(
        siteUrl=GSC_SITE,
        body={
            'startDate': start, 'endDate': end,
            'dimensions': ['query'],
            'rowLimit': 20,
            'orderBy': [{'fieldName': 'clicks', 'sortOrder': 'DESCENDING'}],
        }
    ).execute()

    keywords = []
    for r in resp.get('rows', []):
        keywords.append({
            'keyword':     r['keys'][0],
            'clicks':      r.get('clicks', 0),
            'impressions': r.get('impressions', 0),
            'ctr':         round(r.get('ctr', 0) * 100, 1),
            'position':    round(r.get('position', 0), 1),
        })

    total_clicks = sum(k['clicks'] for k in keywords)
    total_impr   = sum(k['impressions'] for k in keywords)

    return {
        'period': {'start': start, 'end': end},
        'kpi': {
            'total_clicks':      total_clicks,
            'total_impressions': total_impr,
            'avg_ctr':           round(total_clicks / total_impr * 100, 1) if total_impr else 0,
        },
        'keywords': keywords,
    }


def fetch_gsc_pages(days=30):
    """回傳各頁面的自然流量排名（按點擊數排序，Top 25）"""
    from googleapiclient.discovery import build
    svc   = build('searchconsole', 'v1', credentials=_gsc_creds())
    end   = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days+3)).strftime('%Y-%m-%d')

    # ── 頁面總覽（點擊 / 曝光 / 排名）
    resp = svc.searchanalytics().query(
        siteUrl=GSC_SITE,
        body={
            'startDate': start, 'endDate': end,
            'dimensions': ['page'],
            'rowLimit': 25,
            'orderBy': [{'fieldName': 'clicks', 'sortOrder': 'DESCENDING'}],
        }
    ).execute()

    pages = []
    for r in resp.get('rows', []):
        url = r['keys'][0]
        # 取路徑部分，方便閱讀
        slug = url.replace('https://www.taiwanaccess.com.tw', '').replace('https://taiwanaccess.com.tw', '') or '/'
        pages.append({
            'url':         url,
            'slug':        slug,
            'clicks':      r.get('clicks', 0),
            'impressions': r.get('impressions', 0),
            'ctr':         round(r.get('ctr', 0) * 100, 1),
            'position':    round(r.get('position', 0), 1),
        })

    # ── 前 5 頁分別抓 Top 3 關鍵字（page × query）
    top_urls = [p['url'] for p in pages[:5]]
    page_keywords = {}
    for url in top_urls:
        try:
            r2 = svc.searchanalytics().query(
                siteUrl=GSC_SITE,
                body={
                    'startDate': start, 'endDate': end,
                    'dimensions': ['query'],
                    'dimensionFilterGroups': [{
                        'filters': [{'dimension': 'page', 'operator': 'equals', 'expression': url}]
                    }],
                    'rowLimit': 3,
                    'orderBy': [{'fieldName': 'clicks', 'sortOrder': 'DESCENDING'}],
                }
            ).execute()
            slug = url.replace('https://www.taiwanaccess.com.tw', '') or '/'
            page_keywords[slug] = [row['keys'][0] for row in r2.get('rows', [])]
        except Exception:
            pass

    return {
        'period':        {'start': start, 'end': end},
        'pages':         pages,
        'page_keywords': page_keywords,
    }


# ══════════════════════════════════════════
#  EDM 成效：GA4 UTM 追蹤 + Mailchimp API
# ══════════════════════════════════════════

def fetch_edm_utm(days=30):
    """從 GA4 抓 LINE / Email / EDM 各 UTM source 的流量成效"""
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')

    def iv(row, i):
        try: return float(row['metricValues'][i]['value'])
        except: return 0.0

    # 抓所有 source × medium 組合
    raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [
            {'name': 'sessionSource'},
            {'name': 'sessionMedium'},
            {'name': 'sessionCampaignName'},
        ],
        'metrics': [
            {'name': 'sessions'},
            {'name': 'purchaseRevenue'},
            {'name': 'transactions'},
            {'name': 'addToCarts'},
        ],
        'orderBys': [{'metric': {'metricName': 'sessions'}, 'desc': True}],
        'limit': 200,
    })

    # EDM 相關關鍵字
    EDM_SOURCES  = {'line', 'mailchimp', 'email', 'newsletter', 'edm', 'line官方帳號'}
    EDM_MEDIUMS  = {'email', 'edm', 'newsletter', 'social', 'line'}

    channels = []
    for r in raw.get('rows', []):
        src = r['dimensionValues'][0]['value'].lower()
        med = r['dimensionValues'][1]['value'].lower()
        cam = r['dimensionValues'][2]['value']
        if not (src in EDM_SOURCES or med in EDM_MEDIUMS or
                any(k in src for k in ['line','mail','edm','newsletter'])):
            continue
        sess = int(iv(r, 0))
        rev  = round(iv(r, 1))
        txn  = int(iv(r, 2))
        cart = int(iv(r, 3))
        channels.append({
            'source':   r['dimensionValues'][0]['value'],
            'medium':   r['dimensionValues'][1]['value'],
            'campaign': cam if cam != '(not set)' else '',
            'sessions': sess,
            'revenue':  rev,
            'orders':   txn,
            'add_cart': cart,
            'cvr':      round(txn / sess * 100, 2) if sess else 0,
        })

    # 彙總 LINE vs Email
    def group(sources_filter):
        rows = [c for c in channels if any(k in c['source'].lower() or k in c['medium'].lower()
                                           for k in sources_filter)]
        return {
            'sessions': sum(r['sessions'] for r in rows),
            'revenue':  sum(r['revenue']  for r in rows),
            'orders':   sum(r['orders']   for r in rows),
        }

    line_total  = group(['line'])
    email_total = group(['email','mailchimp','edm','newsletter'])

    return {
        'period':      {'start': start, 'end': end},
        'channels':    channels,
        'line_total':  line_total,
        'email_total': email_total,
    }


def fetch_mailchimp(days=30):
    """抓 Mailchimp 最近寄送的活動成效（需設定 mailchimp_api_key）"""
    api_key = os.environ.get('MAILCHIMP_API_KEY') or CFG.get('mailchimp_api_key', '')
    if not api_key:
        raise ValueError('未設定 Mailchimp API Key（mailchimp_api_key）')

    # API key 格式：xxxxx-us1，最後段是 datacenter
    dc = api_key.split('-')[-1]
    base = f'https://{dc}.api.mailchimp.com/3.0'

    def mc_get(path, params={}):
        qs = urllib.parse.urlencode(params)
        url = f'{base}/{path}?{qs}'
        req = urllib.request.Request(url)
        import base64
        token = base64.b64encode(f'anystring:{api_key}'.encode()).decode()
        req.add_header('Authorization', f'Basic {token}')
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())

    # 取最近 10 個已發送活動
    cutoff = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S+00:00')
    resp = mc_get('campaigns', {
        'status': 'sent',
        'count': 10,
        'sort_field': 'send_time',
        'sort_dir': 'DESC',
        'since_send_time': cutoff,
        'fields': 'campaigns.id,campaigns.settings.subject_line,campaigns.settings.title,'
                  'campaigns.emails_sent,campaigns.send_time,campaigns.report_summary',
    })

    campaigns = []
    for c in resp.get('campaigns', []):
        rs = c.get('report_summary', {})
        campaigns.append({
            'title':         c.get('settings', {}).get('title', ''),
            'subject':       c.get('settings', {}).get('subject_line', ''),
            'send_time':     (c.get('send_time') or '')[:10],
            'emails_sent':   c.get('emails_sent', 0),
            'open_rate':     round(rs.get('open_rate', 0) * 100, 1),
            'click_rate':    round(rs.get('click_rate', 0) * 100, 1),
            'opens':         rs.get('opens', 0),
            'clicks':        rs.get('clicks', 0),
            'unsubscribes':  rs.get('unsubscribes', 0),
        })

    total_sent  = sum(c['emails_sent'] for c in campaigns)
    avg_open    = round(sum(c['open_rate'] for c in campaigns) / len(campaigns), 1) if campaigns else 0
    avg_click   = round(sum(c['click_rate'] for c in campaigns) / len(campaigns), 1) if campaigns else 0

    return {
        'period':    {'days': days},
        'kpi':       {'total_sent': total_sent, 'avg_open_rate': avg_open, 'avg_click_rate': avg_click},
        'campaigns': campaigns,
    }


# ══════════════════════════════════════════
#  商品層級漏斗：瀏覽 → 加購 → 購買
# ══════════════════════════════════════════

def fetch_ga4_product_funnel(days=30):
    """商品層級轉化率：瀏覽 → 加購 → 購買"""
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')

    def iv(row, i):
        try: return float(row['metricValues'][i]['value'])
        except: return 0.0

    raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'itemName'}],
        'metrics': [
            {'name': 'itemRevenue'},
            {'name': 'itemsPurchased'},
            {'name': 'itemsAddedToCart'},
        ],
        'orderBys': [{'metric': {'metricName': 'itemRevenue'}, 'desc': True}],
        'limit': 15,
    })

    products = []
    for r in raw.get('rows', []):
        name  = r['dimensionValues'][0]['value']
        rev   = round(iv(r, 0))
        buys  = int(iv(r, 1))
        carts = int(iv(r, 2))
        products.append({
            'name':        name,
            'add_carts':   carts,
            'purchases':   buys,
            'revenue':     rev,
            'cart_to_buy': round(buys / carts * 100, 1) if carts else 0,
        })

    return {'period': {'start': start, 'end': end}, 'products': products}


# ══════════════════════════════════════════
#  年同期比較 YoY
# ══════════════════════════════════════════

def fetch_ga4_yoy(days=30):
    """GA4 年同期比較（當期 vs 去年同期）"""
    end       = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start     = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    yoy_end   = (datetime.today() - timedelta(days=366)).strftime('%Y-%m-%d')
    yoy_start = (datetime.today() - timedelta(days=days + 365)).strftime('%Y-%m-%d')

    def v(row, i, typ=float):
        try: return typ(row['metricValues'][i]['value'])
        except: return typ(0)

    raw = _ga4_report({
        'dateRanges': [
            {'startDate': start,     'endDate': end,      'name': 'current'},
            {'startDate': yoy_start, 'endDate': yoy_end,  'name': 'yoy'},
        ],
        'metrics': [
            {'name': 'purchaseRevenue'},
            {'name': 'transactions'},
            {'name': 'sessions'},
            {'name': 'addToCarts'},
        ],
    })

    rows = raw.get('rows', [])
    cur  = rows[0] if rows else {'metricValues': [{'value': '0'}] * 4}
    yoy  = rows[1] if len(rows) > 1 else {'metricValues': [{'value': '0'}] * 4}

    def chg(a, b): return round((a - b) / b * 100, 1) if b else 0

    rev_c = v(cur, 0); rev_y = v(yoy, 0)
    txn_c = v(cur, 1); txn_y = v(yoy, 1)
    ses_c = v(cur, 2); ses_y = v(yoy, 2)
    crt_c = v(cur, 3); crt_y = v(yoy, 3)

    return {
        'period': {
            'current_start': start, 'current_end': end,
            'yoy_start': yoy_start, 'yoy_end': yoy_end,
        },
        'current': {'revenue': round(rev_c), 'orders': int(txn_c), 'sessions': int(ses_c), 'add_carts': int(crt_c)},
        'yoy':     {'revenue': round(rev_y), 'orders': int(txn_y), 'sessions': int(ses_y), 'add_carts': int(crt_y)},
        'changes': {
            'revenue':  chg(rev_c, rev_y),
            'orders':   chg(txn_c, txn_y),
            'sessions': chg(ses_c, ses_y),
        },
    }


# ══════════════════════════════════════════
#  CC1 銷售目標追蹤
# ══════════════════════════════════════════

CC1_TARGET_YEAR = 120
CC1_TARGET_Q2   = 80
CC1_PRICE       = 14300


def fetch_cc1_progress():
    """CC1 年度 & Q2 銷售進度（需 GA4 商品名稱含 CC1）"""
    today = datetime.today()
    end   = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    year_start = today.replace(month=1, day=1).strftime('%Y-%m-%d')
    q2_start   = f'{today.year}-04-01'
    q2_end_raw = f'{today.year}-06-30'
    q2_end     = q2_end_raw if end >= q2_end_raw else end

    def iv(row, i):
        try: return float(row['metricValues'][i]['value'])
        except: return 0.0

    def query_cc1(start_d, end_d):
        return _ga4_report({
            'dateRanges': [{'startDate': start_d, 'endDate': end_d}],
            'dimensions': [{'name': 'itemName'}],
            'metrics':    [{'name': 'itemsPurchased'}, {'name': 'itemRevenue'}],
            'dimensionFilter': {
                'filter': {
                    'fieldName': 'itemName',
                    'stringFilter': {'matchType': 'CONTAINS', 'value': 'CC1', 'caseSensitive': False},
                }
            },
        })

    raw_yr = query_cc1(year_start, end)
    yr_qty = sum(int(iv(r, 0)) for r in raw_yr.get('rows', []))
    yr_rev = sum(iv(r, 1) for r in raw_yr.get('rows', []))

    raw_q2 = query_cc1(q2_start, q2_end)
    q2_qty = sum(int(iv(r, 0)) for r in raw_q2.get('rows', []))
    q2_rev = sum(iv(r, 1) for r in raw_q2.get('rows', []))

    return {
        'period':  {'year_start': year_start, 'q2_start': q2_start, 'q2_end': q2_end},
        'targets': {'year': CC1_TARGET_YEAR, 'q2': CC1_TARGET_Q2, 'price': CC1_PRICE},
        'year':    {'qty': yr_qty, 'revenue': round(yr_rev), 'pct': round(yr_qty / CC1_TARGET_YEAR * 100, 1)},
        'q2':      {'qty': q2_qty, 'revenue': round(q2_rev), 'pct': round(q2_qty / CC1_TARGET_Q2 * 100, 1)},
    }


# ══════════════════════════════════════════
#  Instagram Insights（Meta Graph API）
# ══════════════════════════════════════════

def fetch_instagram_insights(days=30):
    """Instagram 商業帳號洞察（透過廣告帳號找 IG ID，使用 Meta Graph API v21）"""
    token   = os.environ.get('META_ACCESS_TOKEN') or _load_config().get('meta_access_token', META_TOKEN)
    account = os.environ.get('META_AD_ACCOUNT_ID') or _load_config().get('meta_ad_account_id', META_ACCOUNT)
    if not token:
        raise ValueError('未設定 Meta token')

    def raw_get(path, params={}):
        p = dict(params); p['access_token'] = token
        url = f'https://graph.facebook.com/{GRAPH_VER}/{path}?{urllib.parse.urlencode(p)}'
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.loads(r.read())
        if 'error' in data:
            raise RuntimeError(data['error'].get('message', str(data['error'])))
        return data

    # 透過廣告帳號找 IG business account ID
    ig_list = raw_get(f'{account}/instagram_accounts')
    if not ig_list.get('data'):
        raise ValueError('廣告帳號下沒有連結的 Instagram 帳號')
    ig_id = ig_list['data'][0]['id']

    # 帳號基本資料
    info = raw_get(ig_id, {'fields': 'username,followers_count,media_count'})

    # ── 每日 reach（time_series，支援 since/until）
    since = int((datetime.today() - timedelta(days=days)).timestamp())
    until = int(datetime.today().timestamp())
    reach_raw = raw_get(f'{ig_id}/insights', {
        'metric': 'reach',
        'period': 'day',
        'since':  since,
        'until':  until,
    })
    daily_map = {}
    for m in reach_raw.get('data', []):
        for pt in m.get('values', []):
            d = pt['end_time'][:10]
            daily_map[d] = {'reach': pt.get('value', 0)}

    # ── 週期總量指標（total_value，只能抓當期累計）
    try:
        totals_raw = raw_get(f'{ig_id}/insights', {
            'metric':      'profile_views,total_interactions,accounts_engaged',
            'metric_type': 'total_value',
            'period':      'day',
        })
        totals = {m['name']: m.get('total_value', {}).get('value', 0)
                  for m in totals_raw.get('data', [])}
    except Exception:
        totals = {}

    daily = [{'date': d, **v} for d, v in sorted(daily_map.items())]
    total_reach = sum(d.get('reach', 0) for d in daily)

    return {
        'account': {
            'username':    info.get('username', ''),
            'followers':   info.get('followers_count', 0),
            'media_count': info.get('media_count', 0),
        },
        'kpi': {
            'reach':              total_reach,
            'profile_views':      totals.get('profile_views', 0),
            'total_interactions': totals.get('total_interactions', 0),
            'accounts_engaged':   totals.get('accounts_engaged', 0),
        },
        'daily': daily,
    }


# ══════════════════════════════════════════
#  收益預測（線性回歸 + MA14 Blend）
# ══════════════════════════════════════════

def fetch_revenue_forecast(days=60):
    """GA4 每日營收線性回歸 + 14 天預測（blend 70% lin + 30% MA14）"""
    end   = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d')

    raw = _ga4_report({
        'dateRanges': [{'startDate': start, 'endDate': end}],
        'dimensions': [{'name': 'date'}],
        'metrics':    [{'name': 'purchaseRevenue'}],
        'orderBys':   [{'dimension': {'dimensionName': 'date'}}],
    })

    daily = []
    for r in raw.get('rows', []):
        d = r['dimensionValues'][0]['value']
        try: rev = float(r['metricValues'][0]['value'])
        except: rev = 0.0
        daily.append({'date': d, 'revenue': round(rev)})

    if len(daily) < 7:
        return {'error': '數據不足（需至少 7 天）'}

    n  = len(daily)
    xs = list(range(n))
    ys = [d['revenue'] for d in daily]

    # OLS 線性回歸
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    denom  = sum((x - x_mean) ** 2 for x in xs)
    slope  = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n)) / denom if denom else 0
    intercept = y_mean - slope * x_mean

    # 14-day moving average（最後 14 天的平均）
    ma14 = sum(ys[-14:]) / min(14, len(ys))

    # Blend: 70% 線性 + 30% MA14（減少離群值影響）
    forecast = []
    for i in range(14):
        lin  = max(0, slope * (n + i) + intercept)
        pred = round(lin * 0.7 + ma14 * 0.3)
        forecast.append({
            'date':     (datetime.today() + timedelta(days=i)).strftime('%Y-%m-%d'),
            'forecast': pred,
        })

    direction = '上升' if slope > 100 else '下降' if slope < -100 else '平穩'

    return {
        'period':     {'start': start, 'end': end},
        'historical': daily,
        'forecast':   forecast,
        'trend':      {'slope_daily': round(slope), 'direction': direction, 'ma14': round(ma14)},
    }


# ══════════════════════════════════════════
#  競品關鍵字差距（GSC 近距離 + CTR Gap）
# ══════════════════════════════════════════

def fetch_keyword_gaps(days=90):
    """GSC：近距離關鍵字（排名 11–30）+ CTR 差距分析"""
    from googleapiclient.discovery import build
    svc   = build('searchconsole', 'v1', credentials=_gsc_creds())
    end   = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')
    start = (datetime.today() - timedelta(days=days + 3)).strftime('%Y-%m-%d')

    resp = svc.searchanalytics().query(
        siteUrl=GSC_SITE,
        body={
            'startDate': start, 'endDate': end,
            'dimensions': ['query'],
            'rowLimit': 500,
            'orderBy': [{'fieldName': 'impressions', 'sortOrder': 'DESCENDING'}],
        }
    ).execute()

    CTR_BENCH = {
        1: 28.0, 2: 15.0, 3: 10.0, 4: 7.0,  5: 5.0,
        6: 3.5,  7: 2.5,  8: 2.0,  9: 1.5, 10: 1.2,
        11: 0.8, 12: 0.6, 13: 0.5, 14: 0.4, 15: 0.35,
    }
    def expected_ctr(pos):
        p = int(round(pos))
        if p in CTR_BENCH: return CTR_BENCH[p]
        if p <= 20: return 0.25
        return 0.15

    near_miss, ctr_gaps = [], []
    for r in resp.get('rows', []):
        kw   = r['keys'][0]
        pos  = r.get('position', 0)
        impr = r.get('impressions', 0)
        clks = r.get('clicks', 0)
        ctr  = r.get('ctr', 0) * 100

        if 11 <= pos <= 30 and impr >= 50:
            near_miss.append({
                'keyword':     kw,
                'position':    round(pos, 1),
                'impressions': impr,
                'clicks':      clks,
                'ctr':         round(ctr, 2),
                'potential':   round(impr * expected_ctr(max(1, pos - 10)) / 100),
            })

        if impr >= 100:
            exp = expected_ctr(pos)
            gap = exp - ctr
            if ctr < exp * 0.5 and gap > 0.2:
                ctr_gaps.append({
                    'keyword':      kw,
                    'position':     round(pos, 1),
                    'impressions':  impr,
                    'ctr_actual':   round(ctr, 2),
                    'ctr_expected': round(exp, 2),
                    'ctr_gap':      round(gap, 2),
                })

    near_miss.sort(key=lambda x: x['impressions'], reverse=True)
    ctr_gaps.sort(key=lambda x: x['ctr_gap'], reverse=True)

    return {
        'period':    {'start': start, 'end': end, 'days': days},
        'near_miss': near_miss[:20],
        'ctr_gaps':  ctr_gaps[:15],
    }


# ══════════════════════════════════════════
#  Google Ads API（Basic Access 已核准）
# ══════════════════════════════════════════

GADS_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_CUSTOMER_ID', '7245588980')


def _get_gads_client():
    """讀取 Google Ads 憑證（優先個別環境變數，fallback 本機 YAML）"""
    from google.ads.googleads.client import GoogleAdsClient

    # 雲端：各欄位分開設定為環境變數
    dev_token = os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN')
    if dev_token:
        cfg = {
            'developer_token':  dev_token,
            'client_id':        os.environ.get('GOOGLE_ADS_CLIENT_ID', ''),
            'client_secret':    os.environ.get('GOOGLE_ADS_CLIENT_SECRET', ''),
            'refresh_token':    os.environ.get('GOOGLE_ADS_REFRESH_TOKEN', ''),
            'login_customer_id': GADS_CUSTOMER_ID,
            'use_proto_plus':   True,
        }
    else:
        # 本機：讀 YAML 檔
        import yaml
        yaml_path = os.path.expanduser('~/.claude/google-ads.yaml')
        with open(yaml_path) as f:
            cfg = yaml.safe_load(f)
        cfg.setdefault('login_customer_id', GADS_CUSTOMER_ID)

    return GoogleAdsClient.load_from_dict(cfg, version='v18')


def fetch_google_ads(days=30):
    """Google Ads 廣告活動成效（真實花費 / ROAS）"""
    from google.ads.googleads.errors import GoogleAdsException

    end_dt   = datetime.today() - timedelta(days=1)
    start_dt = datetime.today() - timedelta(days=days)
    end_str   = end_dt.strftime('%Y-%m-%d')
    start_str = start_dt.strftime('%Y-%m-%d')

    try:
        client     = _get_gads_client()
        ga_service = client.get_service('GoogleAdsService')

        query = f"""
            SELECT
                campaign.name,
                campaign.status,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{start_str}' AND '{end_str}'
              AND metrics.impressions > 0
            ORDER BY metrics.cost_micros DESC
            LIMIT 20
        """
        request = client.get_type('SearchGoogleAdsRequest')
        request.customer_id = GADS_CUSTOMER_ID
        request.query = query

        campaigns   = []
        tot_spend   = tot_rev = tot_clicks = tot_orders = 0

        for row in ga_service.search(request=request):
            m     = row.metrics
            spend = m.cost_micros / 1_000_000
            rev   = m.conversions_value
            clk   = int(m.clicks)
            conv  = m.conversions
            roas  = round(rev / spend, 2) if spend > 0 else 0.0
            campaigns.append({
                'name':        row.campaign.name,
                'spend':       round(spend),
                'clicks':      clk,
                'impressions': int(m.impressions),
                'revenue':     round(rev),
                'orders':      round(conv),
                'roas':        roas,
            })
            tot_spend  += spend
            tot_rev    += rev
            tot_clicks += clk
            tot_orders += conv

        campaigns.sort(key=lambda x: x['spend'], reverse=True)

        return {
            'period':    {'start': start_str, 'end': end_str, 'days': days},
            'summary':   {
                'spend':   round(tot_spend),
                'revenue': round(tot_rev),
                'orders':  round(tot_orders),
                'clicks':  tot_clicks,
                'roas':    round(tot_rev / tot_spend, 2) if tot_spend else 0.0,
            },
            'campaigns': campaigns,
            'note':      'google_ads_api',
        }

    except GoogleAdsException as ex:
        errors = '; '.join(e.message for e in ex.failure.errors)
        raise RuntimeError(f'Google Ads API 錯誤：{errors}')


# 保留舊名稱作為 server.py SECTION_MAP 的 alias
fetch_google_ads_via_ga4 = fetch_google_ads
