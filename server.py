#!/usr/bin/env python3
"""
Taiwan Access 行銷儀表板 server（獨立版）
提供分區塊 API /api/section/<name>，讓前端平行載入各 section。

用法:
  python3 server.py
  cloudflared tunnel --url http://localhost:5200

設定: config.json（或 Render 環境變數）
"""

import json, os, time, threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory, session, g

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY') or os.environ.get('ACCESS_TOKEN') or 'tw-dash-2026'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config():
    path = os.path.join(BASE_DIR, 'config.json')
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


CFG           = load_config()
ACCESS_TOKEN  = os.environ.get('ACCESS_TOKEN')  or CFG.get('access_token', '')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY') or CFG.get('anthropic_api_key', '')
PORT          = int(CFG.get('port', 5200))

# ── Per-section cache ─────────────────────────────────────
_section_cache      = {}          # {cache_key: {'data': ..., 'ts': float}}
_cache_lock         = threading.Lock()
DEFAULT_TTL         = 1800        # 30 分鐘（預設）

# 不同資料的更新頻率不同，給更長 TTL 減少重複抓取
SECTION_TTL = {
    'forecast':      4 * 3600,    # 4 小時：線性回歸預測日更
    'keyword_gaps':  4 * 3600,    # 4 小時：GSC 機會詞
    'yoy':           3 * 3600,    # 3 小時：年同期
    'mailchimp':     3 * 3600,    # 3 小時：活動不常更新
    'gsc':           2 * 3600,    # 2 小時：GSC 每日更新
    'gsc_pages':     2 * 3600,
    'instagram':     2 * 3600,
    'threads':       2 * 3600,
    'youtube':       2 * 3600,
    'cc1':           2 * 3600,
    'google_ads_kw': 2 * 3600,    # 2 小時：關鍵字層級
}

# 預熱時要跑的 section（排除慢速或不常用的）
WARM_SECTIONS = [
    'ecommerce', 'meta', 'gsc', 'meta_daily', 'ga4_extras',
    'gsc_pages', 'edm_utm', 'mailchimp', 'product_funnel',
    'google_ads', 'yoy', 'cc1',
]

# ── Section → fetcher mapping ─────────────────────────────
# mode: 'days' = 直接傳 days；'long60/90' = max(N, days)；'fixed' = 無參數
SECTION_MAP = {
    'ecommerce':     ('fetch_ga4_ecommerce',       'days'),
    'ga4_extras':    ('fetch_ga4_extras',           'days'),
    'meta':          ('fetch_meta_ads',             'days'),
    'meta_daily':    ('fetch_meta_daily',           'days'),
    'gsc':           ('fetch_gsc_keywords',         'days'),
    'gsc_pages':     ('fetch_gsc_pages',            'days'),
    'edm_utm':       ('fetch_edm_utm',              'days'),
    'mailchimp':     ('fetch_mailchimp',            'days'),
    'product_funnel':('fetch_ga4_product_funnel',   'days'),
    'yoy':           ('fetch_ga4_yoy',              'days'),
    'instagram':        ('fetch_instagram_insights',   'days'),
    'threads':          ('fetch_threads_insights',     'days'),
    'google_ads_kw':    ('fetch_google_ads_keywords',  'days'),
    'google_ads':    ('fetch_google_ads_via_ga4',   'days'),
    'youtube':       ('fetch_youtube',              'days'),
    'forecast':      ('fetch_revenue_forecast',     'long60'),
    'keyword_gaps':  ('fetch_keyword_gaps',         'long90'),
    'cc1':           ('fetch_cc1_progress',         'fixed'),
}


# ── Fetch helper (thread-safe, no Flask context needed) ───
def _make_cache_key(name, effective, start=None, end=None):
    return (name, start, end) if (start and end) else (name, effective)

def _fetch_one(name, effective, start=None, end=None):
    """執行一個 section 的資料抓取，透過 thread-local 傳遞日期範圍。"""
    import data_fetchers
    # 設定 thread-local 日期，讓 data_fetchers._date_range() 能讀到
    data_fetchers._thread_local.start_date = start
    data_fetchers._thread_local.end_date   = end
    fn_name, mode = SECTION_MAP[name]
    fn = getattr(data_fetchers, fn_name)
    return fn() if mode == 'fixed' else fn(effective)

def _get_cached(cache_key, name):
    """回傳 cache 命中的資料，否則 None。"""
    with _cache_lock:
        entry = _section_cache.get(cache_key)
    if not entry:
        return None
    ttl = SECTION_TTL.get(name, DEFAULT_TTL)
    if time.time() - entry['ts'] < ttl:
        return entry['data']
    return None

def _set_cached(cache_key, data):
    with _cache_lock:
        _section_cache[cache_key] = {'data': data, 'ts': time.time()}


def _warm_cache_bg(days=30):
    """啟動時在背景執行緒預熱 cache，讓第一個使用者不用等 API。"""
    time.sleep(2)  # 等 server 完全啟動
    now = time.time()
    to_fetch = []
    for name in WARM_SECTIONS:
        _, mode = SECTION_MAP[name]
        effective = max(60, days) if mode == 'long60' else max(90, days) if mode == 'long90' else days
        ck = _make_cache_key(name, effective)
        if _get_cached(ck, name) is None:
            to_fetch.append((name, effective))

    if not to_fetch:
        return

    print(f'[warm-up] 預熱 {len(to_fetch)} 個 section…')
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_one, name, eff): (name, eff) for name, eff in to_fetch}
        for future in as_completed(futures):
            name, eff = futures[future]
            try:
                data = future.result(timeout=45)
                _set_cached(_make_cache_key(name, eff), data)
                print(f'[warm-up] ✓ {name}')
            except Exception as e:
                print(f'[warm-up] ✗ {name}: {e}')


# ── Auth ──────────────────────────────────────────────────
def require_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('authenticated'):
            return f(*args, **kwargs)
        token = (
            request.headers.get('X-Access-Token') or
            request.form.get('token') or
            (request.get_json(silent=True) or {}).get('token', '')
        )
        if ACCESS_TOKEN and token != ACCESS_TOKEN:
            return jsonify({'error': 'Token 錯誤，請確認後重試'}), 401
        return f(*args, **kwargs)
    return decorated


# ── Routes ────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory(BASE_DIR, 'dashboard.html')


@app.route('/api/status')
def status():
    return jsonify({'token_required': bool(ACCESS_TOKEN)})


@app.route('/api/auth', methods=['POST'])
def auth():
    data  = request.get_json(silent=True) or {}
    token = data.get('token', '')
    if ACCESS_TOKEN and token != ACCESS_TOKEN:
        return jsonify({'ok': False, 'error': 'Token 錯誤'}), 401
    session['authenticated'] = True
    session.permanent = True
    return jsonify({'ok': True})


def _resolve_effective(mode, days):
    if   mode == 'long60': return max(60, days)
    elif mode == 'long90': return max(90, days)
    return days


@app.route('/api/section/<name>')
@require_token
def section_data(name):
    if name not in SECTION_MAP:
        return jsonify({'error': f'未知 section：{name}'}), 404

    days       = int(request.args.get('days', 30))
    start_date = request.args.get('start')
    end_date   = request.args.get('end')
    _, mode    = SECTION_MAP[name]
    effective  = _resolve_effective(mode, days)
    cache_key  = _make_cache_key(name, effective, start_date, end_date)

    cached = _get_cached(cache_key, name)
    if cached is not None:
        return jsonify(cached)

    try:
        result = _fetch_one(name, effective, start_date, end_date)
        _set_cached(cache_key, result)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/sections/batch')
@require_token
def sections_batch():
    """一次請求批次取得多個 section，伺服器端平行抓取。"""
    names      = [n for n in request.args.get('sections', '').split(',') if n in SECTION_MAP]
    days       = int(request.args.get('days', 30))
    start_date = request.args.get('start')
    end_date   = request.args.get('end')

    if not names:
        return jsonify({})

    results   = {}
    to_fetch  = []

    for name in names:
        _, mode   = SECTION_MAP[name]
        effective = _resolve_effective(mode, days)
        ck        = _make_cache_key(name, effective, start_date, end_date)
        cached    = _get_cached(ck, name)
        if cached is not None:
            results[name] = cached
        else:
            to_fetch.append((name, effective, ck))

    if to_fetch:
        workers = min(8, len(to_fetch))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_map = {
                ex.submit(_fetch_one, name, eff, start_date, end_date): (name, ck)
                for name, eff, ck in to_fetch
            }
            for future in as_completed(future_map):
                name, ck = future_map[future]
                try:
                    data = future.result(timeout=45)
                    _set_cached(ck, data)
                    results[name] = data
                except Exception as e:
                    results[name] = {'error': str(e)}

    return jsonify(results)


@app.route('/api/dashboard/refresh', methods=['POST'])
@require_token
def dashboard_refresh():
    with _cache_lock:
        _section_cache.clear()
    # 清空後立即背景預熱
    threading.Thread(target=_warm_cache_bg, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/api/ads/budget')
@require_token
def api_ads_budget():
    from ads_budget import get_all
    return jsonify(get_all())


@app.route('/api/report/drive', methods=['POST'])
@require_token
def upload_report_drive():
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaInMemoryUpload

    body_data  = request.get_json(force=True)
    report_txt = body_data.get('report_text', '')
    now        = time.strftime('%Y-%m-%d')
    fname      = f'Taiwan Access 行銷報告 {now}'

    token_json  = os.environ.get('GDRIVE_TOKEN_JSON')
    client_json = os.environ.get('GDRIVE_CLIENT_JSON')
    if token_json and client_json:
        t = json.loads(token_json)
        c = json.loads(client_json).get('web', json.loads(client_json))
    else:
        token_path  = os.path.expanduser('~/.claude/gdrive-write-token.json')
        client_path = os.path.expanduser('~/.claude/gdrive-credentials.json')
        if not os.path.exists(token_path):
            return jsonify({'error': '未設定 Google Drive 憑證'}), 400
        with open(token_path) as f:  t = json.load(f)
        with open(client_path) as f: c = json.load(f).get('web', {})

    creds = Credentials(
        token=t.get('access_token'),
        refresh_token=t.get('refresh_token'),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=c.get('client_id'),
        client_secret=c.get('client_secret'),
    )
    if not creds.valid:
        creds.refresh(Request())

    drive  = build('drive', 'v3', credentials=creds)
    folder = os.environ.get('GDRIVE_FOLDER_ID') or load_config().get('gdrive_folder_id')
    meta   = {'name': fname, 'mimeType': 'application/vnd.google-apps.document'}
    if folder: meta['parents'] = [folder]
    media  = MediaInMemoryUpload(report_txt.encode('utf-8'), mimetype='text/plain; charset=utf-8')
    result = drive.files().create(body=meta, media_body=media, fields='id,webViewLink').execute()
    return jsonify({'ok': True, 'link': result['webViewLink'], 'name': fname})


@app.route('/api/dashboard/insights', methods=['POST'])
@require_token
def dashboard_insights():
    key = os.environ.get('ANTHROPIC_API_KEY') or ANTHROPIC_KEY
    if not key:
        return jsonify({'error': '未設定 Anthropic API Key'}), 400

    data  = request.get_json(force=True)
    ec    = data.get('ecommerce', {})
    mt    = data.get('meta', {})
    gsc   = data.get('gsc', {})
    days  = data.get('days', 30)
    kpi   = ec.get('kpi', {})
    mkpi  = mt.get('meta_kpi') or mt.get('kpi', {})
    camps = mt.get('campaigns', [])
    kwds  = gsc.get('keywords', [])

    summary = f"""台灣高空（專業音響器材電商）近 {days} 天行銷數據：

【GA4 電商】
- 營收：${kpi.get('revenue',{}).get('value',0):,}（vs 上期 {kpi.get('revenue',{}).get('change',0):+.1f}%）
- 訂單數：{kpi.get('orders',{}).get('value',0)}（vs 上期 {kpi.get('orders',{}).get('change',0):+.1f}%）
- 平均客單：${kpi.get('aov',{}).get('value',0):,}
- 整體轉換率：{kpi.get('cvr',0):.2f}%

【Meta Ads】
- 總花費：${mkpi.get('spend',0):,}
- 帶來營收：${mkpi.get('revenue',0):,}
- 整體 ROAS：{mkpi.get('roas',0):.2f}x
- 廣告活動明細：
"""
    for c in camps[:6]:
        summary += f"  - {c['name']}：花費 ${c.get('spend',0):,}，ROAS {c.get('roas',0)}x\n"

    top_opps = [k for k in kwds if 4 <= k.get('position',99) <= 15 and k.get('impressions',0) > 100]
    if top_opps:
        summary += "\n【GSC 機會關鍵字（排名 4–15）】\n"
        for k in top_opps[:5]:
            summary += f"  - \"{k['keyword']}\" 排名 {k['position']}，曝光 {k['impressions']:,}，CTR {k['ctr']}%\n"

    prompt = summary + """
請以台灣高空數位行銷顧問身份，根據以上數據提供 5 條具體可執行的廣告優化建議。
每條建議格式：
**[建議標題]**
具體做法（1–2 句）。預期效益。

請用繁體中文，語氣專業但簡潔，只輸出 5 條建議，不要其他說明。"""

    payload = json.dumps({
        'model': 'claude-haiku-4-5-20251001',
        'max_tokens': 1000,
        'messages': [{'role': 'user', 'content': prompt}]
    }).encode()
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages', data=payload,
        headers={
            'Content-Type': 'application/json',
            'x-api-key': key,
            'anthropic-version': '2023-06-01',
        }, method='POST'
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    return jsonify({'insights': result['content'][0]['text']})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', PORT))
    print(f'📊 Taiwan Access 行銷儀表板')
    print(f'   Token  ：{"✓ 已啟用" if ACCESS_TOKEN else "✗ 未設定"}')
    print(f'   Claude ：{"✓ 已設定" if ANTHROPIC_KEY else "⚠ 未設定（AI 建議不可用）"}')
    print(f'   本機   ：http://localhost:{port}')
    # 啟動背景預熱
    threading.Thread(target=_warm_cache_bg, daemon=True).start()
    app.run(host='0.0.0.0', port=port, debug=False)
else:
    # 被 gunicorn 載入時也預熱
    threading.Thread(target=_warm_cache_bg, daemon=True).start()
