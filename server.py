#!/usr/bin/env python3
"""
Taiwan Access 行銷儀表板 server（獨立版）
提供分區塊 API /api/section/<name>，讓前端平行載入各 section。

用法:
  python3 server.py
  cloudflared tunnel --url http://localhost:5200

設定: config.json（或 Render 環境變數）
"""

import json, os, time
import urllib.request
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory, session

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
_section_cache = {}   # {(section, days): {'data': ..., 'ts': float}}
CACHE_TTL      = 1800  # 30 分鐘

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
    'instagram':     ('fetch_instagram_insights',   'days'),
    'threads':       ('fetch_threads_insights',     'days'),
    'google_ads':    ('fetch_google_ads_via_ga4',   'days'),
    'forecast':      ('fetch_revenue_forecast',     'long60'),
    'keyword_gaps':  ('fetch_keyword_gaps',         'long90'),
    'cc1':           ('fetch_cc1_progress',         'fixed'),
}


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


@app.route('/api/section/<name>')
@require_token
def section_data(name):
    if name not in SECTION_MAP:
        return jsonify({'error': f'未知 section：{name}'}), 404

    days               = int(request.args.get('days', 30))
    fn_name, mode      = SECTION_MAP[name]

    if   mode == 'long60': effective = max(60, days)
    elif mode == 'long90': effective = max(90, days)
    else:                  effective = days

    cache_key = (name, effective)
    now       = time.time()
    if cache_key in _section_cache and now - _section_cache[cache_key]['ts'] < CACHE_TTL:
        return jsonify(_section_cache[cache_key]['data'])

    import data_fetchers
    fn = getattr(data_fetchers, fn_name)
    try:
        result = fn() if mode == 'fixed' else fn(effective)
        _section_cache[cache_key] = {'data': result, 'ts': now}
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/dashboard/refresh', methods=['POST'])
@require_token
def dashboard_refresh():
    _section_cache.clear()
    return jsonify({'ok': True})


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
    app.run(host='0.0.0.0', port=port, debug=False)
