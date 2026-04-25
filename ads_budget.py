"""
廣告預算監控 — Google Ads & Meta Marketing API
讀取目前各平台廣告活動的預算使用狀況，超過門檻時標記警告。
"""
import os
import json
import requests
from datetime import date
from pathlib import Path

WARNING_PCT = 70   # % — 黃色警告
DANGER_PCT  = 85   # % — 紅色警告


# ── 工具 ─────────────────────────────────────────────────
def _status(spent: float, total: float) -> str:
    """回傳 'ok' / 'warning' / 'danger'。"""
    if total <= 0:
        return 'ok'
    pct = spent / total * 100
    if pct >= DANGER_PCT:
        return 'danger'
    if pct >= WARNING_PCT:
        return 'warning'
    return 'ok'


def _pct(spent: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return round(min(spent / total * 100, 100), 1)


# ── Google Ads ───────────────────────────────────────────
def _google_cfg() -> dict:
    """讀取 Google Ads 設定，環境變數優先（Render），本地 yaml 備用。"""
    if os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN'):
        return {
            'developer_token': os.environ['GOOGLE_ADS_DEVELOPER_TOKEN'],
            'client_id':       os.environ['GOOGLE_ADS_CLIENT_ID'],
            'client_secret':   os.environ['GOOGLE_ADS_CLIENT_SECRET'],
            'refresh_token':   os.environ['GOOGLE_ADS_REFRESH_TOKEN'],
            'customer_id':     os.environ.get('GOOGLE_ADS_CUSTOMER_ID', '7245588980'),
        }
    yaml_path = Path.home() / '.claude/google-ads.yaml'
    if yaml_path.exists():
        try:
            import yaml
            cfg = yaml.safe_load(yaml_path.read_text())
            cfg.setdefault('customer_id', '7245588980')
            return cfg
        except Exception:
            pass
    return {}


def _google_token(cfg: dict) -> str:
    resp = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id':     cfg['client_id'],
        'client_secret': cfg['client_secret'],
        'refresh_token': cfg['refresh_token'],
        'grant_type':    'refresh_token',
    }, timeout=10)
    return resp.json().get('access_token', '')


def fetch_google_ads() -> dict:
    """
    回傳 Google Ads 帳戶當天費用 + 活躍活動清單。
    {'ok': bool, 'campaigns': [...], 'total_spent_today': float, 'currency': 'TWD', 'error': '...'}
    """
    cfg = _google_cfg()
    if not cfg:
        return {'ok': False, 'error': '未設定 Google Ads 憑證（GOOGLE_ADS_DEVELOPER_TOKEN 等環境變數）'}

    try:
        token = _google_token(cfg)
        if not token:
            return {'ok': False, 'error': '無法取得 Google Access Token'}

        customer_id = cfg['customer_id'].replace('-', '')
        headers = {
            'Authorization':       f'Bearer {token}',
            'developer-token':     cfg['developer_token'],
            'Content-Type':        'application/json',
        }
        # 若為 MCC 帳號需加 login-customer-id；一般帳號不需要

        query = """
            SELECT
              campaign.id,
              campaign.name,
              campaign.status,
              campaign_budget.amount_micros,
              campaign_budget.type,
              metrics.cost_micros,
              metrics.impressions,
              metrics.clicks
            FROM campaign
            WHERE campaign.status IN ('ENABLED', 'PAUSED')
              AND segments.date DURING TODAY
        """
        url  = f'https://googleads.googleapis.com/v17/customers/{customer_id}/googleAds:search'
        resp = requests.post(url, headers=headers, json={'query': query.strip()}, timeout=15)
        data = resp.json()

        if 'error' in data:
            return {'ok': False, 'error': data['error'].get('message', str(data['error']))}

        campaigns = []
        total_spent = 0.0
        for row in data.get('results', []):
            c     = row.get('campaign', {})
            b     = row.get('campaignBudget', {})
            m     = row.get('metrics', {})
            spent = int(m.get('costMicros', 0)) / 1_000_000
            budget = int(b.get('amountMicros', 0)) / 1_000_000
            total_spent += spent
            campaigns.append({
                'id':       c.get('id', ''),
                'name':     c.get('name', ''),
                'status':   c.get('status', ''),
                'budget':   round(budget, 0),
                'spent':    round(spent, 0),
                'pct':      _pct(spent, budget),
                'status_level': _status(spent, budget),
                'impressions': int(m.get('impressions', 0)),
                'clicks':      int(m.get('clicks', 0)),
            })

        campaigns.sort(key=lambda x: x['spent'], reverse=True)
        return {
            'ok': True,
            'campaigns': campaigns,
            'total_spent_today': round(total_spent, 0),
            'currency': 'TWD',
        }

    except Exception as e:
        return {'ok': False, 'error': str(e)}


# ── Meta Ads ─────────────────────────────────────────────
def fetch_meta_ads() -> dict:
    """
    回傳 Meta 廣告帳戶今日花費 + 活躍活動預算。
    需環境變數：META_ACCESS_TOKEN、META_AD_ACCOUNT_ID（格式：act_123456）
    """
    token      = os.environ.get('META_ACCESS_TOKEN', '')
    account_id = os.environ.get('META_AD_ACCOUNT_ID', '')

    if not token or not account_id:
        return {'ok': False, 'error': '未設定 META_ACCESS_TOKEN 或 META_AD_ACCOUNT_ID'}

    if not account_id.startswith('act_'):
        account_id = f'act_{account_id}'

    base = 'https://graph.facebook.com/v19.0'
    try:
        # 帳戶今日花費
        acct_resp = requests.get(
            f'{base}/{account_id}/insights',
            params={
                'fields':      'spend,impressions,clicks',
                'date_preset': 'today',
                'level':       'account',
                'access_token': token,
            },
            timeout=15,
        ).json()
        acct_data = (acct_resp.get('data') or [{}])[0]
        acct_spent = float(acct_data.get('spend', 0))

        # 帳戶花費上限
        acct_info = requests.get(
            f'{base}/{account_id}',
            params={'fields': 'name,currency,spend_cap,amount_spent', 'access_token': token},
            timeout=10,
        ).json()
        if 'error' in acct_info:
            return {'ok': False, 'error': acct_info['error'].get('message', str(acct_info['error']))}

        spend_cap   = float(acct_info.get('spend_cap', 0)) / 100    # 分 → 元
        amount_spent_total = float(acct_info.get('amount_spent', 0)) / 100
        currency    = acct_info.get('currency', 'TWD')

        # 活躍活動
        camp_resp = requests.get(
            f'{base}/{account_id}/campaigns',
            params={
                'fields':      'name,status,daily_budget,lifetime_budget,budget_remaining',
                'filtering':   '[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]',
                'access_token': token,
            },
            timeout=15,
        ).json()

        campaigns = []
        for c in camp_resp.get('data', []):
            daily    = float(c.get('daily_budget', 0)) / 100
            lifetime = float(c.get('lifetime_budget', 0)) / 100
            remain   = float(c.get('budget_remaining', 0)) / 100
            total_b  = daily if daily > 0 else lifetime
            spent_b  = max(total_b - remain, 0) if total_b > 0 else 0
            campaigns.append({
                'name':         c.get('name', ''),
                'status':       c.get('status', ''),
                'daily_budget': round(daily, 0),
                'lifetime_budget': round(lifetime, 0),
                'budget_remaining': round(remain, 0),
                'spent':        round(spent_b, 0),
                'pct':          _pct(spent_b, total_b),
                'status_level': _status(spent_b, total_b),
                'budget_type':  'daily' if daily > 0 else 'lifetime',
            })

        campaigns.sort(key=lambda x: x['pct'], reverse=True)
        return {
            'ok':              True,
            'campaigns':       campaigns,
            'today_spend':     round(acct_spent, 0),
            'total_spend':     round(amount_spent_total, 0),
            'spend_cap':       round(spend_cap, 0),
            'spend_cap_pct':   _pct(amount_spent_total, spend_cap),
            'spend_cap_status': _status(amount_spent_total, spend_cap),
            'currency':        currency,
            'account_name':    acct_info.get('name', ''),
        }

    except Exception as e:
        return {'ok': False, 'error': str(e)}


def get_all() -> dict:
    """同時取回兩個平台資料，並計算是否有需要顯示的警告。"""
    google = fetch_google_ads()
    meta   = fetch_meta_ads()

    # 是否有任何警告
    has_warning = False
    has_danger  = False

    if google.get('ok'):
        for c in google.get('campaigns', []):
            if c['status_level'] == 'danger':   has_danger = True
            if c['status_level'] == 'warning':  has_warning = True

    if meta.get('ok'):
        if meta.get('spend_cap_status') == 'danger':   has_danger = True
        if meta.get('spend_cap_status') == 'warning':  has_warning = True
        for c in meta.get('campaigns', []):
            if c['status_level'] == 'danger':   has_danger = True
            if c['status_level'] == 'warning':  has_warning = True

    alert = 'danger' if has_danger else ('warning' if has_warning else 'ok')

    return {
        'google':    google,
        'meta':      meta,
        'alert':     alert,
        'updated_at': date.today().isoformat(),
    }
