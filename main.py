# -*- coding: utf-8 -*-
"""
Yahooニュース統合スクレイパー（国内8社対応版・GitHub Actions用）
 - Yahooシートにニュース一覧＋本文（最大10ページ）を書き込み
 - Commentsシートにコメント（最大10ページ）を書き込み
 - Gemini を使って 100件まとめてバッチ分析し、以下を判定
   * 主題企業 (P列)
   * カテゴリ (Q列)
   * ポジネガ (R列)
   * 本文中の日産関連文抽出 (S列)
   * 本文中の日産に対するネガティブ文抽出 (T列)
"""
import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# --- Gemini API 関連 ---
from google import genai
from google.api_core.exceptions import ResourceExhausted

# ================== 設定 ==================
SHARED_SPREADSHEET_ID = "1WUnLv7TIxY1-PPyLAmks2CEfAcfse51He32l79eUf7E"
KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
COMMENTS_SHEET_NAME = "Comments"
MAX_SHEET_ROWS_FOR_REPLACE = 10000

# 本文・コメント取得の最大ページ数
MAX_BODY_PAGES = 10
MAX_COMMENT_PAGES = 10

# コメント総数の上限（それ以上あれば「…(over 3000)」等追記）
MAX_COMMENTS_TOTAL = 3000

# Yahoo シートのヘッダ定義
# A:URL, B:タイトル, C:投稿日時, D:ソース,
# E〜N: 本文(1〜10ページ), O:コメント数,
# P:主題企業, Q:カテゴリ, R:ポジネガ, S:日産関連文, T:日産ネガ文
YAHOO_SHEET_HEADERS = [
    "URL",  # A
    "タイトル",  # B
    "投稿日時",  # C
    "ソース",  # D
    "本文_P1",  # E
    "本文_P2",  # F
    "本文_P3",  # G
    "本文_P4",  # H
    "本文_P5",  # I
    "本文_P6",  # J
    "本文_P7",  # K
    "本文_P8",  # L
    "本文_P9",  # M
    "本文_P10", # N
    "コメント数",  # O
    "主題企業",    # P
    "カテゴリ",    # Q
    "ポジネガ",    # R
    "日産関連文",  # S
    "日産ネガ文"   # T
]

# Comments シートのヘッダ定義
# A:URL, B:タイトル, C:投稿日時, D:ソース, E:コメント数, F〜:コメントページ
COMMENTS_SHEET_HEADERS = [
    "URL",    # A
    "タイトル",  # B
    "投稿日時",  # C
    "ソース",   # D
    "コメント数" # E
    # F〜: コメント_P1〜P10 を追加
]

REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))
PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt"
]

# Gemini バッチサイズ（100件を1APIで処理）
GEMINI_MAX_BATCH_SIZE = 100

# ===== Gemini Client 初期化 =====
try:
    GEMINI_CLIENT = genai.Client()
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None

GEMINI_BATCH_PROMPT_BASE = None  # バッチ用プロンプトテンプレート

# ================== ヘルパー関数 ==================
def to_str_safe(x) -> str:
    """None/数値/文字列を安全に文字列へ。前後空白は除去。"""
    if x is None:
        return ""
    try:
        return str(x).strip()
    except Exception:
        return ""

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj: datetime) -> str:
    """yyyy/mm/dd hh:mm:ss 形式にフォーマット"""
    return dt_obj.strftime("%Y/%m/%d %H:%M:%S")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    """Yahoo表示の日時文字列を datetime(JST) に変換"""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return None
    if isinstance(raw, str):
        s = raw.strip()
        # （月）などの曜日を削除
        s = re.sub(r"\([\u670月火水木金土日]\)", "", s).strip()
        # 「配信」を削除
        s = s.replace("配信", "").strip()
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%y/%m/%d %H:%M", "%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31):
                    dt = dt.replace(year=dt.year - 1)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                continue
    return None

def build_gspread_client() -> gspread.Client:
    """サービスアカウントで gspread クライアントを生成"""
    try:
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            return gspread.service_account(filename="credentials.json")
    except FileNotFoundError:
        raise RuntimeError("Google認証情報が見つかりません (環境変数 GCP_SERVICE_ACCOUNT_KEY または credentials.json を確認)")
    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    """keywords.txt からキーワードを読み込み"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        if not keywords:
            raise ValueError("キーワードファイルに有効なキーワードが含まれていません。")
        return keywords
    except FileNotFoundError:
        print(f"致命的エラー: キーワードファイル '{filename}' が見つかりません。")
        return []
    except Exception as e:
        print(f"キーワードファイルの読み込みエラー: {e}")
        return []

def load_gemini_batch_prompt() -> str:
    """
    PROMPT_FILES を読み込み、バッチ分析用のプロンプトテンプレートを作成。
    100件まとめて JSON 配列で返すように指示する。
    """
    global GEMINI_BATCH_PROMPT_BASE
    if GEMINI_BATCH_PROMPT_BASE is not None:
        return GEMINI_BATCH_PROMPT_BASE
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        role_path = os.path.join(script_dir, PROMPT_FILES[0])
        with open(role_path, "r", encoding="utf-8") as f:
            role_instruction = f.read().strip()

        other_contents = []
        for filename in PROMPT_FILES[1:]:
            path = os.path.join(script_dir, filename)
            with open(path, "r", encoding="utf-8") as f:
                c = f.read().strip()
                if c:
                    other_contents.append(c)

        if not role_instruction or not other_contents:
            print("致命的エラー: プロンプトファイルの内容が不完全または空です。")
            GEMINI_BATCH_PROMPT_BASE = ""
            return GEMINI_BATCH_PROMPT_BASE

        base = role_instruction + "\n" + "\n".join(other_contents)
        extra = """ 
追加要件:
- 与えられる記事本文は最大で100件です。
- それぞれの記事について、以下の情報を判定してください。
 * company_info: 記事の主題企業名。共同開発などがあれば () 内に別企業も書いてください。
 * category: PROMPTで指定のカテゴリ分類に従ってください。
 * sentiment: 記事全体のトーンを「ポジティブ」「ネガティブ」「ニュートラル」のいずれかで判定してください。
 * nissan_related: 記事本文中で「日産」や「NISSAN」「ニッサン」などの言及文を抽出。なければ "N/A"。
 * nissan_negative: 上記のうちネガティブな印象の文のみ抽出。なければ "N/A"。

出力フォーマット: JSON 配列（各要素: index, company_info, category, sentiment, nissan_related, nissan_negative）
入力フォーマット: "==== ARTICLE i START ===="〜"==== ARTICLE i END ====" が index=i の本文
"""
        base += "\n\n" + extra + "\n\n{TEXT_BATCH}"
        GEMINI_BATCH_PROMPT_BASE = base
        print(f"Gemini バッチ用プロンプトテンプレートを {PROMPT_FILES} から読み込みました。")
        return GEMINI_BATCH_PROMPT_BASE
    except FileNotFoundError as e:
        print(f"致命的エラー: プロンプトファイルの一部が見つかりません。ファイル名: {e.filename}")
        GEMINI_BATCH_PROMPT_BASE = ""
        return GEMINI_BATCH_PROMPT_BASE
    except Exception as e:
        print(f"致命的エラー: プロンプトファイルの読み込み中にエラー: {e}")
        GEMINI_BATCH_PROMPT_BASE = ""
        return GEMINI_BATCH_PROMPT_BASE

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    """requests.get をリトライ付きで実行。404 の場合は即 None"""
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404:
                print(f" ❌ 404 Not Found: {url}")
                return None
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.random()
                print(f" ⚠️ 接続エラー, リトライ {attempt+1}/{max_retries} ({wait_time:.2f}秒): {e}")
                time.sleep(wait_time)
            else:
                print(f" ❌ 最終リトライ失敗: {e}")
                return None
    return None

# ================== 検索結果から記事URL抽出（HTML＋テキスト両対応） ==================
def extract_article_urls(page_html: str) -> List[str]:
    """
    検索結果ページの HTML/テキストから、記事URL（https://news.yahoo.co.jp/articles/xxxxx）を収集。
    BeautifulSoupでの<a>抽出と、正規表現の両方で拾い、重複除去して返す。
    """
    urls: List[str] = []

    # 1) HTMLアンカーから抽出
    try:
        soup = BeautifulSoup(page_html, "html.parser")
        for a in soup.select('a[href^="https://news.yahoo.co.jp/articles/"]'):
            href = a.get("href", "")
            if href and href.startswith("https://news.yahoo.co.jp/articles/"):
                urls.append(href)
    except Exception:
        pass

    # 2) プレーンテキスト中の URL を正規表現で抽出（Markdown風のテキストにも対応）
    try:
        regex = re.compile(r"https://news\.yahoo\.co\.jp/articles/[A-Za-z0-9]+")
        urls.extend(regex.findall(page_html))
    except Exception:
        pass

    # 重複除去＆順序保持
    seen = set()
    unique_urls = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    return unique_urls

# ================== 記事ページからメタ情報取得 ==================
def fetch_article_meta(url: str) -> Dict[str, str]:
    """
    記事ページから タイトル／投稿日時（文字列）／ソース（提供社名） を取得。
    ページ構造の差異に耐えるため、複数の候補を試す。
    """
    meta = {"タイトル": "", "投稿日時": "", "ソース": ""}

    res = request_with_retry(url)
    if not res:
        return meta

    soup = BeautifulSoup(res.text, "html.parser")

    # タイトル: h1 または og:title
    title = ""
    h1 = soup.find("h1")
    if h1 and to_str_safe(h1.text):
        title = to_str_safe(h1.text)
    else:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = to_str_safe(og.get("content"))

    # 日時: <time> のテキストを優先
    post_str = ""
    time_tag = soup.find("time")
    if time_tag and to_str_safe(time_tag.text):
        post_str = to_str_safe(time_tag.text)
    else:
        # 文章内から "11/19(水)17:45配信" などを探索
        m = re.search(r"\d{1,2}/\d{1,2}\(.+?\)\d{1,2}:\d{2}\s*配信", soup.get_text())
        if m:
            post_str = to_str_safe(m.group(0))

    # ソース: 提供社リンクやラベル
    source = ""
    # 提供社へのリンクっぽい要素
    provider_candidates = [
        ("a", {"href": re.compile(r"/media/")}),
        ("a", {"class": re.compile(r"provider|media", re.I)}),
        ("span", {"class": re.compile(r"provider|media", re.I)}),
        ("div", {"class": re.compile(r"provider|media", re.I)}),
    ]
    for name, attrs in provider_candidates:
        el = soup.find(name, attrs=attrs)
        if el and to_str_safe(el.get_text()):
            source = to_str_safe(el.get_text())
            break
    if not source:
        # ページのテキストから候補抽出（例: "時事通信", "日刊スポーツ" など）
        candidates = ["時事通信", "日刊スポーツ", "スポーツ報知", "J-CASTニュース", "沖縄タイムス",
                      "共同通信", "朝日新聞", "読売新聞", "毎日新聞", "産経新聞", "NHK"]
        text = soup.get_text()
        for name in candidates:
            if name in text:
                source = name
                break

    meta["タイトル"] = title
    meta["投稿日時"] = post_str if post_str else "取得不可"
    meta["ソース"]   = source if source else "取得不可"
    return meta

# ================== Yahooニュース検索 (Selenium) ==================
def get_yahoo_news_with_selenium(keyword: str) -> List[Dict[str, str]]:
    print(f" Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f" WebDriver 初期化に失敗しました: {e}")
        return []

    search_url = (
        f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8"
        "&categories=domestic,world,business,it,science,life,local"
    )
    driver.get(search_url)
    time.sleep(3)  # 検索結果の初期レンダリング待ち（構造変化対策で固定秒）

    page_html = driver.page_source
    driver.quit()

    # 記事URLの抽出（HTML＋テキスト両対応）
    urls = extract_article_urls(page_html)
    if not urls:
        print(" ⚠️ 記事URLを検出できませんでした。構造変更の可能性があります。")
        return []

    articles_data: List[Dict[str, str]] = []
    for url in urls:
        # 記事ページ側からメタ情報取得（タイトル・日時・ソース）
        meta = fetch_article_meta(url)
        if url:
            articles_data.append({
                "URL": url,
                "タイトル": meta.get("タイトル", "") or "",
                "投稿日時": meta.get("投稿日時", "") or "取得不可",
                "ソース": meta.get("ソース", "") or "取得不可",
            })

    print(f" Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

# ================== シート操作ヘルパー ==================
def ensure_yahoo_sheet(gc: gspread.Client) -> gspread.Worksheet:
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=SOURCE_SHEET_NAME,
            rows=str(MAX_SHEET_ROWS_FOR_REPLACE),
            cols=str(len(YAHOO_SHEET_HEADERS)),
        )
    headers = ws.row_values(1)
    if headers != YAHOO_SHEET_HEADERS:
        ws.update(
            range_name=f"A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}",
            values=[YAHOO_SHEET_HEADERS],
        )
    return ws

def ensure_comments_sheet(gc: gspread.Client) -> gspread.Worksheet:
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(COMMENTS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=COMMENTS_SHEET_NAME,
            rows=str(MAX_SHEET_ROWS_FOR_REPLACE),
            cols=str(5 + MAX_COMMENT_PAGES),
        )
        ws.update(
            range_name=f"A1:{gspread.utils.rowcol_to_a1(1, 5 + MAX_COMMENT_PAGES)}",
            values=[COMMENTS_SHEET_HEADERS + [f"コメント_P{i}" for i in range(1, MAX_COMMENT_PAGES + 1)]],
        )
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: List[Dict[str, str]]) -> None:
    ws = ensure_yahoo_sheet(gc)
    existing = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    existing_urls = set(
        to_str_safe(row[0])
        for row in existing[1:]
        if len(row) > 0 and to_str_safe(row[0]).startswith("http")
    )

    new_rows = []
    for a in articles:
        if a["URL"] in existing_urls:
            continue
        row = [
            a["URL"],
            a["タイトル"],
            a["投稿日時"],
            a["ソース"],
        ]
        row.extend([""] * (len(YAHOO_SHEET_HEADERS) - len(row)))
        new_rows.append(row)

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f" Yahooシートに {len(new_rows)} 件追記しました。")
    else:
        print(" 追記すべき新規記事はありません。")

# ================== 本文・コメント取得 ==================
def fetch_article_body_page(url: str) -> str:
    """記事の単一ページから本文テキストを抽出"""
    res = request_with_retry(url)
    if not res:
        return ""
    soup = BeautifulSoup(res.text, "html.parser")
    article_content = (
        soup.find("article")
        or soup.find("div", class_="article_body")
        or soup.find("div", class_=re.compile(r"article_detail|article_body"))
    )
    texts: List[str] = []
    if article_content:
        paragraphs = article_content.find_all("p", class_=re.compile(r"highLightSearchTarget"))
        if not paragraphs:
            paragraphs = article_content.find_all("p")
        for p in paragraphs:
            t = p.get_text(strip=True)
            if t:
                texts.append(t)
    return "\n".join(texts).strip()

def fetch_comments_page(url: str) -> List[str]:
    """
    コメントページ1枚分からコメント本文をリストで返す。
    YahooのHTML構造変化に強めの“範囲指定＋限定セレクタ”＋“ノイズ除去”で抽出する。
    """
    res = request_with_retry(url)
    if not res:
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    # コメント領域の起点候補
    container_candidates = [
        {"name": "section", "attrs": {"id": re.compile(r"^comments?$", re.I)}},
        {"name": "section", "attrs": {"data-testid": re.compile(r"comment", re.I)}},
        {"name": "div",     "attrs": {"id": re.compile(r"^comment", re.I)}},
        {"name": "div",     "attrs": {"class": re.compile(r"^comment", re.I)}},
        {"name": "div",     "attrs": {"class": re.compile(r"CommentItem")}},
        {"name": "section", "attrs": {"class": re.compile(r"comments|Comment", re.I)}},
    ]

    root = None
    for cand in container_candidates:
        root = soup.find(cand["name"], attrs=cand["attrs"])
        if root:
            break
    scope = root if root else soup

    comments: List[str] = []

    # コメント本文っぽい限定セレクタのみ
    strict_selectors = [
        "div[class*='CommentItem__body']",
        "div[class*='CommentItem__text']",
        "p[class*='CommentItem__body']",
        "p[class*='CommentItem__text']",
        "span[class*='CommentItem__body']",
        "span[class*='CommentItem__text']",
        "div[class*='CommentItem'] p",
        "div[class*='CommentItem'] span",
    ]

    # ノイズ定型文除去
    noise_patterns = [
        r"^\s*コメントを書く\s*$",
        r"^\s*ヤフコメポリシー\s*$",
        r"^\s*PayPay残高使えます\s*$",
        r"税込\s*\d+\s*円",
        r"\d{1,2}/\d{1,2}\(.+?\)\d{1,2}:\d{2}\s*配信",
        r"^\s*ABEMA\s*TIMES.*配信\s*$",
        r"^\s*沖縄タイムス.*$",
        r"^\s*【.+?】.*$",
        r"^\s*前職.*選.*$",
    ]
    noise_re = re.compile("|".join(noise_patterns), re.I)

    def is_comment_text(text: str) -> bool:
        if not text:
            return False
        if len(text) < 6:
            return False
        if noise_re.search(text):
            return False
        return True

    for sel in strict_selectors:
        for node in scope.select(sel):
            text = node.get_text(strip=True)
            if is_comment_text(text) and text not in comments:
                comments.append(text)

    # 何も拾えない場合の最終手段（範囲限定）
    if not comments and root:
        for node in root.find_all(["p", "span", "div"]):
            cls = " ".join(node.get("class", []))
            if not re.search(r"CommentItem|comment", cls, re.I):
                continue
            text = node.get_text(strip=True)
            if is_comment_text(text) and text not in comments:
                comments.append(text)

    return comments

def fetch_details_and_update(gc: gspread.Client) -> None:
    """
    各行について:
     - 本文10ページ分 (E〜N) を取得・更新
     - コメント数 (O) を更新
     - Commentsシートにコメント10ページ分を書き込み
    既に本文P1が入っている行はスキップ（再取得しない）
    """
    yahoo_ws = ensure_yahoo_sheet(gc)
    comments_ws = ensure_comments_sheet(gc)
    values = yahoo_ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if len(values) <= 1:
        print(" Yahooシートにデータがありません。本文/コメント取得をスキップします。")
        return

    data_rows = values[1:]
    print("\n===== 📄 ステップ2: 本文＆コメント取得・Commentsシート更新 =====")

    comments_values = comments_ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    comments_url_to_row: Dict[str, int] = {}
    if comments_values:
        for idx, row in enumerate(comments_values[1:], start=2):
            if len(row) > 0 and to_str_safe(row[0]).startswith("http"):
                comments_url_to_row[to_str_safe(row[0])] = idx

    update_body_count = 0
    update_comments_count = 0

    for idx, row in enumerate(data_rows, start=2):
        if len(row) < len(YAHOO_SHEET_HEADERS):
            row.extend([""] * (len(YAHOO_SHEET_HEADERS) - len(row)))

        url = to_str_safe(row[0])
        if not url.startswith("https://news.yahoo.co.jp/articles/"):
            continue

        title = to_str_safe(row[1])
        post_date = to_str_safe(row[2])
        source = to_str_safe(row[3])

        body_pages = row[4:4 + MAX_BODY_PAGES]
        comment_count_str = to_str_safe(row[14]) if len(row) > 14 else ""

        need_body = not to_str_safe(body_pages[0])
        need_comments = True

        new_body_pages = list(body_pages)
        new_comment_count = comment_count_str

        if need_body or need_comments:
            print(f" - 行 {idx} (記事: {title[:20]}...): 詳細取得中...")

        # 本文取得（最大10ページ）
        if need_body:
            body_changed = False
            for page_idx in range(1, MAX_BODY_PAGES + 1):
                page_url = url if page_idx == 1 else f"{url}?page={page_idx}"
                text = fetch_article_body_page(page_url)
                if not text:
                    if page_idx == 1:
                        new_body_pages[0] = "本文取得不可"
                        body_changed = True
                    break

                col_idx = page_idx - 1
                if to_str_safe(new_body_pages[col_idx]) != text:
                    new_body_pages[col_idx] = text
                    body_changed = True

            if body_changed:
                yahoo_ws.update(
                    range_name=f"E{idx}:N{idx}",
                    values=[new_body_pages],
                    value_input_option="USER_ENTERED",
                )
                update_body_count += 1
                time.sleep(1 + random.random() * 0.5)

        # コメント取得（最大10ページ）
        if need_comments:
            all_comments: List[str] = []
            page_strings: List[str] = [""] * MAX_COMMENT_PAGES

            for page_idx in range(1, MAX_COMMENT_PAGES + 1):
                c_url = url + ("/comments" if page_idx == 1 else f"/comments?page={page_idx}")
                comments = fetch_comments_page(c_url)
                if not comments:
                    break

                if len(all_comments) + len(comments) > MAX_COMMENTS_TOTAL:
                    remaining = MAX_COMMENTS_TOTAL - len(all_comments)
                    if remaining > 0:
                        comments = comments[:remaining]
                        all_comments.extend(comments)
                    numbered = [f"[{i}] {c}" for i, c in enumerate(comments, start=1)]
                    page_strings[page_idx - 1] = "\n\n".join(numbered) + "\n\n(* over 3000 comments, truncated)"
                    break
                else:
                    all_comments.extend(comments)
                    numbered = [f"[{i}] {c}" for i, c in enumerate(comments, start=1)]
                    page_strings[page_idx - 1] = "\n\n".join(numbered)

            new_comment_count = str(len(all_comments))

            yahoo_ws.update(
                range_name=f"O{idx}:O{idx}",
                values=[[new_comment_count]],
                value_input_option="USER_ENTERED",
            )
            update_comments_count += 1

            base_vals = [url, title, post_date, source, new_comment_count]
            base_vals.extend(page_strings)

            if url in comments_url_to_row:
                c_row = comments_url_to_row[url]
                comments_ws.update(
                    range_name=f"A{c_row}:{gspread.utils.rowcol_to_a1(c_row, 5 + MAX_COMMENT_PAGES)}",
                    values=[base_vals],
                    value_input_option="USER_ENTERED",
                )
            else:
                comments_ws.append_row(base_vals, value_input_option="USER_ENTERED")
                new_row_index = len(comments_values) + 1 + len(comments_url_to_row)
                comments_url_to_row[url] = new_row_index

            time.sleep(1 + random.random() * 0.5)

    print(f" ✅ 本文取得を {update_body_count} 行に実行")
    print(f" ✅ コメント取得＆Commentsシート更新を {update_comments_count} 行に実行")

# ================== Gemini バッチ分析 ==================
def analyze_with_gemini_batch(texts: List[str]) -> List[Dict[str, str]]:
    """最大100件の本文をまとめて Gemini で分析し、JSON配列を返す。"""
    if not GEMINI_CLIENT or not texts:
        return []

    if len(texts) > GEMINI_MAX_BATCH_SIZE:
        texts = texts[:GEMINI_MAX_BATCH_SIZE]

    prompt_template = load_gemini_batch_prompt()
    if not prompt_template:
        print("Geminiプロンプトが空のため、バッチ分析をスキップします。")
        return []

    trimmed_texts = [t[:3000] for t in texts]
    blocks = [
        f"==== ARTICLE {i} START ====\n{txt}\n==== ARTICLE {i} END ===="
        for i, txt in enumerate(trimmed_texts)
    ]
    text_batch = "\n\n".join(blocks)
    prompt = prompt_template.replace("{TEXT_BATCH}", text_batch)

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            resp = GEMINI_CLIENT.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config={"response_mime_type": "application/json"},
            )
            raw = to_str_safe(resp.text)
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print("Gemini からの JSON パースに失敗しました。返却テキスト:")
                print(raw)
                return []

            if isinstance(data, dict) and "results" in data:
                results = data["results"]
            elif isinstance(data, list):
                results = data
            else:
                print("Gemini応答JSON形式が想定外: dict(results=...) または list[...] ではありません。")
                return []

            out: List[Dict[str, str]] = []
            for item in results:
                if not isinstance(item, dict):
                    continue
                out.append(
                    {
                        "index": item.get("index", 0),
                        "company_info": item.get("company_info", "N/A"),
                        "category": item.get("category", "N/A"),
                        "sentiment": item.get("sentiment", "N/A"),
                        "nissan_related": item.get("nissan_related", "N/A"),
                        "nissan_negative": item.get("nissan_negative", "N/A"),
                    }
                )
            return out

        except ResourceExhausted as e:
            print(f" 🚨 Gemini API クォータ制限エラー (429): {e}")
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt + random.random()
                print(f" ⚠️ Gemini API 一時エラー。{wait:.2f}秒後に再試行 ({attempt+1}/{MAX_RETRIES})\n {e}")
                time.sleep(wait)
            else:
                print(f"Geminiバッチ分析でエラーが発生しました: {e}")
                return []
    return []

def analyze_and_update_sheet(gc: gspread.Client) -> None:
    """本文の入っている行の P〜T 列を Gemini 分析で更新"""
    if not GEMINI_CLIENT:
        print("Geminiクライアントが初期化されていないため、分析をスキップします。")
        return

    ws = ensure_yahoo_sheet(gc)
    values = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if len(values) <= 1:
        print(" Yahooシートにデータがないため、Gemini分析をスキップします。")
        return

    data_rows = values[1:]
    print("\n===== 🧠 ステップ3: Geminiバッチ分析 (100件/1API) =====")

    targets: List[Dict[str, Any]] = []
    for idx, row in enumerate(data_rows, start=2):
        if len(row) < len(YAHOO_SHEET_HEADERS):
            row.extend([""] * (len(YAHOO_SHEET_HEADERS) - len(row)))

        url = to_str_safe(row[0])
        if not url.startswith("https://news.yahoo.co.jp/articles/"):
            continue

        pages = row[4:4 + MAX_BODY_PAGES]
        pages = [p for p in pages if to_str_safe(p) and p != "本文取得不可"]
        if not pages:
            continue

        body_text = "\n\n".join(
            f"【Page{i+1}】\n{to_str_safe(p)}" for i, p in enumerate(pages)
        )

        company_info = to_str_safe(row[15]) if len(row) > 15 else ""
        category     = to_str_safe(row[16]) if len(row) > 16 else ""
        sentiment    = to_str_safe(row[17]) if len(row) > 17 else ""
        nissan_rel   = to_str_safe(row[18]) if len(row) > 18 else ""
        nissan_neg   = to_str_safe(row[19]) if len(row) > 19 else ""
        if company_info and category and sentiment and nissan_rel and nissan_neg:
            continue

        targets.append(
            {
                "row_index": idx,
                "url": url,
                "title": to_str_safe(row[1]),
                "body": body_text,
            }
        )

    if not targets:
        print(" Gemini分析が必要な行はありません。")
        return

    print(f" Gemini分析対象: {len(targets)} 行")
    updated_count = 0

    for i in range(0, len(targets), GEMINI_MAX_BATCH_SIZE):
        batch = targets[i : i + GEMINI_MAX_BATCH_SIZE]
        texts = [t["body"] for t in batch]
        print(f" - {i+1}〜{i+len(batch)}件目をバッチ分析中...")

        try:
            results = analyze_with_gemini_batch(texts)
        except ResourceExhausted:
            print(" 🚨 Geminiクォータ制限に到達したため、残りの分析は次回へ持ち越します。")
            break

        result_by_index: Dict[int, Dict[str, str]] = {}
        for r in results:
            try:
                idx_int = int(r.get("index", 0))
            except Exception:
                idx_int = 0
            result_by_index[idx_int] = r

        for local_idx, item in enumerate(batch):
            row_idx = item["row_index"]
            r = result_by_index.get(local_idx)
            if not r:
                continue

            company_info = r.get("company_info", "N/A")
            category     = r.get("category", "N/A")
            sentiment    = r.get("sentiment", "N/A")
            nissan_rel   = r.get("nissan_related", "N/A")
            nissan_neg   = r.get("nissan_negative", "N/A")

            ws.update(
                range_name=f"P{row_idx}:T{row_idx}",
                values=[[company_info, category, sentiment, nissan_rel, nissan_neg]],
                value_input_option="USER_ENTERED",
            )
            updated_count += 1
            time.sleep(0.8 + random.random() * 0.4)

    print(f" ✅ Geminiバッチ分析結果を {updated_count} 行に反映しました。")

# ================== メイン処理 ==================
def main():
    print("--- Yahooニュース統合スクレイパー開始 ---")
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords:
        print("キーワードがないため終了します。")
        sys.exit(0)

    try:
        gc = build_gspread_client()
    except RuntimeError as e:
        print(f"致命的エラー: {e}")
        sys.exit(1)

    # ステップ1: キーワードごとにニュース検索して Yahoo シートに追記
    for kw in keywords:
        print(f"\n===== 🔑 ステップ1: ニュースリスト取得: {kw} =====")
        articles = get_yahoo_news_with_selenium(kw)
        write_news_list_to_source(gc, articles)
        time.sleep(2)

    # ステップ2: 本文＆コメント取得 + Commentsシート更新
    fetch_details_and_update(gc)

    # ステップ3: Geminiバッチ分析 (100件/1API)
    analyze_and_update_sheet(gc)

    print("\n--- Yahooニュース統合スクレイパー完了 ---")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.append(script_dir)
    main()
