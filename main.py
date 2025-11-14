# -*- coding: utf-8 -*-
"""
Yahooニュース統合スクレイパー（国内8社対応版・GitHub Actions用）
 - Yahooシートにニュース一覧＋本文（最大10ページ）を書き込み
 - Commentsシートにコメント（最大10ページ）を書き込み
 - Gemini を使って 10件まとめてバッチ分析し、以下を判定
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
from typing import List, Tuple, Optional, Dict, Any
import sys

from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

# コメント総数の上限（それ以上あれば「…(over 3000)」などと追記）
MAX_COMMENTS_TOTAL = 3000

# Yahoo シートのヘッダ定義
# A:URL, B:タイトル, C:投稿日時, D:ソース,
# E〜N: 本文(1〜10ページ), O:コメント数,
# P:主題企業, Q:カテゴリ, R:ポジネガ, S:日産関連文, T:日産ネガ文
YAHOO_SHEET_HEADERS = [
    "URL",          # A
    "タイトル",      # B
    "投稿日時",      # C
    "ソース",        # D
    "本文_P1",      # E
    "本文_P2",      # F
    "本文_P3",      # G
    "本文_P4",      # H
    "本文_P5",      # I
    "本文_P6",      # J
    "本文_P7",      # K
    "本文_P8",      # L
    "本文_P9",      # M
    "本文_P10",     # N
    "コメント数",     # O
    "主題企業",       # P
    "カテゴリ",       # Q
    "ポジネガ",       # R
    "日産関連文",     # S
    "日産ネガ文"      # T
]

# Comments シートのヘッダ定義
# A:URL, B:タイトル, C:投稿日時, D:ソース, E:コメント数, F〜:コメントページ
COMMENTS_SHEET_HEADERS = [
    "URL",          # A
    "タイトル",      # B
    "投稿日時",      # C
    "ソース",        # D
    "コメント数"      # E
    # F〜: コメントページ1〜10
]

REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_posinega.txt",
    "prompt_category.txt",
    "prompt_target_company.txt"
]

# ===== Gemini Client 初期化 =====
try:
    GEMINI_CLIENT = genai.Client()
except Exception as e:
    print(f"警告: Geminiクライアントの初期化に失敗しました。Gemini分析はスキップされます。エラー: {e}")
    GEMINI_CLIENT = None

GEMINI_BATCH_PROMPT_BASE = None  # バッチ用プロンプトテンプレート

# ================== ヘルパー関数 ==================

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
        # シート内で既に日付型になっている場合もあり得る
        try:
            # gspreadはシリアル値で返さないので基本通らない想定
            return None
        except Exception:
            return None

    if isinstance(raw, str):
        s = raw.strip()

        # (月) などの曜日を削除
        s = re.sub(r"\([月火水木金土日]\)", "", s).strip()
        # 「配信」を削除
        s = s.replace("配信", "").strip()

        # よくあるパターンを順に試す
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%y/%m/%d %H:%M", "%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                # 未来すぎる場合は前年に補正
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
            # ローカル fallback
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
    10件まとめて JSON 配列で返すように指示する。
    """
    global GEMINI_BATCH_PROMPT_BASE
    if GEMINI_BATCH_PROMPT_BASE is not None:
        return GEMINI_BATCH_PROMPT_BASE

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # 役割プロンプト
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

        # 追加の明示的な指示（バッチ & 日産抽出）
        extra = """
追加要件:
- 与えられる記事本文は最大で10件です。
- それぞれの記事について、以下の情報を判定してください。
  * company_info: 記事の主題企業名。共同開発などがあれば () 内に別企業も書いてください。
  * category: 企業、モデル、技術、社会、投資など、PROMPTで指定のカテゴリ分類に従ってください。
  * sentiment: 記事全体のトーンを「ポジティブ」「ネガティブ」「ニュートラル」のいずれかで判定してください。
  * nissan_related: 記事本文中で「日産」や「NISSAN」「ニッサン」など、日産自動車やその商品・サービスに言及している文を、日本語の文章として可能な限り抽出してまとめてください。なければ "N/A" としてください。
  * nissan_negative: 上記 nissan_related の文のうち、日産や日産の商品・サービスに対してネガティブな印象を与える内容（批判・不満・懸念など）だけを抽出してまとめてください。なければ "N/A" としてください。

出力フォーマット:
- 必ず JSON 配列形式で出力してください。
- 配列の各要素は、次のキーを持つオブジェクトとします:
  {
    "index": 0,  // 入力順に 0,1,2,... としたインデックス
    "company_info": "string",
    "category": "string",
    "sentiment": "string",
    "nissan_related": "string",
    "nissan_negative": "string"
  }

入力フォーマット:
- 以下のように、記事本文が複数與えられます。
- "==== ARTICLE i START ====" と "==== ARTICLE i END ====" に挟まれた部分が、index=i の記事本文です。

実行タスク:
- 各記事ごとに、上記の JSON オブジェクトを生成し、インデックス順に並べた JSON 配列を1つだけ出力してください。
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
                print(f"  ❌ 404 Not Found: {url}")
                return None
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.random()
                print(f"  ⚠️ 接続エラー, リトライ {attempt+1}/{max_retries} ({wait_time:.2f}秒): {e}")
                time.sleep(wait_time)
            else:
                print(f"  ❌ 最終リトライ失敗: {e}")
                return None
    return None

# ================== Yahooニュース検索 (Selenium) ==================

def get_yahoo_news_with_selenium(keyword: str) -> List[Dict[str, str]]:
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
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

    # GitHub Actions では PATH 上の chromedriver を使う想定
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

    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']"))
        )
        time.sleep(3)
    except Exception as e:
        print(f"  ⚠️ ページロード/要素待ちタイムアウト: {e}")
        time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    articles_data: List[Dict[str, str]] = []
    today_jst = jst_now()

    for article in articles:
        try:
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            link_tag = article.find("a", href=True)
            url = (
                link_tag["href"]
                if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/")
                else ""
            )

            time_tag = article.find("time")
            date_str = time_tag.text.strip() if time_tag else ""

            # ソース抽出（構造は頻繁に変わるため、長めテキストを採用）
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            if source_container:
                time_and_comments = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                if time_and_comments:
                    spans = [
                        s.text.strip()
                        for s in time_and_comments.find_all("span")
                        if not s.find("svg")
                    ]
                    # 日付らしきものは除去
                    spans = [
                        s
                        for s in spans
                        if not re.match(r"\d{1,2}/\d{1,2}.*\d{1,2}:\d{2}", s)
                    ]
                    if spans:
                        source_text = max(spans, key=len)

            formatted_date = "取得不可"
            if date_str:
                dt_obj = parse_post_date(date_str, today_jst)
                if dt_obj:
                    formatted_date = format_datetime(dt_obj)
                else:
                    formatted_date = re.sub(r"\([月火水木金土日]\)", "", date_str).strip()

            if title and url:
                articles_data.append(
                    {
                        "URL": url,
                        "タイトル": title,
                        "投稿日時": formatted_date,
                        "ソース": source_text or "取得不可",
                    }
                )
        except Exception:
            continue

    print(f"  Yahoo!ニュース件数: {len(articles_data)} 件取得")
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
        str(row[0])
        for row in existing[1:]
        if len(row) > 0 and str(row[0]).startswith("http")
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
        # 残りの列は空で埋める
        row.extend([""] * (len(YAHOO_SHEET_HEADERS) - len(row)))
        new_rows.append(row)

    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f"  Yahooシートに {len(new_rows)} 件追記しました。")
    else:
        print("  追記すべき新規記事はありません。")

# ================== 本文・コメント取得 ==================

def fetch_article_body_page(url: str) -> str:
    """
    記事の単一ページから本文テキストを抽出
    """
    res = request_with_retry(url)
    if not res:
        return ""

    soup = BeautifulSoup(res.text, "html.parser")

    # articleタグや article_body相当を探す
    article_content = (
        soup.find("article")
        or soup.find("div", class_="article_body")
        or soup.find("div", class_=re.compile(r"article_detail|article_body"))
    )

    texts: List[str] = []
    if article_content:
        # ハイライト対象の p から優先的に取得
        paragraphs = article_content.find_all(
            "p", class_=re.compile(r"highLightSearchTarget")
        )
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
    （YahooのHTML構造は頻繁に変わるため、汎用的なセレクタで取得）
    """
    res = request_with_retry(url)
    if not res:
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    comments: List[str] = []

    # 代表的なクラス名をいくつか試しながらコメント本文っぽい要素を拾う
    candidate_selectors = [
        "div[class*='CommentItem__body']",
        "p[class*='CommentItem__body']",
        "span[class*='CommentItem__body']",
        "p[class*='sc-']",
    ]
    for sel in candidate_selectors:
        for node in soup.select(sel):
            text = node.get_text(strip=True)
            # ある程度の長さがあるものだけ
            if text and len(text) > 5 and text not in comments:
                comments.append(text)

    # 候補が少なすぎる場合は、より緩い抽出は行わず、そのまま返す
    return comments

def fetch_details_and_update(gc: gspread.Client) -> None:
    """
    Yahooシートの各行について:
      - 本文10ページ分 (E〜N) を取得・更新
      - コメント数(O) を更新
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

    # Commentsシート側のURL→row番号マップを先に作っておく
    comments_values = comments_ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    comments_url_to_row: Dict[str, int] = {}
    if comments_values:
        for idx, row in enumerate(comments_values[1:], start=2):
            if len(row) > 0 and row[0].startswith("http"):
                comments_url_to_row[row[0]] = idx

    update_body_count = 0
    update_comments_count = 0

    for idx, row in enumerate(data_rows, start=2):
        # 行長をヘッダ長に合わせる
        if len(row) < len(YAHOO_SHEET_HEADERS):
            row.extend([""] * (len(YAHOO_SHEET_HEADERS) - len(row)))

        url = row[0].strip()
        if not url.startswith("https://news.yahoo.co.jp/articles/"):
            continue

        title = row[1]
        post_date = row[2]
        source = row[3]

        # 本文列 (E〜N)
        body_pages = row[4:4 + MAX_BODY_PAGES]
        # コメント数 (O)
        comment_count_str = row[14].strip() if len(row) > 14 else ""

        # 本文P1 が空の場合のみ本文取得を実施（無限再取得防止）
        need_body = not body_pages[0].strip()

        # コメント関連は都度更新（古いコメント数を残したくないため）
        need_comments = True

        new_body_pages = list(body_pages)
        new_comment_count = comment_count_str

        if need_body or need_comments:
            print(f"  - 行 {idx} (記事: {title[:20]}...): 詳細取得中...")

        # ===== 本文取得 (最大10ページ) =====
        if need_body:
            body_changed = False
            for page_idx in range(1, MAX_BODY_PAGES + 1):
                if page_idx == 1:
                    page_url = url
                else:
                    page_url = f"{url}?page={page_idx}"

                text = fetch_article_body_page(page_url)
                if not text:
                    # 2ページ目以降で本文が空 => 以降のページは存在しないとみなして break
                    if page_idx == 1:
                        # 1ページ目すら取れない場合
                        new_body_pages[0] = "本文取得不可"
                        body_changed = True
                    break

                col_idx = page_idx - 1  # 0~9
                if new_body_pages[col_idx] != text:
                    new_body_pages[col_idx] = text
                    body_changed = True

            # 更新反映
            if body_changed:
                yahoo_ws.update(
                    range_name=f"E{idx}:N{idx}",
                    values=[new_body_pages],
                    value_input_option="USER_ENTERED",
                )
                update_body_count += 1
                # シート API 負荷対策
                time.sleep(1 + random.random() * 0.5)

        # ===== コメント取得 (最大10ページ) =====
        if need_comments:
            all_comments: List[str] = []
            page_strings: List[str] = [""] * MAX_COMMENT_PAGES

            for page_idx in range(1, MAX_COMMENT_PAGES + 1):
                if page_idx == 1:
                    c_url = url + "/comments"
                else:
                    c_url = url + f"/comments?page={page_idx}"

                comments = fetch_comments_page(c_url)
                if not comments:
                    # コメントがまったく取れないページが来たら以降は終了
                    break

                # 総数制限
                if len(all_comments) + len(comments) > MAX_COMMENTS_TOTAL:
                    remaining = MAX_COMMENTS_TOTAL - len(all_comments)
                    if remaining > 0:
                        comments = comments[:remaining]
                    all_comments.extend(comments)
                    # このページのセル用文字列
                    numbered = []
                    for i, c in enumerate(comments, start=1):
                        numbered.append(f"[{i}] {c}")
                    page_strings[page_idx - 1] = "\n\n".join(numbered) + "\n\n(※ over 3000 comments, truncated)"
                    # 3000を超えたので終了
                    break
                else:
                    all_comments.extend(comments)
                    numbered = []
                    for i, c in enumerate(comments, start=1):
                        numbered.append(f"[{i}] {c}")
                    page_strings[page_idx - 1] = "\n\n".join(numbered)

            # コメント数更新
            new_comment_count = str(len(all_comments))

            # Yahooシート側 O列
            yahoo_ws.update(
                range_name=f"O{idx}:O{idx}",
                values=[[new_comment_count]],
                value_input_option="USER_ENTERED",
            )
            update_comments_count += 1

            # Commentsシート側
            if url in comments_url_to_row:
                c_row = comments_url_to_row[url]
                # 既存行を上書き
                base_vals = [url, title, post_date, source, new_comment_count]
                base_vals.extend(page_strings)
                comments_ws.update(
                    range_name=f"A{c_row}:{gspread.utils.rowcol_to_a1(c_row, 5 + MAX_COMMENT_PAGES)}",
                    values=[base_vals],
                    value_input_option="USER_ENTERED",
                )
            else:
                # 新規行として末尾に追加
                base_vals = [url, title, post_date, source, new_comment_count]
                base_vals.extend(page_strings)
                comments_ws.append_row(base_vals, value_input_option="USER_ENTERED")
                new_row_index = len(comments_values) + 1 + len(comments_url_to_row)  # ざっくり
                comments_url_to_row[url] = new_row_index

            time.sleep(1 + random.random() * 0.5)

    print(f" ✅ 本文取得を {update_body_count} 行に実行")
    print(f" ✅ コメント取得＆Commentsシート更新を {update_comments_count} 行に実行")

# ================== Gemini バッチ分析 ==================

def analyze_with_gemini_batch(texts: List[str]) -> List[Dict[str, str]]:
    """
    最大10件の本文をまとめて Gemini で分析し、JSON配列を返す。
    texts[i] が index=i に対応。
    """
    if not GEMINI_CLIENT:
        return []

    if not texts:
        return []

    prompt_template = load_gemini_batch_prompt()
    if not prompt_template:
        print("Geminiプロンプトが空のため、バッチ分析をスキップします。")
        return []

    # 長さを制限（1件あたり15,000文字まで）
    trimmed_texts = [t[:15000] for t in texts]

    # {TEXT_BATCH} を生成
    blocks = []
    for i, txt in enumerate(trimmed_texts):
        blocks.append(
            f"==== ARTICLE {i} START ====\n{txt}\n==== ARTICLE {i} END ===="
        )
    text_batch = "\n\n".join(blocks)

    prompt = prompt_template.replace("{TEXT_BATCH}", text_batch)

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            resp = GEMINI_CLIENT.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                },
            )
            raw = resp.text.strip()
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
            # クォータ制限 -> ここで全体処理を終了させる（呼び出し側で検知）
            print(f"  🚨 Gemini API クォータ制限エラー (429): {e}")
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt + random.random()
                print(f"  ⚠️ Gemini API 一時エラー。{wait:.2f}秒後に再試行 ({attempt+1}/{MAX_RETRIES}) | {e}")
                time.sleep(wait)
            else:
                print(f"Geminiバッチ分析でエラーが発生しました: {e}")
                return []

    return []

def analyze_and_update_sheet(gc: gspread.Client) -> None:
    """
    Yahooシートの本文 (E〜N) が入っている行で、
      - P:主題企業
      - Q:カテゴリ
      - R:ポジネガ
      - S:日産関連文
      - T:日産ネガ文
    が空のものを対象に、最大10件まとめて Gemini で分析＆更新する。
    """
    if not GEMINI_CLIENT:
        print("Geminiクライアントが初期化されていないため、分析をスキップします。")
        return

    ws = ensure_yahoo_sheet(gc)
    values = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if len(values) <= 1:
        print(" Yahooシートにデータがないため、Gemini分析をスキップします。")
        return

    data_rows = values[1:]

    print("\n===== 🧠 ステップ3: Geminiバッチ分析 (10件/1API) =====")

    # 分析対象行を収集
    targets: List[Dict[str, Any]] = []
    for idx, row in enumerate(data_rows, start=2):
        if len(row) < len(YAHOO_SHEET_HEADERS):
            row.extend([""] * (len(YAHOO_SHEET_HEADERS) - len(row)))

        url = row[0].strip()
        if not url.startswith("https://news.yahoo.co.jp/articles/"):
            continue

        # 本文ページを結合
        pages = row[4:4 + MAX_BODY_PAGES]
        pages = [p for p in pages if p and p != "本文取得不可"]
        if not pages:
            continue
        body_text = "\n\n".join(
            f"【Page{i+1}】\n{p}" for i, p in enumerate(pages)
        )

        # すでに P〜T がすべて埋まっている行はスキップ
        company_info = row[15] if len(row) > 15 else ""
        category = row[16] if len(row) > 16 else ""
        sentiment = row[17] if len(row) > 17 else ""
        nissan_rel = row[18] if len(row) > 18 else ""
        nissan_neg = row[19] if len(row) > 19 else ""
        if company_info and category and sentiment and nissan_rel and nissan_neg:
            continue

        targets.append(
            {
                "row_index": idx,
                "url": url,
                "title": row[1],
                "body": body_text,
            }
        )

    if not targets:
        print("  Gemini分析が必要な行はありません。")
        return

    print(f"  Gemini分析対象: {len(targets)} 行")

    updated_count = 0
    # 10件ずつバッチ処理
    for i in range(0, len(targets), 10):
        batch = targets[i : i + 10]
        texts = [t["body"] for t in batch]
        print(f"  - {i+1}〜{i+len(batch)}件目をバッチ分析中...")

        try:
            results = analyze_with_gemini_batch(texts)
        except ResourceExhausted:
            # クォータ制限が出たらこれ以上の分析はあきらめる
            print("  🚨 Geminiクォータ制限に到達したため、残りの分析は次回へ持ち越します。")
            break

        # index で紐づけ
        result_by_index: Dict[int, Dict[str, str]] = {}
        for r in results:
            try:
                idx_int = int(r.get("index", 0))
            except Exception:
                idx_int = 0
            result_by_index[idx_int] = r

        # バッチ内各行を更新
        for local_idx, item in enumerate(batch):
            row_idx = item["row_index"]
            r = result_by_index.get(local_idx)
            if not r:
                # 対応する結果がない場合はスキップ
                continue

            company_info = r.get("company_info", "N/A")
            category = r.get("category", "N/A")
            sentiment = r.get("sentiment", "N/A")
            nissan_rel = r.get("nissan_related", "N/A")
            nissan_neg = r.get("nissan_negative", "N/A")

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

    # ステップ3: Geminiバッチ分析
    analyze_and_update_sheet(gc)

    print("\n--- Yahooニュース統合スクレイパー完了 ---")


if __name__ == "__main__":
    # スクリプトディレクトリを sys.path に追加（PROMPT_FILES 読み込み用）
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.append(script_dir)

    main()
