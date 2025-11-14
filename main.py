############################################
#  Yahooニュース本文（最大10ページ）取得関数
############################################
def fetch_yahoo_article_pages(driver, url, max_pages=10):
    """
    Yahooニュースの記事本文を最大10ページまで取得。
    Returns:
        pages_text: list[str]   # 各ページ本文
        combined_text: str      # 全ページ連結（Gemini判定用）
    """
    pages_text = []

    for page_num in range(1, max_pages + 1):
        # 1ページ目だけ?page=が存在しないことがあるため例外処理
        if page_num == 1:
            page_url = url
        else:
            page_url = f"{url}?page={page_num}"

        try:
            driver.get(page_url)
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # 本文ノード（Yahooの通常記事構造に準拠）
            article_nodes = soup.select("article p")

            # ページが存在しない場合はノードがほぼ無い → break
            if not article_nodes or len(article_nodes) < 2:
                break

            # 改行付きでテキスト抽出
            page_text = "\n".join([p.get_text(strip=True) for p in article_nodes])
            pages_text.append(page_text)

        except Exception as e:
            print(f"[WARN] Page {page_num} fetch error: {e}")
            break

    combined_text = "\n".join(pages_text)
    return pages_text, combined_text



#####################################################
#  ここから main.py の本文取得部分（差し替え完成版）
#####################################################

# ==========================
#  本文・コメント数取得処理
# ==========================

# すでにValuesで A〜D列を取得済みとして、row_index などは既存ロジックを維持
for idx, row in enumerate(values, start=2):

    title = row[0]
    url = row[1]
    date_str = row[2]
    source = row[3]

    print(f"▶ 記事処理中: {title}")

    # 本文未取得 または 日付の条件 OK の場合のみ実行
    if should_fetch_article(url, sheet, idx):

        try:
            # ★ Yahooニュース本文を最大10ページ取得
            pages_text, combined_text = fetch_yahoo_article_pages(driver, url)

            # --- 1ページ1セルで書き込み（E〜N列を利用） ---
            page_columns = ['E','F','G','H','I','J','K','L','M','N']

            for i, page_content in enumerate(pages_text):
                if i < len(page_columns):
                    sheet.update(f"{page_columns[i]}{idx}", [[page_content]])

            # ★ 全ページを結合した本文（combined_text）で判定
            full_text_for_ai = combined_text

        except Exception as e:
            print(f"[ERROR] 本文取得中にエラー: {e}")
            sheet.update(f"E{idx}", [[f"本文取得エラー: {e}"]])
            continue

    else:
        # 既に本文あり → 再取得しない
        existing_text = sheet.acell(f"E{idx}").value
        full_text_for_ai = existing_text if existing_text else ""

    # ============================
    #      コメント数の取得処理
    # ============================

    try:
        comment_count = fetch_comment_count(driver, url)
        sheet.update(f"O{idx}", [[comment_count]])
    except Exception as e:
        print(f"[WARN] コメント数取得失敗: {e}")
        sheet.update(f"O{idx}", [[f"Error: {e}"]])

    # ============================
    #      Gemini による解析
    # ============================

    try:
        ai_result = analyze_with_gemini(full_text_for_ai)

        # 結果の分解
        category = ai_result.get("category", "")
        sentiment = ai_result.get("sentiment", "")
        company_info = ai_result.get("company_info", "")

        # スプレッドシートに書き込み
        sheet.update(f"P{idx}", [[sentiment]])
        sheet.update(f"Q{idx}", [[category]])
        sheet.update(f"R{idx}", [[company_info]])

    except Exception as e:
        print(f"[ERROR] Gemini解析失敗: {e}")
        sheet.update(f"P{idx}", [[f"AIエラー: {e}"]])
