# 05-1_【Python】Search the list using Google API —— CLEAN FULL VERSION
# 仕様:
# - 事前スキャンで 各シートの「未処理 / 総行数」を表示（合計も）
# - 処理対象は "searched_URL" が空の行のみ（未処理判定）
# - 処理件数は 'all' または 数値で指定
# - 未処理からランダム抽出で処理（重複なし）。抽出例を表示
# - 検索結果がゼロでも必ず "--- row_start ---" を書き込んで「処理済み」痕跡を残す
# - Aへ書き戻し: Excelは該当シートを置換保存 / CSVは上書き保存
# - Bは「今回処理した分のみ」のデルタログを CWD/log_Searched/ に出力（timestamp & processed_at列付与）
# - ドメイン重複は“今回処理バッチ内”で重複しないように制御（シート単位）

import os
import time
import random
import glob
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

# ==== 環境変数からAPIキーとCSE IDを取得 ====
API_KEY = os.environ.get("google_search_api_key")
CSE_ID = os.environ.get("google_search_engine_id")
if not API_KEY or not CSE_ID:
    raise ValueError("APIキーまたはCSE IDが環境変数から取得できません")

# ==== レート制御（429対策） ====
QPM_TARGET = 20
BASE_DELAY = 60.0 / QPM_TARGET
MAX_RETRIES = 6
BACKOFF_FACTOR = 2.0
JITTER_RANGE = (0.05, 0.25)

# 1プロセスで使い回す
_GOOGLE_SERVICE = None

def throttle_wait(delay=BASE_DELAY):
    time.sleep(delay + random.uniform(*JITTER_RANGE))

# ==== 重複回避のための保存パス生成（接頭辞で連番） ====

def get_unique_path_prefix(path_str: str) -> str:
    path = os.path.abspath(path_str)
    if not os.path.exists(path):
        return path
    d = os.path.dirname(path)
    base = os.path.basename(path)
    i = 1
    while True:
        candidate = os.path.join(d, f"{i:03d}_{base}")
        if not os.path.exists(candidate):
            return candidate
        i += 1

# ==== Google検索 ====

def google_search(query, api_key, cse_id, num=10):
    global _GOOGLE_SERVICE
    if _GOOGLE_SERVICE is None:
        _GOOGLE_SERVICE = build("customsearch", "v1", developerKey=api_key, cache_discovery=False)
    delay = BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = _GOOGLE_SERVICE.cse().list(q=query, cx=cse_id, num=num).execute()
            throttle_wait(BASE_DELAY)
            return [item["link"] for item in res.get("items", [])]
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 429 or (status and 500 <= status < 600):
                sleep_s = delay + random.uniform(*JITTER_RANGE)
                print(f"[{status}] retry {attempt}/{MAX_RETRIES} after {sleep_s:.2f}s")
                time.sleep(sleep_s)
                delay *= BACKOFF_FACTOR
                continue
            raise
        except Exception:
            throttle_wait(BASE_DELAY)
            if attempt == MAX_RETRIES:
                raise
    return []

# ==== 独自ドメイン抽出 ====

def get_domain(url):
    try:
        netloc = urlparse(url).netloc
        return netloc.lower().lstrip("www.")
    except Exception:
        return url

# =========================
# ① 同階層の「フォルダ」を列挙して選択（allなし・カンマ区切り可）
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
dirs_1depth = sorted([p for p in SCRIPT_DIR.iterdir() if p.is_dir()])

if not dirs_1depth:
    raise FileNotFoundError("同じフォルダ直下にサブフォルダが見つかりません。")

print("処理対象のフォルダを選択してください:")
for i, d in enumerate(dirs_1depth, start=1):
    print(f"{i}: {d.name}")
n_dirs = len(dirs_1depth)
raw_dir_pick = input(f"番号（1〜{n_dirs}。カンマ区切りで複数可）: ").strip()
idxs = sorted({int(x.strip()) for x in raw_dir_pick.split(",") if x.strip().isdigit()})
if not idxs:
    raise ValueError(f"フォルダ番号の入力が不正です。1〜{n_dirs} の範囲で指定してください。")
target_dirs = [dirs_1depth[i-1] for i in idxs if 1 <= i <= n_dirs]

# =========================
# ② 選んだフォルダ内で Keyword-list_* を探す（1階層下のみ）
#    見つからなければ、そのフォルダ内の .xlsx / .csv を候補に
# =========================

def collect_candidate_files(base_dir: Path):
    cands = []
    cands += [Path(p) for p in glob.glob(str(base_dir / "Keyword-list_*.xlsx"))]
    cands += [Path(p) for p in glob.glob(str(base_dir / "Keyword-list_*.csv"))]
    if not cands:
        cands += [Path(p) for p in glob.glob(str(base_dir / "*.xlsx"))]
        cands += [Path(p) for p in glob.glob(str(base_dir / "*.csv"))]
    return sorted(cands)

# =========================
# ③ 各フォルダごとにファイル→シート/CSVを処理（事前スキャン→未処理だけ処理）
# =========================
for selected_dir in target_dirs:
    files = collect_candidate_files(selected_dir)
    if not files:
        print(f"[WARN] フォルダ '{selected_dir.name}' に対象ファイル(.xlsx/.csv)が見つかりません。スキップします。")
        continue

    print("\n" + "="*72)
    print(f"▶ フォルダ: {selected_dir.name}")
    print("処理するファイルを選択してください:")
    for i, p in enumerate(files, start=1):
        print(f"{i}: {p.name}")
    n_files = len(files)
    raw_file_pick = input(f"番号（1〜{n_files}。カンマ区切りで複数可）: ").strip()
    idxs = sorted({int(x.strip()) for x in raw_file_pick.split(",") if x.strip().isdigit()})
    if not idxs:
        raise ValueError(f"ファイル番号の入力が不正です。1〜{n_files} の範囲で指定してください。")
    target_files = [files[i-1] for i in idxs if 1 <= i <= n_files]

    for input_path in target_files:
        print("\n" + "-"*72)
        print(f"▶ ファイル処理開始: {input_path.name}")
        is_excel = input_path.suffix.lower() == ".xlsx"

        # ---- シート選択（Excelのみは all 可）----
        if is_excel:
            excel = pd.ExcelFile(input_path)
            print("処理するシートを選択してください:")
            for idx, name in enumerate(excel.sheet_names, start=1):
                print(f"{idx}: {name}")
            n_sheets = len(excel.sheet_names)
            raw = input(f"番号（1〜{n_sheets}。カンマ区切り または all）: ").strip().lower()
            if raw == "all":
                target_sheets = excel.sheet_names
            else:
                indices = []
                for token in raw.split(","):
                    token = token.strip()
                    if not token.isdigit():
                        raise ValueError(f"不正な番号入力です: {token}")
                    n = int(token)
                    if not (1 <= n <= n_sheets):
                        raise ValueError(f"番号が範囲外です: {n}（1〜{n_sheets}）")
                    indices.append(n - 1)
                indices = sorted(set(indices))
                target_sheets = [excel.sheet_names[i] for i in indices]
        else:
            target_sheets = [None]  # CSV

        # ---- 事前スキャン：各シート/CSVの行数と未処理数を先に読み込んで表示 ----
        sheet_row_counts = {}
        sheet_remaining_counts = {}
        dfs_cache = {}
        total_selected_rows = 0
        total_remaining_rows = 0

        for sheet_name in target_sheets:
            if is_excel:
                df = pd.read_excel(input_path, sheet_name=sheet_name)
                label = sheet_name
            else:
                df = pd.read_csv(input_path)
                label = "CSV"

            if "searched_URL" not in df.columns:
                df["searched_URL"] = ""

            dfs_cache[label] = df
            total_rows = len(df)
            remaining_mask = df["searched_URL"].fillna("") == ""
            remaining = int(remaining_mask.sum())

            sheet_row_counts[label] = total_rows
            sheet_remaining_counts[label] = remaining
            total_selected_rows += total_rows
            total_remaining_rows += remaining

        print("------")
        print("選択したシートごとの 未処理 / 総行数:")
        for sheet in sheet_row_counts:
            print(f" - {sheet}: 未処理={sheet_remaining_counts[sheet]} / 総行数={sheet_row_counts[sheet]}")
        print(f"▶ 合計: 未処理={total_remaining_rows} / 総行数={total_selected_rows}")

        # ---- 各シート/CSVの処理本体（未処理のみ、ランダム抽出、件数指定可） ----
        for sheet_name in target_sheets:
            if is_excel:
                label = sheet_name
                df = dfs_cache[label]
            else:
                label = "CSV"
                df = dfs_cache[label]

            total_rows = len(df)
            remaining_mask = df["searched_URL"].fillna("") == ""
            remaining_indices = list(df.index[remaining_mask])
            remaining = len(remaining_indices)

            print(f"\n[ {label} ] 未処理: {remaining} / 総行数: {total_rows}")
            if remaining == 0:
                print("→ 未処理行はありません。スキップします。")
                continue

            # 処理件数の指定
            ask = input(f"処理する行数を入力（'all' または 数値、最大 {remaining}）: ").strip().lower()
            if ask in ("", "all"):
                n_proc = remaining
            else:
                if not ask.isdigit():
                    raise ValueError(f"不正な入力です（all または 数値）: {ask}")
                n_proc = max(0, min(int(ask), remaining))

            # ランダム抽出（未処理から重複なしで n_proc 件）
            if n_proc > 0:
                target_indices = random.sample(remaining_indices, k=n_proc)
                target_indices.sort()  # 書き戻し時の視認性のため昇順
            else:
                target_indices = []
            example_rows = [(idx + 1) for idx in target_indices[:min(5, len(target_indices))]]
            print(f"→ 今回は ランダムに {n_proc} 行を処理します。例: {example_rows}")

            # 検索実行（今回処理分）
            all_domains = set()  # 同一シート内の今回の処理でドメイン重複を避ける
            it = tqdm(range(n_proc), total=n_proc, desc=f"Google検索中 [{label}]")
            for k in it:
                i = target_indices[k]
                row = df.iloc[i]
                # 先頭3列をクエリに使う（存在しない列は無視）
                cols = [row.iloc[j] if j < len(row) else None for j in range(3)]

                # クエリが空なら処理済みマークのみ
                if all(pd.isna(x) or str(x).strip() == "" for x in cols):
                    df.at[i, "searched_URL"] = "--- row_start ---"
                    continue

                query = " ".join([str(x) for x in cols if pd.notna(x) and str(x).strip()])
                try:
                    urls = google_search(query, API_KEY, CSE_ID, num=10)
                except Exception as e:
                    print(f"[WARN] 検索失敗: {query} :: {e}")
                    urls = []
                urls_cleaned = [u.strip() for u in urls if u.strip()]

                if urls_cleaned:
                    content = "--- row_start ---\n" + "\n".join(urls_cleaned)
                else:
                    content = "--- row_start ---"  # 結果ゼロでも処理済み痕跡
                # ドメイン重複除去（今回処理分に対して）
                if content.strip() != "--- row_start ---":
                    lines = content.split("\n")
                    header = lines[0]
                    uniq_urls = []
                    for url in lines[1:]:
                        domain = get_domain(url)
                        if domain not in all_domains:
                            uniq_urls.append(url)
                            all_domains.add(domain)
                    content = "\n".join([header] + uniq_urls) if uniq_urls else header

                df.at[i, "searched_URL"] = content

            # ==== (1) Aへ書き戻し（上書き）====
            if is_excel:
                from openpyxl import load_workbook  # 確実にopenpyxlを使う
                # 既存ブックの当該シートを置換保存（他シートは保持）
                with pd.ExcelWriter(input_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df.to_excel(writer, sheet_name=label, index=False)
                print(f"💾 Aへ書き戻し完了 → {input_path.name} / {label}")
            else:
                # CSV の場合は A=CSV をそのまま上書き
                df.to_csv(input_path, index=False, encoding="utf-8-sig")
                print(f"💾 A(CSV) を上書き保存 → {input_path.name}")

            # ==== (2) B: ログを CWD/log_Searched/ に保存 ====
# 仕様: ファイルA（対象シート）と同じ行・列構造を“空欄で”踏襲し、
#       今回処理した行だけオリジナル内容を転記する（＝位置が分かるスパースログ）
run_ts = datetime.now().strftime("%Y%m%d-%H%M%S")

# Aと同じ形（全セル空文字）のフレームを用意
# ※元のdfは今回時点の最新（書き戻し反映済み）
df_log = pd.DataFrame("", index=df.index, columns=df.columns)

# 今回処理した行だけ、元dfの全列をそのまま転記
if target_indices:
    df_log.loc[target_indices, :] = df.loc[target_indices, :]

# メタ情報列（processed_at）を付与（未処理行は空）
if "processed_at" not in df_log.columns:
    df_log["processed_at"] = ""
if target_indices:
    df_log.loc[target_indices, "processed_at"] = run_ts

# 出力先: カレント直下 log_Searched/
log_dir = input_path.parent / "log_Searched"
log_dir.mkdir(parents=True, exist_ok=True)

if is_excel:
    out_name = f"searched({label})_{input_path.stem}__log_{run_ts}.xlsx"
    output_path = log_dir / out_name
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_log.to_excel(writer, sheet_name=label, index=False)
    print(f"📝 ログ出力（B: スパースログ）: {output_path}")
else:
    out_name = f"searched({label})_{input_path.stem}__log_{run_ts}.csv"
    output_path = log_dir / out_name
    df_log.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"📝 ログ出力（B/CSV: スパースログ）: {output_path}")

print("\nすべての処理が完了しました。")
