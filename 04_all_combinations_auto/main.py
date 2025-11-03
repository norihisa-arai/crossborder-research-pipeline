import pandas as pd
from itertools import product
from pathlib import Path
from math import prod
import time
import logging
import sys
import re
import glob

# =============================
# 設定
# =============================
ROOT_DIR = Path(r"C:\Users\tohjo\OneDrive\桌面\荒井\プロジェクト\Google検索による商品キーワード生成")
DEFAULT_SHEET_NAME = "Sheet1"
CSV_PART_ROWS = 1_000_000
WRITE_CHUNK_SIZE = 50_000

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable=None, total=None, unit=""):
        print("※ tqdm が未インストールのため簡易進捗を表示します（pip install tqdm 推奨）")
        count = 0
        step = max(1, (total or 1000) // 100)
        for item in iterable:
            count += 1
            if total and count % step == 0:
                pct = count / total * 100
                print(f"\r進捗: {pct:.1f}% ({count}/{total}) {unit}", end="")
            yield item
        if total:
            print(f"\r進捗: 100.0% ({count}/{total}) {unit}")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# =============================
# ユーティリティ
# =============================

def list_subfolders(base: Path):
    return [p for p in base.iterdir() if p.is_dir()]

def choose_folder(base: Path) -> Path:
    folders = list_subfolders(base)
    if not folders:
        raise FileNotFoundError(f"フォルダが見つかりません: {base}")
    print("\n▼ フォルダを番号で選択してください（Enterでキャンセル）")
    for i, f in enumerate(folders, 1):
        print(f"  {i}. {f.name}")
    while True:
        s = input("番号: ").strip()
        if s == "":
            raise SystemExit("キャンセルされました。")
        if s.isdigit() and 1 <= int(s) <= len(folders):
            return folders[int(s) - 1]
        print("無効な入力です。再入力してください。")

def find_keyword_files(folder: Path, prefix: str = "Keyword-list_", exts=(".xlsx", ".xls")):
    return [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in exts and p.name.startswith(prefix)]

def choose_file(files):
    """Keyword-list_* を選択。番号選択のみで決定（確認y/nは不要）。
    1件だけ見つかった場合も自動採用します。
    """
    if not files:
        raise FileNotFoundError("対象ファイル（接頭辞 'Keyword-list_'）が見つかりません。")
    print("▼ 以下のファイルが見つかりました：")
    for i, f in enumerate(files, 1):
        print(f"  {i}. {f.name}")
    if len(files) == 1:
        return files[0]
    else:
        while True:
            s = input("番号を選択してください（Enterで中止）: ").strip()
            if s == "":
                raise SystemExit("中止しました。")
            if s.isdigit() and 1 <= int(s) <= len(files):
                return files[int(s) - 1]
            else:
                print("無効な入力です。")


def read_excel_any(input_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(input_path, sheet_name=DEFAULT_SHEET_NAME, engine="openpyxl")
    except Exception:
        xls = pd.ExcelFile(input_path, engine="openpyxl")
        first = xls.sheet_names[0]
        logging.warning(f"'{DEFAULT_SHEET_NAME}' が見つからないため、'{first}' を読み込みます。")
        df = pd.read_excel(input_path, sheet_name=first, engine="openpyxl")
    return df


def clean_series(s: pd.Series) -> pd.Series:
    s2 = s.map(lambda x: str(x).strip() if pd.notna(x) else x)
    s2 = s2.dropna()
    s2 = s2[s2 != ""]
    return s2.drop_duplicates()

def confirm_or_pick_columns(df: pd.DataFrame) -> list:
    cols = list(df.columns)
    print("\n▼ 検出された列：")
    for i, c in enumerate(cols, 1):
        print(f"  {i}. {c}")
    ans = input("上記すべてを対象列として使用しますか？ (y=すべて / n=選択): ").strip().lower()
    if ans == "y":
        return cols
    while True:
        sel = input("使用する列番号をカンマ区切りで入力（例: 1,3,4）: ").strip()
        if not sel:
            print("少なくとも1つ選択してください。")
            continue
        try:
            idxs = [int(x) for x in re.split(r"\s*,\s*", sel) if x]
            picked = []
            for i in idxs:
                if 1 <= i <= len(cols):
                    picked.append(cols[i - 1])
            if picked:
                print("選択列:", picked)
                ans2 = input("この列でよろしいですか？ (y/n): ").strip().lower()
                if ans2 == "y":
                    return picked
        except ValueError:
            pass
        print("入力を確認してください。")

def iter_product(values_lists):
    return product(*values_lists) if values_lists else iter(())

# =============================
# 出力（CSVのみ・重複回避あり）
# =============================

def next_unique_csv_base(base_out: Path) -> Path:
    parent = base_out.parent
    stem = base_out.stem
    suffix = base_out.suffix

    def series_exists(stem_candidate: str) -> bool:
        base_candidate = parent / f"{stem_candidate}{suffix}"
        if base_candidate.exists():
            return True
        pattern = str(parent / f"{stem_candidate}_part*{suffix}")
        return any(Path(p).exists() for p in glob.glob(pattern))

    if not series_exists(stem):
        return base_out

    i = 1
    while True:
        stem_i = f"{stem}({i})"
        if not series_exists(stem_i):
            return parent / f"{stem_i}{suffix}"
        i += 1

def write_csv_in_parts_unique(base_out: Path, header_cols: list, rows_iter, total_rows: int):
    base_out = next_unique_csv_base(base_out)

    def part_path(n):
        return base_out.with_name(f"{base_out.stem}_part{n:03d}{base_out.suffix}")

    written_total = 0
    buffer = []
    start_time = time.time()
    pbar = tqdm(iterable=rows_iter, total=total_rows, unit="row")
    for row in pbar:
        buffer.append(row)
        if len(buffer) >= WRITE_CHUNK_SIZE:
            while buffer:
                remain_cap = CSV_PART_ROWS - (written_total % CSV_PART_ROWS)
                take = min(len(buffer), remain_cap)
                chunk = buffer[:take]
                buffer = buffer[take:]
                current_part = (written_total // CSV_PART_ROWS) + 1
                out_path = part_path(current_part)
                df_chunk = pd.DataFrame.from_records(chunk, columns=header_cols)
                header = not out_path.exists() or (written_total % CSV_PART_ROWS == 0)
                df_chunk.to_csv(out_path, mode="a", index=False, header=header, encoding="utf-8-sig")
                written_total += len(chunk)
                elapsed = time.time() - start_time
                speed = written_total / max(elapsed, 1)
                pbar.set_description(f"wrote: {written_total:,} rows @ {speed:,.0f} r/s")

    while buffer:
        remain_cap = CSV_PART_ROWS - (written_total % CSV_PART_ROWS)
        take = min(len(buffer), remain_cap)
        chunk = buffer[:take]
        buffer = buffer[take:]
        current_part = (written_total // CSV_PART_ROWS) + 1
        out_path = part_path(current_part)
        df_chunk = pd.DataFrame.from_records(chunk, columns=header_cols)
        header = not out_path.exists() or (written_total % CSV_PART_ROWS == 0)
        df_chunk.to_csv(out_path, mode="a", index=False, header=header, encoding="utf-8-sig")
        written_total += len(chunk)
        elapsed = time.time() - start_time
        speed = written_total / max(elapsed, 1)
        logging.info(f"書き出し {written_total:,} 行 / 経過 {elapsed:.1f}s / 速度 {speed:,.0f} r/s")

    return written_total, (written_total - 1) // CSV_PART_ROWS + (1 if written_total else 0), base_out

# =============================
# メインロジック
# =============================

def main():
    logging.info("処理を開始します。")
    folder = choose_folder(ROOT_DIR)
    logging.info(f"選択フォルダ: {folder}")
    files = find_keyword_files(folder)
    input_file = choose_file(files)
    logging.info(f"入力ファイル: {input_file}")

    df = read_excel_any(input_file)
    target_columns = confirm_or_pick_columns(df)
    value_lists = [clean_series(df[col]).tolist() for col in target_columns]
    counts = [len(v) for v in value_lists]
    total_rows = prod(counts) if counts else 0

    print("\n▼ 各列のユニーク数")
    for col, n in zip(target_columns, counts):
        print(f"  - {col}: {n:,} 個")
    print(f"▼ 生成予定行数（直積）: {total_rows:,} 行")

    if total_rows == 0:
        raise SystemExit("組み合わせ対象がありません（ユニーク値が空）。")

    ans = input("この処理を実行しますか？ (y/n): ").strip().lower()
    if ans != "y":
        raise SystemExit("中止しました。")

    base_name = f"AllCombinations_{input_file.stem}.csv"
    csv_base = input_file.parent / base_name

    rows_iter_all = iter_product(value_lists)
    logging.info("CSV分割出力を開始します。")
    written, parts, base_used = write_csv_in_parts_unique(csv_base, target_columns, rows_iter_all, total_rows)
    logging.info(f"CSV出力完了: {written:,} 行 / {parts} ファイル / ベース: {base_used.name}")
    logging.info("処理が完了しました。")

if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        logging.info(str(e))
    except Exception as e:
        logging.exception("予期せぬエラーが発生しました。")
        sys.exit(1)
