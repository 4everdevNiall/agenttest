import os, json, time, re
import pandas as pd
from atproto import Client

# ====== ENV VARS ======
BSKY_HANDLE  = os.environ["BSKY_HANDLE"]
BSKY_APP_PWD = os.environ["BSKY_APP_PWD"]

# Excel in repo
EXCEL_PATH   = os.getenv("EXCEL_PATH", "data/survey.xlsx")

# Optional overrides
TIMESTAMP_COL = os.getenv("TIMESTAMP_COL", "Timestamp")
NAME_COL      = os.getenv("NAME_COL", "Name")
MESSAGE_COL   = os.getenv("MESSAGE_COL", "Message")
SHEET_NAME    = os.getenv("SHEET_NAME", None)  # None = first sheet

# Debug/controls
RESET_STATE          = os.getenv("RESET_STATE", "false").lower() == "true"
FORCE_POST_FIRST_N   = int(os.getenv("FORCE_POST_FIRST_N", "0"))  # e.g., 1 or 3 to force a few posts

STATE_FILE = "last_row.json"

def load_state():
    if RESET_STATE:
        print("[debug] RESET_STATE=true → starting from -1 and deleting state file if present")
        try:
            if os.path.exists(STATE_FILE):
                os.remove(STATE_FILE)
        except Exception as e:
            print(f"[warn] Could not delete state file: {e}")
        return {"last_index": -1}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
            print(f"[debug] Loaded state: {s}")
            return s
    print("[debug] No state file found, starting from -1")
    return {"last_index": -1}

def save_state(s):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f)
    print(f"[debug] Saved state: {s}")

def fetch_dataframe() -> pd.DataFrame:
    print(f"[debug] Reading Excel from: {EXCEL_PATH} (sheet={SHEET_NAME or 0})")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME or 0, header=0)
    print(f"[debug] Columns: {list(df.columns)}  | rows: {len(df)}")
    return df.fillna("")

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def find_review_column(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    if MESSAGE_COL in cols:
        print(f"[info] Using MESSAGE_COL override: {MESSAGE_COL}")
        return MESSAGE_COL

    norm_cols = {col: _norm(col) for col in cols}
    needles = [
        r"\bethos\b.*\b(did|does)\b.*\bwell\b",
        r"\bwhat\b.*\bethos\b.*\bwell\b",
        r"\bwhat went well\b.*\bethos\b",
        r"\bfeedback\b.*\bethos\b.*\bwell\b",
    ]
    for col, nc in norm_cols.items():
        if any(re.search(p, nc) for p in needles):
            print(f"[info] Auto-detected review column: {col}")
            return col

    def avg_len(series: pd.Series) -> float:
        try:
            return series.astype(str).str.len().mean()
        except Exception:
            return 0.0

    textiest = max(cols, key=lambda c: avg_len(df[c]))
    print(f"[warn] Falling back to textiest column: {textiest}")
    return textiest

def format_post(row: dict, review_col: str) -> str:
    review = str(row.get(review_col, "")).strip()
    name = (str(row.get(NAME_COL, "Anonymous")).strip() or "Anonymous")
    ts   = str(row.get(TIMESTAMP_COL, "")).strip()

    if not review:
        return ""
    base = review
    if name:
        base += f"\n— {name}"
    if ts:
        base += f" • {ts}"
    return base[:290]

def main():
    state = load_state()
    df = fetch_dataframe()
    if df.empty:
        print("[info] No data found in sheet.")
        return

    review_col = find_review_column(df)
    print(f"[debug] Using review column: {review_col}")
    rows = df.to_dict("records")

    # Show first 3 extracted texts for sanity
    samples = []
    for i in range(min(3, len(rows))):
        t = format_post(rows[i], review_col)
        samples.append(t[:120])
    print(f"[debug] First 3 formatted samples: {samples}")

    start = state["last_index"] + 1
    print(f"[debug] Last index was {state['last_index']} → starting at {start} of {len(rows)} rows")

    # Force posts if requested
    if FORCE_POST_FIRST_N > 0:
        print(f"[debug] FORCE_POST_FIRST_N={FORCE_POST_FIRST_N} → will post first N rows regardless of state")
        start = 0

    if start >= len(rows) and FORCE_POST_FIRST_N == 0:
        print("[info] No new rows to post.")
        return

    client = Client()
    print(f"[debug] Logging in to Bluesky as {BSKY_HANDLE}")
    client.login(BSKY_HANDLE, BSKY_APP_PWD)

    posted = 0
    end = len(rows) if FORCE_POST_FIRST_N == 0 else min(FORCE_POST_FIRST_N, len(rows))
    for idx in range(start, end):
        text = format_post(rows[idx], review_col)
        if text.strip():
            client.send_post(text=text)
            posted += 1
            print(f"[posted] row {idx} ({review_col}): {text[:120]}...")
            time.sleep(2)
        else:
            print(f"[skip] row {idx}: empty or no review text")
        state["last_index"] = idx
        save_state(state)

    print(f"[info] Done. Posted {posted} item(s).")

if __name__ == "__main__":
    main()
