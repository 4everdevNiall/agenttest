import os, json, time, re
import pandas as pd
from atproto import Client

# ====== ENV VARS ======
BSKY_HANDLE  = os.environ["BSKY_HANDLE"]   # e.g. testpage.bsky.social
BSKY_APP_PWD = os.environ["BSKY_APP_PWD"]  # Bluesky App Password

# Excel location (defaults to repo's data/survey.xlsx)
EXCEL_PATH   = os.getenv("EXCEL_PATH", "data/survey.xlsx")

# Optional overrides via repo Variables/Secrets
TIMESTAMP_COL = os.getenv("TIMESTAMP_COL", "Timestamp")
NAME_COL      = os.getenv("NAME_COL", "Name")
MESSAGE_COL   = os.getenv("MESSAGE_COL", "Message")
SHEET_NAME    = os.getenv("SHEET_NAME", None)  # e.g., "Form responses 1" (None = first sheet)

STATE_FILE = "last_row.json"  # remembers the last posted row index


# ---------- Helpers ----------
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_index": -1}


def save_state(s):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f)


def fetch_dataframe() -> pd.DataFrame:
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME or 0, header=0)
    return df.fillna("")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())


def find_review_column(df: pd.DataFrame) -> str:
    """
    Priority:
      1) If MESSAGE_COL exists, use it.
      2) Regex match on header meaning (ETHOS + 'did/does' + 'well', etc.).
      3) Fallback to 'textiest' column (largest avg cell length).
    """
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
    if not review:
        return ""

    name = (str(row.get(NAME_COL, "Anonymous")).strip() or "Anonymous")
    ts   = str(row.get(TIMESTAMP_COL, "")).strip()

    base = review
    if name:
        base += f"\n— {name}"
    if ts:
        base += f" • {ts}"
    return base[:290]  # ~300 char safety


# ---------- Main ----------
def main():
    state = load_state()
    df = fetch_dataframe()
    if df.empty:
        print("[info] No data found in sheet.")
        return

    review_col = find_review_column(df)
    rows = df.to_dict("records")

    start = state["last_index"] + 1
    if start >= len(rows):
        print("[info] No new rows to post.")
        return

    client = Client()
    client.login(BSKY_HANDLE, BSKY_APP_PWD)

    for idx in range(start, len(rows)):
        text = format_post(rows[idx], review_col)
        if text.strip():
            client.send_post(text=text)
            print(f"[posted] row {idx} ({review_col}): {text[:80]}...")
            time.sleep(2)
        else:
            print(f"[skip] row {idx}: empty or no review text")
        state["last_index"] = idx
        save_state(state)


if __name__ == "__main__":
    main()
7
