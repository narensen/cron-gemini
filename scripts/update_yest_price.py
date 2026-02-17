import sys
import yfinance as yf
from supabase import create_client

# -----------------------------
# Supabase init
# -----------------------------
SUPABASE_KEY = "sb_secret_ZM3eyP6AYlfNHEg7yGbYjA_T_xr_fEj"
SUPABASE_URL = "https://javabjsklqxusqrkdbst.supabase.co"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# -----------------------------
# Yahoo symbol mapping
# -----------------------------
def yahoo_symbol(symbol: str, region: str) -> str:
    if region == "INDIA":
        return f"{symbol}.NS"
    if region == "LONDON":
        return f"{symbol}.L"
    return symbol


# -----------------------------
# Compute yesterday move
# -----------------------------
def get_yesterday_move(yf_symbol: str):
    ticker = yf.Ticker(yf_symbol)

    hist = ticker.history(period="5d", interval="1d")

    if hist.empty:
        raise ValueError("No history")

    row = hist.iloc[-1]

    open_price = row["Open"]
    close_price = row["Close"]

    if open_price is None or open_price == 0:
        raise ValueError("Invalid open")

    pct_change = ((close_price - open_price) / open_price) * 100

    return round(float(pct_change), 2)


# -----------------------------
# Main
# -----------------------------
def main(region: str):
    region = region.upper()

    tickers = (
        supabase
        .table("markets")
        .select("symbol")
        .eq("region", region)
        .execute()
        .data
    )

    for row in tickers:
        symbol = row["symbol"]

        try:
            yf_sym = yahoo_symbol(symbol, region)
            pct_change = get_yesterday_move(yf_sym)

            supabase.table("market_prices") \
                .update({
                    "yesterday_price_change": pct_change
                }) \
                .eq("symbol", symbol) \
                .eq("region", region) \
                .execute()

            print(f"[UPDATED] {symbol} ({region}) → {pct_change}%")

        except Exception as e:
            print(f"[WARN] {symbol} skipped: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: eod_update.py <REGION>")

    main(sys.argv[1])
