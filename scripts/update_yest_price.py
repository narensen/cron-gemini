import sys
import yfinance as yf
from supabase import create_client

# -----------------------------
# Supabase init
# -----------------------------
SUPABASE_KEY = "YOUR_KEY"
SUPABASE_URL = "YOUR_URL"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# -----------------------------
# Yahoo symbol mapping
# -----------------------------
def yahoo_symbol(symbol: str, region: str) -> str:
    region = region.upper()

    if region == "CRYPTO":
        return f"{symbol}-USD"

    if region == "INDIA":
        return f"{symbol}.NS"

    if region == "LONDON":
        return f"{symbol}.L"

    return symbol


# -----------------------------
# Equity logic
# -----------------------------
def get_equity_move(yf_symbol: str):
    ticker = yf.Ticker(yf_symbol)

    hist = ticker.history(period="5d", interval="1d")

    if hist.empty or len(hist) < 1:
        raise ValueError("No equity data")

    row = hist.iloc[-1]

    open_price = row["Open"]
    close_price = row["Close"]

    if open_price is None or open_price == 0:
        raise ValueError("Invalid open")

    pct_change = ((close_price - open_price) / open_price) * 100

    return round(float(pct_change), 2)


# -----------------------------
# Crypto logic — rolling 24h
# -----------------------------
def get_crypto_24h_move(yf_symbol: str):
    ticker = yf.Ticker(yf_symbol)

    # hourly candles to approximate rolling window
    hist = ticker.history(period="2d", interval="1h")

    if hist.empty or len(hist) < 24:
        raise ValueError("No crypto data")

    latest = hist["Close"].iloc[-1]
    prev_24h = hist["Close"].iloc[-24]

    pct_change = ((latest - prev_24h) / prev_24h) * 100

    return round(float(pct_change), 2)


# -----------------------------
# Dispatcher
# -----------------------------
def get_price_change(symbol: str, region: str):
    yf_sym = yahoo_symbol(symbol, region)

    if region == "CRYPTO":
        return get_crypto_24h_move(yf_sym)

    return get_equity_move(yf_sym)


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
            pct_change = get_price_change(symbol, region)

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
