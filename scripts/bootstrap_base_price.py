import sys
import yfinance as yf
from supabase import create_client
import os

# -----------------------------
# Supabase init (env vars)
# -----------------------------
SUPABASE_KEY="sb_secret_ZM3eyP6AYlfNHEg7yGbYjA_T_xr_fEj"
SUPABASE_URL="https://javabjsklqxusqrkdbst.supabase.co"

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
# Previous close
# -----------------------------
def get_previous_close(yf_symbol: str) -> float:
    ticker = yf.Ticker(yf_symbol)
    hist = ticker.history(period="2d", interval="1d")

    if len(hist) < 2:
        raise ValueError("Insufficient history")

    close_price = hist.iloc[-2]["Close"]
    if close_price is None:
        raise ValueError("Close missing")

    return round(float(close_price), 2)


# -----------------------------
# Main
# -----------------------------
def main(region: str):
    region = region.upper()

    # Fetch finalized tickers from Gemini output
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

        # Check if already initialized
        existing = (
            supabase
            .table("market_prices")
            .select("base_price")
            .eq("symbol", symbol)
            .eq("region", region)
            .limit(1)
            .execute()
            .data
        )

        if existing and existing[0]["base_price"] is not None:
            continue

        try:
            yf_sym = yahoo_symbol(symbol, region)
            prev_close = get_previous_close(yf_sym)

            supabase.table("market_prices").upsert({
                "symbol": symbol,
                "region": region,
                "base_price": prev_close,
                "display_price": prev_close,
            }).execute()

            print(f"[OK] {symbol} ({region}) → {prev_close}")

        except Exception as e:
            print(f"[WARN] {symbol} ({region}) skipped: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: bootstrap_base_prices.py <REGION>")
    main(sys.argv[1])
