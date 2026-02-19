import yfinance as yf
from supabase import create_client
import os

# -----------------------------
# Supabase init
# -----------------------------
SUPABASE_KEY = "sb_secret_ZM3eyP6AYlfNHEg7yGbYjA_T_xr_fEj"
SUPABASE_URL = "https://javabjsklqxusqrkdbst.supabase.co"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# -----------------------------
# Canonical symbol (DB truth)
# -----------------------------
def canonical_symbol(symbol: str) -> str:
    """
    Strip any suffix like .NS or .L.
    This is what we store in DB.
    """
    return symbol.strip().upper().split(".")[0]


# -----------------------------
# Convert to yfinance symbol
# -----------------------------
def to_yf_symbol(symbol: str, region: str) -> str:
    symbol = canonical_symbol(symbol)
    region = (region or "").upper()

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
        raise ValueError("No history")

    close_price = hist.iloc[-2]["Close"]
    return round(float(close_price), 2)


# -----------------------------
# Fetch markets
# -----------------------------
def fetch_markets():
    data = supabase.table("markets").select("symbol, region").execute().data
    return [(row["symbol"], row["region"]) for row in data]


# -----------------------------
# Fetch holdings
# -----------------------------
def fetch_holdings():
    data = supabase.table("holdings").select("symbol, region").execute().data

    pairs = []
    for row in data:
        symbol = row["symbol"]
        region = row.get("region") or "US"
        pairs.append((symbol, region))

    return pairs


# -----------------------------
# Merge universe
# -----------------------------
def build_universe():
    universe = set()

    for pair in fetch_markets():
        universe.add(pair)

    for pair in fetch_holdings():
        universe.add(pair)

    return list(universe)


# -----------------------------
# Main
# -----------------------------
def main():
    universe = build_universe()

    print(f"Bootstrapping {len(universe)} symbols")

    for symbol, region in universe:
        try:
            canon = canonical_symbol(symbol)
            yf_symbol = to_yf_symbol(symbol, region)

            prev_close = get_previous_close(yf_symbol)

            supabase.table("market_prices").upsert(
                {
                    "symbol": canon,  # ← NO SUFFIX
                    "region": region,
                    "base_price": prev_close,
                    "display_price": prev_close,
                },
                on_conflict="symbol,region"
            ).execute()

            print(f"[OK] {canon} ({region}) → {prev_close}")

        except Exception as e:
            print(f"[WARN] {symbol} ({region}) skipped: {e}")


if __name__ == "__main__":
    main()
