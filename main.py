import yfinance as yf
from supabase import create_client
import os

# -----------------------------
# Supabase init
# -----------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# -----------------------------
# Yahoo symbol mapping
# -----------------------------
def yahoo_symbol(symbol: str, region: str) -> str:
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
        raise ValueError("Insufficient history")

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
            yf_sym = yahoo_symbol(symbol, region)
            prev_close = get_previous_close(yf_sym)

            supabase.table("market_prices").upsert(
                {
                    "symbol": symbol,
                    "region": region,
                    "base_price": prev_close,
                    "display_price": prev_close,
                },
                on_conflict="symbol,region"
            ).execute()

            print(f"[OK] {symbol} ({region}) → {prev_close}")

        except Exception as e:
            print(f"[WARN] {symbol} ({region}) skipped: {e}")


if __name__ == "__main__":
    main()
