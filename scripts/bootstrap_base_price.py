import yfinance as yf
import httpx
from supabase import create_client

SUPABASE_KEY = "sb_secret_ZM3eyP6AYlfNHEg7yGbYjA_T_xr_fEj"
SUPABASE_URL = "https://javabjsklqxusqrkdbst.supabase.co"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://finance.yahoo.com/",
}


# -----------------------------
# Canonical ticker
# -----------------------------
def canonical(symbol):
    return symbol.strip().upper().split(".")[0]


# -----------------------------
# Region validator
# -----------------------------
def matches_region(region, exch_disp, exchange):
    region = region.upper()

    if region == "LONDON":
        return "LSE" in exch_disp or "London" in exch_disp

    if region == "INDIA":
        return "NSE" in exch_disp or "BSE" in exch_disp

    if region == "US":
        return exchange in {"NMS", "NYQ", "NGM", "ASE"}

    if region == "CRYPTO":
        return True

    return False


# -----------------------------
# Validate ticker via Yahoo
# -----------------------------
async def validate_symbol(symbol, region):
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            SEARCH_URL,
            params={"q": symbol, "quotesCount": 10, "newsCount": 0},
            headers=HEADERS,
        )

        quotes = resp.json().get("quotes", [])

        for q in quotes:
            sym = q.get("symbol")
            exch_disp = q.get("exchDisp", "")
            exchange = q.get("exchange")

            if canonical(sym) == canonical(symbol) and matches_region(region, exch_disp, exchange):
                return canonical(sym)

    return None


# -----------------------------
# Build yfinance symbol
# -----------------------------
def to_yf(symbol, region):
    if region == "INDIA":
        return f"{symbol}.NS"
    if region == "LONDON":
        return f"{symbol}.L"
    return symbol


# -----------------------------
# Fetch history once
# -----------------------------
def fetch_history(yf_symbol):
    ticker = yf.Ticker(yf_symbol)
    hist = ticker.history(period="2d", interval="1d")

    if len(hist) < 2:
        raise ValueError("Not enough history")

    return hist


# -----------------------------
# Compute metrics
# -----------------------------
def compute_metrics(hist):
    day_before = hist.iloc[-2]
    yesterday = hist.iloc[-1]

    base_price = round(float(day_before["Close"]), 2)

    open_price = float(yesterday["Open"])
    close_price = float(yesterday["Close"])

    if open_price == 0:
        change = 0.0
    else:
        change = ((open_price - close_price) / open_price) * 100

    change = round(change, 4)

    return base_price, change


# -----------------------------
# Fetch pairs from DB
# -----------------------------
def fetch_pairs(region):
    rows = (
        supabase.table("markets")
        .select("company_name, symbol")
        .eq("region", region)
        .execute()
        .data
    )

    return rows


# -----------------------------
# Main
# -----------------------------
async def main(region):
    rows = fetch_pairs(region)

    print(f"Bootstrapping {len(rows)} symbols")

    for row in rows:
        company = row["company_name"]
        raw_symbol = row["symbol"]

        try:
            symbol = await validate_symbol(raw_symbol, region)

            if not symbol:
                print(f"[SKIP] {company} → invalid ticker {raw_symbol}")
                continue

            yf_symbol = to_yf(symbol, region)

            print(f"Validated {company} → {yf_symbol}")

            hist = fetch_history(yf_symbol)
            price, price_change = compute_metrics(hist)

            supabase.table("market_prices").upsert(
                {
                    "symbol": yf_symbol,
                    "region": region,
                    "base_price": price,
                    "display_price": price,
                    "yesterday_price_change": price_change,
                },
                on_conflict="symbol,region",
            ).execute()

            print(f"[OK] {symbol} → price {price} | change {price_change}%")

        except Exception as e:
            print(f"[WARN] {company} failed: {e}")


if __name__ == "__main__":
    import sys
    import asyncio

    region = sys.argv[1]
    asyncio.run(main(region))
