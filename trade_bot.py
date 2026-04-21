import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime
import requests # This library pings the internet

# 1. DATABASE CONNECTION
raw_password = 'Suku@1997' 
safe_password = urllib.parse.quote_plus(raw_password)

try:
    engine = create_engine(f'postgresql://postgres:{safe_password}@localhost:5432/postgres')
    
    # 2. FETCH LIVE EXCHANGE RATE (USD to INR)
    print("Fetching live exchange rates...")
    response = requests.get('https://open.er-api.com/v6/latest/USD')
    rate_data = response.json()
    live_rate = rate_data['rates']['INR']
    print(f"Current Live Rate: 1 USD = {live_rate} INR")

    # 3. LIST OF PRODUCTS (Arbitrage Opportunity)
    # You can add your eco-friendly bags here later!
    products = [
        {
            'name': 'Araku Specialty Coffee',
            'cat': 'Premium Export',
            'in_cost': 1200.00,
            'us_price': 45.00
        },
        {
            'name': 'Eco-Friendly Jute Bag',
            'cat': 'Textiles',
            'in_cost': 150.00,
            'us_price': 12.00
        }
    ]

    # 4. PROCESS DATA
    final_rows = []
    for p in products:
        total_rev_inr = p['us_price'] * live_rate
        margin = ((total_rev_inr - p['in_cost']) / total_rev_inr) * 100
        
        final_rows.append({
            'product_name': p['name'],
            'category': p['cat'],
            'india_price_inr': p['in_cost'],
            'us_price_usd': p['us_price'],
            'exchange_rate': round(live_rate, 4),
            'profit_margin_percent': round(margin, 2),
            'last_updated': datetime.now()
        })

    # 5. PUSH TO POSTGRESQL
    df = pd.DataFrame(final_rows)
    df.to_sql('international_trade_data', engine, if_exists='append', index=False)

    print(f"--- SUCCESS: Pushed {len(products)} products with live rates! ---")

except Exception as e:
    print(f"--- ERROR: {e} ---")