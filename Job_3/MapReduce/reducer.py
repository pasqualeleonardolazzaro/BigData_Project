#!/usr/bin/env python3
import sys
from collections import defaultdict

def read_input(file):
    for line in file:
        yield line.strip()


trends = defaultdict(list)

for line in sys.stdin:
    ticker, year, percentage_change, name = line.split(';')
    year = int(year)
    percentage_change = float(percentage_change)
    trends[ticker].append((year, percentage_change, name))
'''
print("Trends dictionary:")
for ticker in trends:
    print(ticker, trends[ticker])
'''
trend_groups = defaultdict(set)
tickers = list(trends.keys())

for i in range(len(tickers)):
    for j in range(i + 1, len(tickers)):
        ticker1 = tickers[i]
        ticker2 = tickers[j]

        # Troviamo gli anni comuni per entrambe le aziende
        common_years = sorted(set(year for year, _, _ in trends[ticker1]) & set(year for year, _, _ in trends[ticker2]))

        # Verifichiamo se ci sono abbastanza anni comuni per confrontare i trend
        if len(common_years) < 3:
            continue

        # Confrontiamo i trend per tre anni consecutivi
        for k in range(len(common_years) - 2):
            years_slice = common_years[k:k+3]
            #print(f"Years slice: {years_slice}")

            trend1 = [(year, trend) for year, trend, _ in trends[ticker1] if year in years_slice]
            trend2 = [(year, trend) for year, trend, _ in trends[ticker2] if year in years_slice]

            #print(f"Trends for {ticker1} in years {years_slice}: {trend1}")
            #print(f"Trends for {ticker2} in years {years_slice}: {trend2}")

            if len(trend1) == 3 and len(trend2) == 3 and trend1 == trend2:
                trend_key = tuple((year, trend) for year, trend in trend1)
                trend_groups[trend_key].add(trends[ticker1][0][2])  # Name of the first company
                trend_groups[trend_key].add(trends[ticker2][0][2])  # Name of the second company
                #print(f"Matching trend found: {trend_key} for companies: {trend_groups[trend_key]}")

# Stampiamo i risultati
#print("\nFinal output:")
for trend_key, companies in trend_groups.items():
    if len(companies) > 1:
        formatted_data = [f"{year}:{change}%" for year, change in trend_key]
        companies_str = ', '.join(companies)
        trends_str = ', '.join(formatted_data)
        print(f"{{ {companies_str} }}: {trends_str}")
