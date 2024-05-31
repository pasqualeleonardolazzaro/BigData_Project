#!/usr/bin/env python3
import sys
from collections import defaultdict

trends = defaultdict(list)
ticker_year_trends = defaultdict(dict)

# Leggiamo l'input e organizziamo i dati
for line in sys.stdin:
    ticker, year, percentage_change, name = line.strip().split('\t')
    year = int(year)
    percentage_change = float(percentage_change)
    trends[ticker].append((year, percentage_change, name))
    ticker_year_trends[ticker][year] = percentage_change

# Mappa per raggruppare i ticker con gli stessi trend negli stessi anni
pattern_groups = defaultdict(list)

# Processiamo i dati per ogni ticker una volta
for ticker, years_data in ticker_year_trends.items():
    sorted_years = sorted(years_data.keys())

    # Cerchiamo pattern di 3 anni consecutivi
    for i in range(len(sorted_years) - 2):
        year1, year2, year3 = sorted_years[i:i + 3]
        if int(year3) - int(year1) == 2:  # Controlliamo che gli anni siano consecutivi
            # Creiamo una chiave unica per il pattern di trend di 3 anni
            # pattern_key = (years_data[year1], years_data[year2], years_data[year3])
            pattern_groups_key = (year1, year2, year3, years_data[year1], years_data[year2], years_data[year3])
            pattern_groups[pattern_groups_key].append(ticker)

# A questo punto pattern_groups contiene gruppi di ticker che condividono lo stesso trend in 3 anni consecutivi

# Possiamo quindi confrontare i gruppi o stampare i risultati
for (year1, year2, year3, p1, p2, p3), tickers in pattern_groups.items():
    if len(tickers) > 1:
        print(f"Tickers {tickers} share the same trend [{p1}, {p2}, {p3}] in years [{year1}, {year2}, {year3}]")