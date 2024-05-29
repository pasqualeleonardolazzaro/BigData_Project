import sys


# Dictionary to hold data for each ticker and year
data = {}

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')
    if len(parts) < 7:
        continue  # Skip malformed lines

    ticker = parts[1]
    date = parts[2]
    try:
        close_price = float(parts[6])
    except ValueError:
        continue  # Skip lines where close price is not a number

    year = int(date[:4])
    if year < 2000:
        continue  # Filter out data before the year 2000

    # Key by ticker and year
    key = (ticker, year)

    if key not in data:
        # Store both the first and last close prices and dates
        data[key] = {'first_date': date, 'last_date': date, 'first_close': close_price, 'last_close': close_price}
    else:
        if date < data[key]['first_date']:
            data[key]['first_date'] = date
            data[key]['first_close'] = close_price
        if date > data[key]['last_date']:
            data[key]['last_date'] = date
            data[key]['last_close'] = close_price

# Output the percentage change for each ticker and year
for (ticker, year), values in data.items():
    if values['first_close'] == 0:
        continue  # Avoid division by zero
    percentage_change = ((values['last_close'] - values['first_close']) / values['first_close']) * 100
    print(f"{year}\t{ticker}:{percentage_change:.1f}%")