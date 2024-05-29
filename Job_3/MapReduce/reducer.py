#!/usr/bin/env python3
import sys  # Importing the sys library to interact with system-specific parameters and functions
from collections import defaultdict  # Import defaultdict from collections to handle default values for new keys


def print_groups(groups):  # Define a function to print groups of tickers
    for trend, tickers in groups.items():  # Iterate through each trend and corresponding tickers
        if len(tickers) > 1:  # Only interested in groups with more than one company
            print(f"{tickers}: {trend}")  # Print the group and the trend


current_year = None  # Variable to keep track of the current year being processed
data = defaultdict(lambda: defaultdict(list))  # Nested dictionary to store tickers for each year and trend

for line in sys.stdin:  # Loop over each line in the standard input
    line = line.strip()  # Strip any leading and trailing whitespace from the line
    year, ticker_change = line.split('\t', 1)  # Split the line into year and ticker_change at the first tab
    ticker, change = ticker_change.split(':')  # Split the ticker_change into ticker and change at the colon
    year = int(year)  # Convert the year to an integer

    if current_year is not None and year != current_year:  # Check if the year has changed
        if current_year >= 2002:  # Process only if the year is 2000 or later
            # Find matching trends in three consecutive years
            trends = set(data[current_year].keys()) & set(data[current_year - 1].keys()) & set(data[current_year - 2].keys())
            groups = defaultdict(list)  # Dictionary to store groups of tickers by trend
            for trend in trends:  # Iterate over each trend found in all three years
                grouped_tickers = set(data[current_year][trend]) & set(data[current_year - 1][trend]) & set(data[current_year - 2][trend])
                if grouped_tickers:  # Check if there are tickers common to the trend across all three years
                    groups[trend].extend(list(grouped_tickers))  # Add the tickers to the group for the trend
            print_groups(groups)  # Print the groups

        # Move to next year
        data[current_year - 2].clear
