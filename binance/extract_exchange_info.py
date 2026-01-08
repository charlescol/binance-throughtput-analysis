import requests
import json

# URL for Binance exchange information
API_URL = "https://api.binance.com/api/v3/exchangeInfo"

# Fetch exchange information from Binance API
response = requests.get(API_URL)
response.raise_for_status()
exchange_info = response.json()

# Prepare a dictionary to hold symbol factors
tick_step_factors = []

# Helper to compute factor: position of first significant digit after decimal
def significant_factor(decimal_str):
    """
    Given a string like '0.00000100', returns the factor = position of first non-zero
    character after the decimal point (1-based).
    """
    if '.' not in decimal_str:
        return 0
    _, frac = decimal_str.split('.')
    for idx, ch in enumerate(frac):
        if ch != '0':
            return idx + 1
    return 0

# Iterate through each symbol in the exchange information
for item in exchange_info.get('symbols', []):
    symbol = item.get('symbol')
    tick_str = None
    step_str = None

    # Each symbol has a list of filters; we want PRICE_FILTER and LOT_SIZE
    for f in item.get('filters', []):
        filter_type = f.get('filterType')
        if filter_type == 'PRICE_FILTER':
            tick_str = f.get('tickSize')
        elif filter_type == 'LOT_SIZE':
            step_str = f.get('stepSize')

    # Compute factors if values exist
    if tick_str is not None and step_str is not None:
        tick_factor = significant_factor(tick_str)
        step_factor = significant_factor(step_str)
        tick_step_factors.append({
            'tickvalueFactor': tick_factor,
            'stepSizeFactor': step_factor,
            'symbol': symbol
        })

# Write the resulting data to a JSON file
output_filename = 'symbol_factors.json'
with open(output_filename, 'w') as outfile:
    json.dump({'symbols' : tick_step_factors}, outfile, indent=4)

print(f"Factors saved to {output_filename}")
