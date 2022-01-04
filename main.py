import mysql.connector as mysql
from config import config
import pyotp
import robin_stocks.robinhood as r
import requests
import random

short_min_prob = 0.80
long_min_prob = 0.50
minror = 3
ticker = 'GOOGL'

#__________________________________

totp = pyotp.TOTP(config['ROBINHOOD DATA']['MFA']).now()
r.login(config['ROBINHOOD DATA']['USER'], config['ROBINHOOD DATA']['PASS'], store_session=False, mfa_code=totp)

mydb = mysql.connect(
  host=config['SCALEGRID']['HOST'],
  user=config['SCALEGRID']['USER'],
  password=config['SCALEGRID']['PASS'],
    database=config['SCALEGRID']['DB']
)
mycursor = mydb.cursor()
#__________________________________

class Tradier_endpoints:
    def __init__(self):
        self.token = config['PAPER TRADIER']['ACCESS_TOKEN']
        self.account = '{}/VA81452706'
        self.baseurl = config['PAPER TRADIER']['BASE_URL']

        self.accountid = self.account + "/orders"
        self.stockslink = config['PAPER TRADIER']['MARKETS_URL'] + "/quotes"
        self.optionslink = config['PAPER TRADIER']['MARKETS_URL'] + "/options/chains"
        self.expslink = config['PAPER TRADIER']['MARKETS_URL'] + "/options/expirations"
        self.strikeslink = config['PAPER TRADIER']['MARKETS_URL'] + "/options/strikes"
        self.statslink = config['REAL TRADIER']['FUNDAMENTALS_URL'] + "/statistics"
        self.finlink = config['REAL TRADIER']['FUNDAMENTALS_URL'] + "/financials"
        self.orderlink = config['REAL TRADIER']['ORDER_URL'] + self.accountid
        self.orderlink = self.accountid.format(config['REAL TRADIER']['ORDER_URL'])

    def options_orders(self, symbol, side, optionsymbol, qty):
        url = self.orderlink.format(self.baseurl)
        option_response = requests.post(
            url,
            data={'class': 'option', 'symbol': str(symbol), 'option_symbol': str(optionsymbol),
                  'side': str(side), 'quantity': str(qty), 'type': 'market', 'duration': 'day'},
            headers={'Authorization': 'Bearer {}'.format(self.token),
                     'Accept': 'application/json'}
        )
        return option_response.json()

    def expiration_response(self, symbol):
        url = self.expslink.format(self.baseurl)
        exp_response = requests.get(
            url,
            params={'symbol': f'{symbol}', 'includeAllRoots': 'true', 'strikes': 'true'},
            headers={'Authorization': 'Bearer {}'.format(self.token), 'Accept': 'application/json'}
        )
        return exp_response.json()

    def chains(self, symbol, exp):
        url = self.optionslink.format(self.baseurl)
        option_response = requests.get(
            url,
            params={'symbol': f'{symbol}', 'expiration': f'{exp}', 'greeks': 'true'},
            headers={'Authorization': 'Bearer {}'.format(self.token),
                     'Accept': 'application/json'}
        )
        return option_response.json()
#__________________________________

def option_symbol(symbol, exp, type, strike):
    endpoint = Tradier_endpoints()
    option_response = endpoint.chains(symbol, exp)
    options = option_response['options']['option']
    for option in options:
        optiontype = option['option_type']
        optionstrike = option['strike']
        optionsymbol = option['symbol']
        if optiontype == type and optionstrike == strike:
            return optionsymbol

def quote(symbol):
    quotes = r.stocks.get_quotes(symbol)
    for quote in quotes:
        last = round(float(quote['last_trade_price']),2)
        return last

#-------------------------------#

def longput(symbol):
    end = Tradier_endpoints()
    exp = Tradier_endpoints()
    expirationfxn = exp.expiration_response(symbol)
    expirations = expirationfxn['expirations']['expiration']

    lchain = []
    for expiration in expirations:
        expiry = expiration['date']
        puts = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='put', expirationDate=expiry)
        for put in puts:
            strike = float(put['strike_price'])
            expiration = put['expiration_date']
            bid = round(float(put['bid_price']), 1)
            lpop = round(float(put['chance_of_profit_long']), 2)
            ssymb = str(option_symbol(symbol, expiration, 'call', strike))
            prem = bid * 100
            if lpop > long_min_prob:
                lchain.append(ssymb)
    symb = random.choice(lchain)
    end.options_orders(symbol, 'buy_to_open', symb, 1)

def shortput(symbol):
    end = Tradier_endpoints()
    exp = Tradier_endpoints()
    expirationfxn = exp.expiration_response(symbol)
    expirations = expirationfxn['expirations']['expiration']

    schain = []
    for expiration in expirations:
        expiry = expiration['date']
        puts = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='put', expirationDate=expiry)
        for put in puts:
            strike = float(put['strike_price'])
            expiration = put['expiration_date']
            bid = round(float(put['bid_price']), 1)
            lpop = round(float(put['chance_of_profit_long']), 2)
            spop = round(float(put['chance_of_profit_short']), 2)
            ssymb = str(option_symbol(symbol, expiration, 'call', strike))
            cost = quote(symbol) * 100
            prem = bid * 100
            ror = (prem / cost) * 100
            if spop > short_min_prob:
                if ror > minror:
                    schain.append(ssymb)
    symb = random.choice(schain)
    end.options_orders(symbol, 'sell_to_open', symb, 1)

def shortcall(symbol):
    end = Tradier_endpoints()
    exp = Tradier_endpoints()
    expirationfxn = exp.expiration_response(symbol)
    expirations = expirationfxn['expirations']['expiration']

    schain = []
    for expiration in expirations:
        expiry = expiration['date']
        calls = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='call', expirationDate=expiry)
        for call in calls:
            strike = float(call['strike_price'])
            expiration = call['expiration_date']
            bid = round(float(call['bid_price']), 1)
            lpop = round(float(call['chance_of_profit_long']), 2)
            spop = round(float(call['chance_of_profit_short']), 2)
            ssymb = str(option_symbol(symbol, expiration, 'call', strike))
            cost = quote(symbol) * 100
            prem = bid * 100
            ror = (prem / cost) * 100
            if spop > short_min_prob:
                if ror > minror:
                    schain.append(ssymb)
    symb = random.choice(schain)
    end.options_orders(symbol, 'sell_to_open', symb, 1)

def longcall(symbol):
    end = Tradier_endpoints()
    exp = Tradier_endpoints()
    expirationfxn = exp.expiration_response(symbol)
    expirations = expirationfxn['expirations']['expiration']

    lchain = []
    for expiration in expirations:
        expiry = expiration['date']
        calls = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='call', expirationDate=expiry)
        for call in calls:
            strike = float(call['strike_price'])
            expiration = call['expiration_date']
            bid = round(float(call['bid_price']), 1)
            lpop = round(float(call['chance_of_profit_long']), 2)
            ssymb = str(option_symbol(symbol, expiration, 'call', strike))
            prem = bid * 100
            if lpop > long_min_prob:
                lchain.append(ssymb)
    symb = random.choice(lchain)
    end.options_orders(symbol, 'buy_to_open', symb, 1)

def profitable_options(symbol):
    longput(symbol)
    shortput(symbol)
    longcall(symbol)
    shortcall(symbol)

profitable_options(ticker)