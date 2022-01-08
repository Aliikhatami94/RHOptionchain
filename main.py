import mysql.connector as mysql
from config import config
import pyotp
import robin_stocks.robinhood as r
import requests
import random
import pyspark as sp
import time
import dask
import asyncio
import aiohttp

short_min_prob = 0.75
long_min_prob = 0.50
min_ror = 3
ticker = 'BABA'

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

    async def options_orders(self, symbol, side, optionsymbol, qty):
        async with aiohttp.ClientSession() as session:
            url = self.orderlink.format(self.baseurl)
            option_response = await session.post(
                url,
                data={'class': 'option', 'symbol': str(symbol), 'option_symbol': str(optionsymbol),
                      'side': str(side), 'quantity': str(qty), 'type': 'market', 'duration': 'day'},
                headers={'Authorization': 'Bearer {}'.format(self.token),
                         'Accept': 'application/json'}
            )
            return await option_response.json()

    async def expiration_response(self, symbol):
        async with aiohttp.ClientSession() as session:
            url = self.expslink.format(self.baseurl)
            exp_response = await session.get(
                url,
                params={'symbol': f'{symbol}','includeAllRoots': 'true', 'strikes': 'true'},
                headers={'Authorization': 'Bearer {}'.format(self.token), 'Accept': 'application/json'},
                ssl=False
            )
            return await exp_response.json()

    async def chains(self, symbol, exp):
        async with aiohttp.ClientSession() as session:
            url = self.optionslink.format(self.baseurl)
            option_response = await session.get(
                url,
                params={'symbol': f'{symbol}', 'expiration': f'{exp}', 'greeks': 'true'},
                headers={'Authorization': 'Bearer {}'.format(self.token),
                         'Accept': 'application/json'},
                ssl=False
            )
            return await option_response.json()

#__________________________________

async def optionchains(symbol, exp, type, strike):
    endpoint = Tradier_endpoints()
    option_response = await endpoint.chains(symbol, exp)
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

def longput(symbol=None):
    endpoint = Tradier_endpoints()
    expirationfxn = asyncio.run(endpoint.expiration_response(symbol))
    expirations = expirationfxn['expirations']['expiration']

    longs = []
    for expiration in expirations:
        expiry = expiration['date']
        puts = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='put', expirationDate=expiry)
        for put in puts:
            strike = float(put['strike_price'])
            expiration = put['expiration_date']
            bid = round(float(put['bid_price']), 1)
            lpop = round(float(put['chance_of_profit_long']), 2)
            ssymb = str(optionchains(symbol, expiration, 'call', strike))
            if lpop > long_min_prob:
                longs.append(ssymb)
    random_picks = random.choice(longs)
    #endpoints.options_orders(symbol, 'buy_to_open', symb, 1)

def shortput(symbol):
    endpoint = Tradier_endpoints()
    expirationfxn = asyncio.run(endpoint.expiration_response(symbol))
    expirations = expirationfxn['expirations']['expiration']

    shorts = []
    start = time.time()
    for expiration in expirations:
        expiry = expiration['date']
        puts = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='put', expirationDate=expiry)
        for put in puts:
            strike = float(put['strike_price'])
            expiration = put['expiration_date']
            bid = round(float(put['bid_price']), 1)
            prob = round(float(put['chance_of_profit_short']), 2)
            optionchain = str(optionchains(symbol, expiration, 'call', strike))
            cost = quote(symbol) * 100
            prem = bid * 100
            ror = (prem / cost) * 100
            if prob > short_min_prob:
                if ror > min_ror:
                    shorts.append(optionchain)
    graph = dask.delayed()(shorts)
    computed_list = graph.compute()
    random_pick = random.choice(computed_list)
    print(random_pick)
    end = time.time()
    total = end - start
    print(total)

def shortcall(symbol):
    endpoint = Tradier_endpoints()
    expirationfxn = asyncio.run(endpoint.expiration_response(symbol))
    expirations = expirationfxn['expirations']['expiration']

    shorts = []
    start = time.time()
    for expiration in expirations:
        expiry = expiration['date']
        calls = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='call', expirationDate=expiry)

        for call in calls:
            strike = float(call['strike_price'])
            exp = call['expiration_date']
            bid = round(float(call['bid_price']), 1)
            prob = round(float(call['chance_of_profit_short']), 2)
            optionchain = str(asyncio.run(optionchains(symbol, exp, 'call', strike)))
            cost = quote(symbol) * 100
            premium = bid * 100
            ror = (premium / cost) * 100
            if prob > short_min_prob:
                if ror > min_ror:
                    shorts.append(optionchain)
        graph = dask.delayed()(shorts)
        computed_list = graph.compute()
        print(computed_list)
        end = time.time()
        total = end - start
        print(total)
    #end.options_orders(symbol, 'sell_to_open', symb, 1)

def longcall(symbol):
    endpoint = Tradier_endpoints()
    expirationfxn = asyncio.run(endpoint.expiration_response(symbol))
    expirations = expirationfxn['expirations']['expiration']

    longs = []
    for expiration in expirations:
        expiry = expiration['date']
        calls = r.options.find_options_by_specific_profitability(inputSymbols=symbol, optionType='call', expirationDate=expiry)
        for call in calls:
            strike = float(call['strike_price'])
            expiration = call['expiration_date']
            bid = round(float(call['bid_price']), 1)
            lpop = round(float(call['chance_of_profit_long']), 2)
            ssymb = str(optionchains(symbol, expiration, 'call', strike))
            prem = bid * 100
            if lpop > long_min_prob:
                longs.append(ssymb)
    random_picks = random.choice(longs)
    #end.options_orders(symbol, 'buy_to_open', symb, 1)

def profitable_options(symbol):
    longput(symbol)
    shortput(symbol)
    longcall(symbol)
    shortcall(symbol)

shortcall(ticker)
