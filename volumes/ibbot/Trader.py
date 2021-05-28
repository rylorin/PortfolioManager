"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""

import os.path
import time
import logging
import datetime
from datetime import timedelta
from datetime import date
import argparse
import sqlite3
import math

from ibapi import wrapper
from ibapi import utils
from ibapi import contract
from ibapi.client import EClient
from ibapi.utils import iswrapper

# types
from ibapi.common import * # @UnusedWildImport
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import * # @UnusedWildImport
from ibapi.order import * # @UnusedWildImport
from ibapi.order_state import * # @UnusedWildImport
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.ticktype import * # @UnusedWildImport
from ibapi.tag_value import TagValue

from ibapi.account_summary_tags import *

# from ContractSamples import ContractSamples
# from OrderSamples import OrderSamples

from Testbed.Program import printWhenExecuting
from Testbed.Program import TestApp

from Testbed.ContractSamples import ContractSamples

def SetupLogger():
    # RYL
    if os.path.exists("../db/var/log"):
        logfile = time.strftime("../db/var/log/pyibapi.%Y%m%d_%H%M%S.log")
    else:
        if not os.path.exists("log"):
            os.makedirs("log")
        logfile = time.strftime("log/pyibapi.%y%m%d_%H%M%S.log")

    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    # RYL logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
    logging.basicConfig(filename=logfile,
                        filemode="w",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)
#    print("SetupLogger done.")

class TraderOrder:

    @staticmethod
    def Order():
        order = Order()
        order.tif = "DAY"
        order.transmit = False
        return order

    """ <summary>
	#/ A Midprice order is designed to split the difference between the bid and ask prices, and fill at the current midpoint of
	#/ the NBBO or better. Set an optional price cap to define the highest price (for a buy order) or the lowest price (for a sell
	#/ order) you are willing to accept. Requires TWS 975+. Smart-routing to US stocks only.
    </summary>"""
    @staticmethod
    def Midprice(action:str, quantity:float, priceCap:float):
        #! [midprice]
        order = TraderOrder.Order()
        order.action = action
        order.orderType = "MIDPRICE"
        order.totalQuantity = quantity
        order.lmtPrice = priceCap # optional
        #! [midprice]
        return order

    @staticmethod
    def BuyBenchmark(quantity:float):
        order = TraderOrder.Midprice("BUY", quantity, 1)
        order.transmit = True
        return order

    @staticmethod
    def SellBenchmark(quantity:float):
        order = TraderOrder.Midprice("SELL", quantity, 1000000)
        order.transmit = True
        return order

class Trader(TestApp):

    def __init__(self):
        TestApp.__init__(self)
        self.db = None
        self.account = None
        self.portfolioNAV = None
        self.benchmarkSymbol = None
        self.cashAvailableRatio = 1/100
        self.portfolioLoaded = False
        self.ordersLoaded = False
        self.optionContractsAvailable = False
        self.lastCashAdjust = None
        self.lastNakedPutsSale = None
        self.nextTickerId = 1024
    
    def getNextTickerId(self):
        self.nextTickerId += 1
        return self.nextTickerId

    def getDbConnection(self):
        if self.db == None:
            self.db = sqlite3.connect('../db/var/db/data.db')
        return self.db

    def clearApiReqId(self):
        self.getDbConnection()
        c = self.db.cursor()
        c.execute('UPDATE contract SET api_req_id = NULL WHERE api_req_id NOT NULL')
        c.close()
        self.db.commit()

    def clearPortfolioBalances(self, accountName: str):
        self.getDbConnection()
        # clear currencies cash balances
        c = self.db.cursor()
        #t = (self.portfilioID, )
        #c.execute('UPDATE balance SET quantity = 0 WHERE portfolio_id = ?', t)
        #print(c.rowcount)
        t = (accountName, )
        c.execute('UPDATE balance SET quantity = 0 WHERE portfolio_id = (SELECT id from portfolio WHERE account = ?)', t)
        #print(c.rowcount)
        c.close()
        self.db.commit()

    def clearPortfolioPositions(self, accountName: str):
        self.getDbConnection()
        # clear currencies cash balances
        c = self.db.cursor()
        #t = (self.portfilioID, )
        #c.execute('UPDATE balance SET quantity = 0 WHERE portfolio_id = ?', t)
        #print(c.rowcount)
        t = (accountName, )
        c.execute('UPDATE position SET quantity = 0 WHERE portfolio_id = (SELECT id from portfolio WHERE account = ?)', t)
        #print(c.rowcount)
        c.close()
        self.db.commit()

    def clearOpenOrders(self, accountName: str):
        self.getDbConnection()
        # clear currencies cash balances
        c = self.db.cursor()
        #t = (self.portfilioID, )
        #c.execute('UPDATE balance SET quantity = 0 WHERE portfolio_id = ?', t)
        #print(c.rowcount)
        t = (accountName, )
        c.execute('DELETE FROM open_order WHERE account_id = (SELECT id from portfolio WHERE account = ?)', t)
        #print(c.rowcount)
        c.close()
        self.db.commit()

    def findPortfolio(self, account: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, )
        c.execute('SELECT id FROM portfolio WHERE account = ?', t)
        r = c.fetchone()
        portfolio_id = int(r[0])
        c.close()
        self.db.commit()
        return portfolio_id

    @staticmethod
    def normalizeSymbol(symbol):
        return symbol.rstrip('d').replace(' ', '-').replace('.T', '')

    def findOrCreateStockContract(self, contract: Contract):
        self.getDbConnection()
        c = self.db.cursor()
        # first search for conId
        t = (contract.conId, )
        c.execute('SELECT id FROM contract WHERE con_id = ?', t)
        r = c.fetchone()
        if not r:
            # search for stock symbol
            t = ('STK', self.normalizeSymbol(contract.symbol), )
            c.execute('SELECT id FROM contract WHERE secType = ? AND symbol = ?', t)
            r = c.fetchone()
            if not r:
                print('stock contract not found:', contract)
                t = ('STK', self.normalizeSymbol(contract.symbol), contract.primaryExchange, contract.currency, contract.conId)
                c.execute('INSERT INTO contract(secType, symbol, exchange, currency, con_id) VALUES (?, ?, ?, ?, ?)', t)
                id = c.lastrowid
                t = (id, )
                c.execute('INSERT INTO stock(id) VALUES (?)', t)
                c.close()
                self.db.commit()
            else:
                id = r[0]
                c.close()
        else:
            id = r[0]
            c.close()
        return id

    def findOrCreateOptionContract(self, contract: Contract):
        self.getDbConnection()
        # look for stock or Underlying stock
        c = self.db.cursor()
        # first search for conId
        t = (contract.conId, )
        c.execute('SELECT id FROM contract WHERE con_id = ?', t)
        r = c.fetchone()
        if not r:
            # search for stock symbol
            t = ('STK', self.normalizeSymbol(contract.symbol), )
            c.execute('SELECT id, name FROM contract WHERE secType = ? AND symbol = ?', t)
            r = c.fetchone()
            if not r:
                # need to create stock
                print('Underlying stock contract not found:', contract)
                t = ('STK', self.normalizeSymbol(contract.symbol), contract.currency, contract.primaryExchange, )
                c.execute('INSERT INTO contract(secType, symbol, currency, exchange) VALUES (?, ?, ?, ?)', t)
                stockid = c.lastrowid
                t = (stockid, )
                c.execute('INSERT INTO stock(id) VALUES (?)', t)
                name = contract.localSymbol
            else:
                stockid = r[0]
                name = r[1]
            # search for option
            t = (stockid, contract.right, contract.strike, datetime.datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').date())
            c.execute('SELECT id FROM option WHERE stock_id = ? AND call_or_put = ? AND strike = ? AND last_trade_date = ?', t)
            r = c.fetchone()
            if not r:
                t = (contract.secType, '{} {} {:.1f} {}'.format(self.normalizeSymbol(contract.symbol), datetime.datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').strftime('%d%b%y').upper(), contract.strike, contract.right), contract.currency, contract.conId, name, )
                c.execute('INSERT INTO contract(secType, symbol, currency, con_id, name) VALUES (?, ?, ?, ?, ?)', t)
                id = c.lastrowid
                t = (id, stockid, contract.right, contract.strike, datetime.datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d').date(), contract.multiplier)
                c.execute('INSERT INTO option(id, stock_id, call_or_put, strike, last_trade_date, multiplier) VALUES (?, ?, ?, ?, ?, ?)', t)
                c.close()
                self.db.commit()
            else:
                id = r[0]
                c.close()
        else:
            id = r[0]
            c.close()
        return id

    def findOrCreateContract(self, contract: Contract):
        if contract.secType == 'STK':
            id = self.findOrCreateStockContract(contract)
        elif contract.secType == 'OPT':
            id = self.findOrCreateOptionContract(contract)
        else:
            print('unknown contract.secType: ', contract)
            id = None
        return id

    def getContractConId(self, stock: str):
        self.getDbConnection()
        # look for stock or Underlying stock
        c = self.db.cursor()
        # first search for conId
        t = (self.normalizeSymbol(stock), )
        c.execute('SELECT con_id FROM contract WHERE contract.symbol = ?', t)
        r = c.fetchone()
        getContractConId = r[0]
        c.close()
        return getContractConId

    def createOrUpdatePosition(self, cid: int, position: float, averageCost: float, accountName: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (accountName, )
        c.execute('SELECT id, base_currency FROM portfolio WHERE account = ?', t)
        r = c.fetchone()
        pid = r[0]
        base = r[1]
        if position == 0:
            t = (pid, cid)
            c.execute('DELETE FROM position WHERE portfolio_id = ? AND contract_id = ?', t)
        else:
            t = (averageCost * position, position, pid, cid)
            c.execute('UPDATE position SET cost = ?, quantity = ? WHERE portfolio_id = ? AND contract_id = ?', t)
            #print(c.rowcount)
            if (c.rowcount == 0):
                c.execute("INSERT INTO position(cost, quantity, portfolio_id, contract_id, open_date) VALUES (?, ?, ?, ?, date('now'))", t)
        c.close()
        self.db.commit()

    """
    Symbols related functions
    """

    def getSymbolPriceInBase(self, account: str, symbol: str):
        self.getDbConnection()
        c = self.db.cursor()

        # get base currency
        t = (account, )
        c.execute('SELECT portfolio.base_currency FROM portfolio WHERE portfolio.account = ?', t)
        r = c.fetchone()
        base_currency = r[0]
        t = (symbol, base_currency, )
        c.execute(
            'SELECT (contract.price / currency.rate) ' \
            'FROM contract, currency ' \
            'WHERE contract.symbol = ?' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = ?',
            t)
        r = c.fetchone()
        benchmarkPrice = float(r[0])
        c.close()
        print('getSymbolPriceInBase:', benchmarkPrice)
        return benchmarkPrice

    def getContractBuyableQuantity(self, account: str, symbol: str):
        print('getContractBuyableQuantity.', 'account:', account, 'symbol', symbol)
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, symbol, )
        c.execute(
            'SELECT (balance.quantity / contract.price) ' \
            'FROM portfolio, balance, contract ' \
            'WHERE portfolio.account = ? AND contract.symbol = ? ' \
            ' AND balance.portfolio_id = portfolio.id AND balance.currency = contract.currency ',
            t)
        r = c.fetchone()
        getContractBuyableQuantity = float(r[0])
        print('getContractBuyableQuantity:', getContractBuyableQuantity)
        c.close()
        return getContractBuyableQuantity

    def getSymbolPrice(self, symbol: str):
        self.getDbConnection()
        c = self.db.cursor()

        t = (symbol, )
        c.execute(
            'SELECT contract.price ' \
            'FROM contract ' \
            'WHERE contract.symbol = ?',
            t)
        r = c.fetchone()
        getSymbolPrice = float(r[0])
        c.close()
        print('getSymbolPrice:', getSymbolPrice)
        return getSymbolPrice

    def getSymbolCurrency(self, symbol: str):
        self.getDbConnection()
        c = self.db.cursor()

        t = (symbol, )
        c.execute(
            'SELECT contract.currency ' \
            'FROM contract ' \
            'WHERE contract.symbol = ?',
            t)
        r = c.fetchone()
        getSymbolCurrency = r[0]
        c.close()
        print('getSymbolCurrency:', getSymbolCurrency)
        return getSymbolCurrency

    """
    Get Cash positions information
    """

    def getTotalCashAmount(self, account: str):
        self.getDbConnection()
        c = self.db.cursor()
        # how much cash do we have?
        t = (account, )
        c.execute(
            'SELECT SUM(balance.quantity / currency.rate) ' \
            'FROM portfolio, balance, currency ' \
            'WHERE balance.portfolio_id = portfolio.id AND portfolio.account = ?' \
            ' AND balance.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency',
            t)
        r = c.fetchone()
        total_cash = float(r[0])
        print('total cash:', total_cash)
        c.close()
        return total_cash

    def getCurrencyBalance(self, account: str, currency: str):
        self.getDbConnection()
        c = self.db.cursor()

        t = (account, currency, )
        c.execute(
            'SELECT balance.quantity ' \
            'FROM portfolio, balance ' \
            'WHERE portfolio.account = ? ' \
            ' AND balance.portfolio_id = portfolio.id ' \
            ' AND balance.currency = ?',
            t)
        r = c.fetchone()
        getCurrencyBalance = float(r[0])
        c.close()
        print('getCurrencyBalance:', getCurrencyBalance)
        return getCurrencyBalance

    def getBaseToCurrencyRate(self, account: str, currency: str):
        self.getDbConnection()
        c = self.db.cursor()

        t = (account, currency, )
        c.execute(
            'SELECT currency.rate ' \
            'FROM portfolio, currency ' \
            'WHERE portfolio.account = ? ' \
            ' AND currency.base = portfolio.base_currency ' \
            ' AND currency.currency = ?',
            t)
        r = c.fetchone()
        getBaseToCurrencyRatio = float(r[0])
        c.close()
        print('getBaseToCurrencyRatio:', getBaseToCurrencyRatio)
        return getBaseToCurrencyRatio

    """
    Get stock positions information
    """

    def getPortfolioStocksValue(self, account: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, 'STK', )
        c.execute(
            'SELECT SUM(position.quantity * contract.price / currency.rate) ' \
            'FROM position, portfolio, contract, currency ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency',
            t)
        r = c.fetchone()
        getPortfolioStocksValue = float(r[0])
        c.close()
        print('getPortfolioStocksValue:', getPortfolioStocksValue)
        return getPortfolioStocksValue

    """
    Get options positions information
    """

    def getPortfolioOptionsValue(self, account: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, 'OPT', )
        c.execute(
            'SELECT SUM(position.quantity * contract.price * option.multiplier / currency.rate) ' \
            'FROM position, portfolio, contract, option, currency ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? AND position.contract_id = option.id ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency',
            t)
        r = c.fetchone()
        getPortfolioOptionsValue = float(r[0])
        c.close()
        print('getPortfolioOptionsValue:', getPortfolioOptionsValue)
        return getPortfolioOptionsValue

    # returned value is <= 0 in base currency
    def getNakedPutAmount(self, account: str, stock: str):
        self.getDbConnection()
        c = self.db.cursor()
        # how much do we need to cover ALL short puts?
        t = (account, 'OPT', 'P', stock, )
        c.execute(
            'SELECT SUM(position.quantity * option.strike * option.multiplier / currency.rate) ' \
            'FROM position, portfolio, contract, option, currency, contract stock ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.quantity < 0 AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? AND position.contract_id = option.id AND option.call_or_put = ? ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency' \
            ' AND option.stock_id = stock.id AND stock.symbol = ?',
            t)
        r = c.fetchone()
        getNakedPutAmount = float(r[0])
        c.close()
        print('naked put amount:', getNakedPutAmount, 'for symbol:', stock)
        return getNakedPutAmount

    def getTotalNakedPutAmount(self, account: str):
        self.getDbConnection()
        c = self.db.cursor()
        # how much do we need to cover ALL short puts?
        t = (account, 'OPT', 'P', )
        c.execute(
            'SELECT SUM(position.quantity * option.strike * option.multiplier / currency.rate) ' \
            'FROM position, portfolio, contract, option, currency ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.quantity < 0 AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? AND position.contract_id = option.id AND option.call_or_put = ? ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency',
            t)
        r = c.fetchone()
        naked_puts_engaged = float(r[0])
        c.close()
        print('total naked put:', naked_puts_engaged)
        return naked_puts_engaged

    def getItmNakedPutAmount(self, account: str):
        self.getDbConnection()
        c = self.db.cursor()
        # how much do we need to cover ITM short puts?
        t = (account, 'OPT', 'P', )
        c.execute(
            'SELECT SUM(position.quantity * option.strike * option.multiplier / currency.rate) ' \
            'FROM position, portfolio, contract, option, currency, contract stock ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.quantity < 0 ' \
            ' AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? ' \
            ' AND position.contract_id = option.id ' \
            ' AND option.call_or_put = ? ' \
            ' AND option.stock_id = stock.id ' \
            ' AND stock.price < option.strike ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency',
            t)
        r = c.fetchone()
        naked_puts_amount = float(r[0])
        c.close()
        print('ITM naked put:', naked_puts_amount)
        return naked_puts_amount

    def getShortCallPositionQuantity(self, account: str, contract: Contract):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, contract.conId, 'OPT', 'C', )
        c.execute(
            'SELECT SUM(position.quantity * option.multiplier), MIN(option.multiplier) ' \
            'FROM position, portfolio, contract, option ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.contract_id = contract.id AND contract.con_id = ? AND contract.secType = ? ' \
            ' AND position.contract_id = option.id AND option.call_or_put = ?',
            t)
        r = c.fetchone()
        if r[0]:
            position = int(r[0])
            multiplier = int(r[1])
        else:
            position = 0
            multiplier = 100
        print('call positions adjust:', position)
        c.close()
        return position

    """
    Get order book information
    """

    def cancelStockOrderBook(self, account: str, symbol: str, action: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, symbol, 'STK', action, 'Submitted', 'PreSubmitted', )
        c.execute(
            'SELECT open_order.order_id '\
            'FROM open_order, portfolio, contract ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = contract.id AND contract.symbol = ? AND contract.secType = ? ' \
            ' AND open_order.action_type = ?' \
            ' AND open_order.status IN (?, ?)',
            t)
        for r in c:
            print('canceling order:', action, r[0])
            self.cancelOrder(int(r[0]))
        c.close()

    def getStockQuantityOnOrderBook(self, account: str, symbol: str, action: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, symbol, 'STK', action, 'Submitted', 'PreSubmitted', )
        c.execute(
            'SELECT SUM(remaining_qty) '\
            'FROM open_order, portfolio, contract ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = contract.id AND contract.symbol = ? AND contract.secType = ? ' \
            ' AND open_order.action_type = ?' \
            ' AND open_order.status IN (?, ?)',
            t)
        r = c.fetchone()
        if r[0]:
            if action == 'BUY':
                order_book_quantity = float(r[0])
            elif action == 'SELL':
                order_book_quantity = -float(r[0])
        else:
            order_book_quantity = 0
        print('order_book_quantity:', order_book_quantity)
        c.close()
        return order_book_quantity

    def getOptionsQuantityOnOrderBook(self, account: str, stock: str, putOrCall, action: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, putOrCall, stock, action, 'Submitted', 'PreSubmitted', )
        c.execute(
            'SELECT SUM(open_order.remaining_qty * option.multiplier) '\
            'FROM open_order, portfolio, option, contract ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = option.id AND option.call_or_put = ? AND option.stock_id = contract.id AND contract.symbol = ? ' \
            ' AND open_order.action_type = ?' \
            ' AND open_order.status IN (?, ?)',
            t)
        r = c.fetchone()
        if r[0]:
            if action == 'BUY':
                getOptionsQuantityOnOrderBook = float(r[0])
            elif action == 'SELL':
                getOptionsQuantityOnOrderBook = -float(r[0])
        else:
            getOptionsQuantityOnOrderBook = 0
        c.close()
        print('order_book_quantity:', getOptionsQuantityOnOrderBook)
        return getOptionsQuantityOnOrderBook

    """
    Trading functions
    """

    @printWhenExecuting
    def sellNakedPuts(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        seconds = time.time()
        # run every 1 minutes
        if (seconds < (self.lastNakedPutsSale + (1 * 60))):
            return
        self.lastNakedPutsSale = seconds

        # how much cash do we have?
        portfolio_nav = self.getTotalCashAmount(self.account)
        portfolio_nav += self.getPortfolioStocksValue(self.account)
        portfolio_nav += self.getPortfolioOptionsValue(self.account)
        print('portfolio_nav:', portfolio_nav)

        # how much did we engage with ALL short puts?
        naked_puts_engaged = self.getTotalNakedPutAmount(self.account)

        puttable_amount = portfolio_nav * self.nakedPutsRatio + naked_puts_engaged
        print('puttable_amount:', puttable_amount)

    def adjustCash(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        seconds = time.time()
        # run every 10 minutes
        if (seconds < (self.lastCashAdjust + (10 * 60))):
            return
        self.lastCashAdjust = seconds

        # how much cash do we have?
        total_cash = self.getTotalCashAmount(self.account)

        # how much do we need to cover ALL short puts?
        naked_puts_engaged = self.getTotalNakedPutAmount(self.account)

        # how much do we need to cover ITM short puts?
        naked_puts_amount = self.getItmNakedPutAmount(self.account,)

        # open orders quantity
        benchmark_on_buy = self.getStockQuantityOnOrderBook(self.account, self.benchmarkSymbol, 'BUY')
        benchmark_on_buy -= self.getOptionsQuantityOnOrderBook(self.account, self.benchmarkSymbol, 'P', 'SELL')
        print('benchmark_on_buy', benchmark_on_buy)
        benchmark_on_sale = self.getStockQuantityOnOrderBook(self.account, self.benchmarkSymbol, 'SELL')
        benchmark_on_sale -= self.getOptionsQuantityOnOrderBook(self.account, self.benchmarkSymbol, 'C', 'SELL')
        print('benchmark_on_sale', benchmark_on_sale)

        # benchmark price in base
        benchmarkPriceInBase = self.getSymbolPriceInBase(self.account, self.benchmarkSymbol)
        benchmarkPrice = self.getSymbolPrice(self.benchmarkSymbol)
        benchmarkCurrency = self.getSymbolCurrency(self.benchmarkSymbol)
        benchmarkCurrencyBalance = self.getCurrencyBalance(self.account, benchmarkCurrency)
        benchmarkBaseToCurrencyRatio = self.getBaseToCurrencyRate(self.account, benchmarkCurrency)

        net_cash = total_cash + naked_puts_amount
        print('net_cash:', net_cash)

        if net_cash < 0:
            to_adjust = net_cash / benchmarkPriceInBase
        elif benchmarkCurrencyBalance > (net_cash * benchmarkBaseToCurrencyRatio):
            net_cash += self.getNakedPutAmount(self.account, self.benchmarkSymbol)
            print('adjusted net_cash:', net_cash)
            if net_cash > 0:
                to_adjust = (net_cash * benchmarkBaseToCurrencyRatio) / benchmarkPrice
            else:
                to_adjust = 0
            print('buyable_benchmark:', to_adjust)
        else:
            to_adjust = 0
        print('to_adjust:', to_adjust)
        to_adjust = math.floor(to_adjust)
        print('adjusted to_adjust:', to_adjust)
        if (to_adjust != (benchmark_on_buy + benchmark_on_sale)):
            # adjustement order required
            self.cancelStockOrderBook(self.account, self.benchmarkSymbol, 'BUY')
            self.cancelStockOrderBook(self.account, self.benchmarkSymbol, 'SELL')

            contract = Contract()
            contract.symbol = self.benchmarkSymbol
            contract.secType = "STK"
            contract.currency = benchmarkCurrency
            contract.exchange = "SMART"
            to_adjust += self.getOptionsQuantityOnOrderBook(self.account, self.benchmarkSymbol, 'P', 'SELL')
            if (to_adjust > 0):
                print('toBuy: ', to_adjust)
                self.placeOrder(self.nextOrderId(), contract, TraderOrder.BuyBenchmark(to_adjust))
            elif (to_adjust < 0):
                print('to sell: ', -to_adjust)
                self.placeOrder(self.nextOrderId(), contract, TraderOrder.SellBenchmark(-to_adjust))

    def sellCoveredCallsIfPossible(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        print('sellCoveredCallsIfPossible')
        self.getDbConnection()
        c = self.db.cursor()
        cid = self.findOrCreateContract(contract)
        if (contract.secType == 'STK'):
            portfolio_id = self.findPortfolio(accountName)
            short_call_position = self.getShortCallPositionQuantity(accountName, contract)
            print('short_call_position:', short_call_position)
            stocks_on_sale = self.getStockQuantityOnOrderBook(accountName, contract.symbol, 'SELL')
            print('short_call_position:', stocks_on_sale)
            call_on_order_book = self.getOptionsQuantityOnOrderBook(accountName, contract.symbol, 'C', 'SELL')
            print('call_on_order_book:', call_on_order_book)
        c.close()
        self.db.commit()

    @printWhenExecuting
    def rollOptionIfNeeded(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        print('rollOptionIfNeeded')

    def findWheelSymbolsInfo(self):
        seconds = time.time()
        # process one symbol every 1 minute
        if (seconds < self.nextWheelProcess):
            return
        print('running findWheelSymbolsInfo.')
        print(
            'self.wheelSymbolsProcessing:', self.wheelSymbolsProcessing,
            'self.wheelSymbolsExpirations:', self.wheelSymbolsExpirations,
            'self.wheelSymbolsProcessingStrikes:', self.wheelSymbolsProcessingStrikes,
            'self.wheelSymbolsProcessed', self.wheelSymbolsProcessed)
        # Don't come back too often. As I am afraid that 1 secs is too small regarding granularity, I put 2
        self.nextWheelProcess = seconds + 2
        if self.wheelSymbolsProcessing != None:
            # We are processing all strikes for each expiration for one stock
            # self.wheelSymbolsProcessingSymbol: stock symbol being processed
            # self.wheelSymbolsProcessingStrikes: strikes list associated to process
            # self.wheelSymbolsProcessingExpirations: expirations list associated to process, one at a time
            print(
                'self.wheelSymbolsProcessing:', self.wheelSymbolsProcessing,
                'self.wheelSymbolsExpirations:', len(self.wheelSymbolsExpirations),
                'self.wheelSymbolsProcessingStrikes:', len(self.wheelSymbolsProcessingStrikes))
            today = date.today()
            exp = None
            while len(self.wheelSymbolsExpirations) > 0:
                exp = self.wheelSymbolsExpirations.pop()
                expiration = datetime.date(int(exp[0:4]), int(exp[4:6]), int(exp[6:8]))
                if (expiration - today).days < 50:
                    break
                exp = None
            if exp != None:
                print(
                    'expiration:', expiration)
                price = self.getSymbolPrice(self.wheelSymbolsProcessing)
                for strike in self.wheelSymbolsProcessingStrikes:
                    #print(exp, len(self.wheelSymbolsProcessingStrikes), (price * 0.8), strike, (price * 1.2))
                    if ((price * 0.8) < strike) and (strike < (price * 1.2)):
                        contract = Contract()
                        contract.exchange = 'SMART'
                        contract.secType = 'OPT'
                        contract.symbol = self.wheelSymbolsProcessing
                        contract.strike = strike
                        contract.lastTradeDateOrContractMonth = exp
                        contract.right = 'C'
                        self.reqContractDetails(self.getNextTickerId(), contract)
                        contract.right = 'P'
                        self.reqContractDetails(self.getNextTickerId(), contract)
                        #print('submitted reqContractDetails for 2 options')
                # IB API gives 11 seconds snapshots
                self.nextWheelProcess += 11
            else:
                self.wheelSymbolsProcessed.append(self.wheelSymbolsProcessing)
                self.wheelSymbolsProcessing = None
                self.wheelSymbolsProcessingStrikes = None 
        elif len(self.wheelSymbols) > 0:
            self.wheelSymbolsProcessingSymbol = self.wheelSymbols.pop()
            contract = Contract()
            contract.exchange = 'SMART'
            contract.secType = 'STK'
            contract.conId = self.getContractConId(self.wheelSymbolsProcessingSymbol)
            contract.symbol = self.wheelSymbolsProcessingSymbol
            self.reqContractDetails(self.getNextTickerId(), contract)
            self.reqSecDefOptParams(self.getNextTickerId(), contract.symbol, "", contract.secType, contract.conId)
        else:
            print('findWheelSymbolsInfo. All done!')
            self.wheelSymbols = self.wheelSymbolsProcessed
            self.wheelSymbolsProcessed = []
            # we are done for 60 minutes
            self.nextWheelProcess += (60 * 60)
            self.optionContractsAvailable = True

    """
    IB API wrappers
    """

    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)
        if errorCode == 200:
            if errorString == 'No security definition has been found for the request':
                self.getDbConnection()
                c = self.db.cursor()
                t = (reqId, )
                c.execute('UPDATE contract SET api_req_id = NULL WHERE api_req_id = ?', t)
                c.close()
                self.db.commit()
            else:
                print('error. unexpected error string:', errorString)
    # ! [error] self.XreqId2nErr[reqId] += 1

    @iswrapper
    # ! [tickprice]
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
#        print("TickPrice. TickerId:", reqId, "tickType:", tickType,
 #             "Price:", price, "CanAutoExecute:", attrib.canAutoExecute,
  #            "PastLimit:", attrib.pastLimit, end=' ')
        if price < 0:
            price = None
        self.getDbConnection()
        c = self.db.cursor()
        t = (price, reqId, )
        if tickType == TickTypeEnum.LAST:
            c.execute('UPDATE contract SET price = ? WHERE api_req_id = ?', t)
        elif tickType == TickTypeEnum.BID:
            c.execute('UPDATE contract SET bid = ? WHERE api_req_id = ?', t)
        elif tickType == TickTypeEnum.ASK:
            c.execute('UPDATE contract SET ask = ? WHERE api_req_id = ?', t)
        elif tickType == TickTypeEnum.CLOSE:
            c.execute('UPDATE contract SET previous_close_price = ? WHERE api_req_id = ?', t)
        elif ((tickType == TickTypeEnum.HIGH) or (tickType == TickTypeEnum.LOW)):
#            print('tickPrice. ignored type:', tickType, 'for reqId:', reqId)
            pass
        else:
            print('tickPrice. unexpected type:', tickType, 'for reqId:', reqId)
        c.close()
        self.db.commit()
    # ! [tickprice]

    @iswrapper
    # ! [tickoptioncomputation]
    def tickOptionComputation(self, reqId: TickerId, tickType: TickType, tickAttrib: int,
                              impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):
        super().tickOptionComputation(reqId, tickType, tickAttrib, impliedVol, delta,
                                      optPrice, pvDividend, gamma, vega, theta, undPrice)
#        print("TickOptionComputation. TickerId:", reqId, "TickType:", tickType,
 #             "TickAttrib:", tickAttrib,
  #            "ImpliedVolatility:", impliedVol, "Delta:", delta, "OptionPrice:",
   #           optPrice, "pvDividend:", pvDividend, "Gamma: ", gamma, "Vega:", vega,
    #          "Theta:", theta, "UnderlyingPrice:", undPrice)
        if tickType == TickTypeEnum.MODEL_OPTION:
            self.getDbConnection()
            c = self.db.cursor()
            t = (optPrice, reqId, )
# tickPrice in charge of this            c.execute('UPDATE contract SET price = ? WHERE id = (SELECT id from option WHERE api_req_id = ?)', t)
            t = (impliedVol, delta, pvDividend, gamma, vega, theta, reqId, )
            c.execute(
                'UPDATE option '
                'SET Implied_Volatility = ?, Delta = ?, pv_Dividend = ?, Gamma = ?, Vega = ?, Theta = ? '
                'WHERE option.id = (SELECT contract.id FROM contract where contract.api_req_id = ?)', 
                t)
            c.close()
            self.db.commit()
        elif ((tickType == TickTypeEnum.BID_OPTION_COMPUTATION)
            or (tickType == TickTypeEnum.ASK_OPTION_COMPUTATION)
            or (tickType == TickTypeEnum.LAST_OPTION_COMPUTATION)):
#            print('TickOptionComputation. ignored type:', tickType, 'for reqId:', reqId)
            pass
        else:
            print('TickOptionComputation. unexpected type:', tickType, 'for reqId:', reqId)

    # ! [tickoptioncomputation]

    @iswrapper
    # ! [ticksnapshotend]
    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        self.getDbConnection()
        c = self.db.cursor()
        t = (reqId, )
        c.execute('UPDATE contract SET api_req_id = NULL WHERE api_req_id = ?', t)
        c.execute('SELECT COUNT(*) FROM contract WHERE api_req_id NOTNULL',)
        r = c.fetchone()
        count = int(r[0])
        c.close()
        self.db.commit()
        if (count == 0):
            self.nextWheelProcess = 0
            self.findWheelSymbolsInfo()
    # ! [ticksnapshotend]

    @iswrapper
    # ! [securityDefinitionOptionParameter]
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,
                                          underlyingConId: int, tradingClass: str, multiplier: str,
                                          expirations: SetOfString, strikes: SetOfFloat):
        super().securityDefinitionOptionParameter(reqId, exchange,
                                                underlyingConId, tradingClass, multiplier, expirations, strikes)
        if exchange == "SMART":
            print("SecurityDefinitionOptionParameter.",
                "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier,
                "Expirations:", expirations, "Strikes:", str(strikes))
            self.wheelSymbolsExpirations = expirations
            self.wheelSymbolsProcessingStrikes = strikes
            self.wheelSymbolsProcessing = tradingClass
    # ! [securityDefinitionOptionParameter]

    @iswrapper
    # ! [updateaccountvalue]
    def updateAccountValue(self, key: str, val: str, currency: str,
                           accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        self.getDbConnection()
        if (key == 'CashBalance') and (currency != 'BASE'):
            # update currency cash value
            c = self.db.cursor()
            id = self.findPortfolio(accountName)
            t = (val, id, currency)
            #print(t)
            c.execute('UPDATE balance SET quantity = ? WHERE portfolio_id = ? AND currency = ?', t)
            #print(c.rowcount)
            if (c.rowcount == 0) and (val != 0):
                c.execute('INSERT INTO balance(quantity, portfolio_id, currency) VALUES (?, ?, ?)', t)
            c.close()
            self.db.commit()
        elif (key == 'ExchangeRate') and (currency != 'BASE'):
            # update exchange rate
            c = self.db.cursor()
            t = (accountName, )
            c.execute('SELECT id, base_currency FROM portfolio WHERE account = ?', t)
            r = c.fetchone()
            #print(r)
            id = r[0]
            base = r[1]
            t = (val, base, currency)
            c.execute('UPDATE currency SET rate = 1.0/? WHERE base = ? AND currency = ?', t)
            #print(c.rowcount)
            if (c.rowcount == 0):
                c.execute('INSERT INTO currency(rate, base, currency) VALUES (1.0/?, ?, ?)', t)
            t = (val, currency, base)
            c.execute('UPDATE currency SET rate = ? WHERE base = ? AND currency = ?', t)
            #print(c.rowcount)
            if (c.rowcount == 0):
                c.execute('INSERT INTO currency(rate, base, currency) VALUES (?, ?, ?)', t)
            c.close()
            self.db.commit()
        elif (key == 'NetLiquidationByCurrency') and (currency == 'BASE'):
            self.portfolioNAV = float(val)
    # ! [updateaccountvalue]

    @iswrapper
    # ! [updateportfolio]
    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue,
                                averageCost, unrealizedPNL, realizedPNL, accountName)
        if (contract.secType != 'UNK') and (contract.secType != 'CASH'):
            # very surprising, contract.strike is correct inside position callback, but is in pence in updatePortfolio callback
            if contract.currency == 'GBP':
                contract.strike /= 100.0
            cid = self.findOrCreateContract(contract)
            self.createOrUpdatePosition(cid, position, averageCost, accountName)
            self.getDbConnection()
            c = self.db.cursor()
            t = (marketPrice, cid)
            c.execute('UPDATE contract SET price = ? WHERE id = ?', t)
            c.close()
            self.db.commit()
#            self.sellCoveredCallsIfPossible(contract, position, marketPrice, marketValue,
#                                    averageCost, unrealizedPNL, realizedPNL, accountName)
    # ! [updateportfolio]

    @iswrapper
    # ! [accountdownloadend]
    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        self.portfolioLoaded = True
    # ! [accountdownloadend]

    @iswrapper
    # ! [position]
    def position(self, accountName: str, contract: Contract, position: float,
                 averageCost: float):
        super().position(accountName, contract, position, averageCost)
        cid = self.findOrCreateContract(contract)
        self.createOrUpdatePosition(cid, position, averageCost, accountName)
        unused() # dans quel cas ?
    # ! [position]

    @iswrapper
    # ! [openorder]
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        self.getDbConnection()
        c = self.db.cursor()
        # Update OpenOrder table
        t = (orderId, )
        c.execute('SELECT id, contract_id FROM open_order WHERE order_id = ?', t)
        r = c.fetchone()
        if not r:
            portfolio_id = self.findPortfolio(order.account)
            contract_id = self.findOrCreateContract(contract)
            if contract_id:
                t = (portfolio_id, contract_id, order.permId, order.clientId, orderId, order.action, order.totalQuantity, order.cashQty, order.lmtPrice, order.auxPrice, orderState.status, order.totalQuantity, )
                c.execute(
                    'INSERT INTO open_order(account_id, contract_id, perm_id, client_id, order_id, action_type, total_qty, cash_qty, lmt_price, aux_price, status, remaining_qty) ' \
                    'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    t)
        c.close()
        self.db.commit()
    # ! [openorder]

    @iswrapper
    # ! [openorderend]
    def openOrderEnd(self):
        super().openOrderEnd()
        self.ordersLoaded = True
    # ! [openorderend]

    @iswrapper
    # ! [orderstatus]
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        # Update OpenOrder
        self.getDbConnection()
        c = self.db.cursor()
        t = (status, remaining, orderId, )
        c.execute('UPDATE open_order SET status = ?, remaining_qty = ? WHERE order_id = ?', t)
        c.close()
        self.db.commit()
    # ! [orderstatus]

    @iswrapper
    # ! [contractdetails]
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.findOrCreateContract(contractDetails.contract)
        nextReqId = self.getNextTickerId()
        self.getDbConnection()
        c = self.db.cursor()
        if contractDetails.contract.secType == 'STK':
            t = (contractDetails.industry, contractDetails.category, contractDetails.subcategory, contractDetails.contract.conId, )
            c.execute('UPDATE stock SET industry = ?, category = ?, subcategory = ? WHERE id = (SELECT id FROM contract WHERE contract.con_id = ?)', t)
            t = (nextReqId, contractDetails.contract.conId, )
            c.execute('UPDATE contract SET api_req_id = ? WHERE contract.con_id = ?', t)
        elif contractDetails.contract.secType == 'OPT':
            t = (nextReqId, contractDetails.contract.conId, )
            c.execute(
                'UPDATE contract '
                'SET ask = NULL, price = NULL, bid = NULL, previous_close_price = NULL, api_req_id = ? '
                'WHERE contract.con_id = ?', t)
        c.close()
        self.db.commit()
        print('contractDetails. reqId:', reqId, 'contract:', contractDetails.contract, 'saved reqMktData req id:', nextReqId)
        self.reqMktData(nextReqId, contractDetails.contract, "104,106", True, False, [])
    # ! [contractdetails]

    @iswrapper
    # ! [updateaccounttime]
    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        self.findWheelSymbolsInfo()
        self.adjustCash()
    # ! [updateaccounttime]

    @iswrapper
    # ! [managedaccounts]
    def managedAccounts(self, accountsList: str):
        if self.account:
            super().managedAccounts(accountsList)
        else:
            # first time
            super().managedAccounts(accountsList)
            self.benchmarkSymbol = 'VT'
            self.nakedPutsRatio = 0.5
            self.wheelSymbols = [ 'PFSI', 'CCL', 'MGM' ]
            self.wheelSymbolsProcessed = []
            self.wheelSymbolsProcessing = None
            self.wheelSymbolsProcessingStrikes = None
            self.wheelSymbolsExpirations = None

            self.lastCashAdjust = 0
            self.lastNakedPutsSale = 0
            self.nextWheelProcess = 0

            self.clearApiReqId()
            self.clearPortfolioBalances(self.account)
            self.clearPortfolioPositions(self.account)
            self.clearOpenOrders(self.account)

            self.reqMarketDataType(MarketDataTypeEnum.FROZEN)
            # start account updates
            self.reqAccountUpdates(True, self.account)
            # Requesting the next valid id. The parameter is always ignored.
            self.reqIds(-1)
            self.reqOpenOrders()
    # ! [managedaccounts]

    """
    Main Program
    """

    @printWhenExecuting
    def start(self):
        if self.started:
            return
        self.started = True
        # first retrieve account info
        self.reqManagedAccts()

    @printWhenExecuting
    def stop(self):
        super().stop()
        # ! [cancelaaccountupdates]
        self.reqAccountUpdates(False, self.account)
        # ! [cancelaaccountupdates]
        self.clearApiReqId()
        if (self.db):
            self.db.close()

def main():
    SetupLogger()
    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.ERROR)

    cmdLineParser = argparse.ArgumentParser("api tests")
    # cmdLineParser.add_option("-c", action="store_True", dest="use_cache", default = False, help = "use the cache")
    # cmdLineParser.add_option("-f", action="store", type="string", dest="file", default="", help="the input file")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int,
                               dest="port", default=7497, help="The TCP port to use")
    # RYL
    cmdLineParser.add_argument("--host", action="store",
                               dest="host", default="localhost", help="The IB TWS hostname to use")
    args = cmdLineParser.parse_args()
    print("Using args", args)
    logging.debug("Using args %s", args)
    # print(args)

    # enable logging when member vars are assigned
    from ibapi import utils
    Order.__setattr__ = utils.setattr_log
    Contract.__setattr__ = utils.setattr_log
    DeltaNeutralContract.__setattr__ = utils.setattr_log
    TagValue.__setattr__ = utils.setattr_log
    TimeCondition.__setattr__ = utils.setattr_log
    ExecutionCondition.__setattr__ = utils.setattr_log
    MarginCondition.__setattr__ = utils.setattr_log
    PriceCondition.__setattr__ = utils.setattr_log
    PercentChangeCondition.__setattr__ = utils.setattr_log
    VolumeCondition.__setattr__ = utils.setattr_log

    try:
        app = Trader()
        # ! [connect]
        # RYL
        app.connect(args.host, args.port, clientId=0)
        # ! [connect]
        print("serverVersion:%s connectionTime:%s" % (app.serverVersion(),
                                                      app.twsConnectionTime()))

        # ! [clientrun]
        app.run()
        # ! [clientrun]
    except:
        raise
    finally:
        app.dumpTestCoverageSituation()
        app.dumpReqAnsErrSituation()

if __name__ == "__main__":
    main()