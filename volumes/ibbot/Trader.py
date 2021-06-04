"""
TBD
"""

from ibapi.scanner import NO_ROW_NUMBER_SPECIFIED
import time
from datetime import date
import datetime
import sqlite3
import math
from functools import cmp_to_key

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

from TraderOrder import TraderOrder

def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fn2

def printinstance(inst:Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))

# this is here for documentation generation
"""
#! [ereader]
        # You don't need to run this in your code!
        self.reader = reader.EReader(self.conn, self.msg_queue)
        self.reader.start()   # start thread
#! [ereader]
"""

# ! [socket_init]
class Trader(wrapper.EWrapper, EClient):

    def __init__(self):
        wrapper.EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.db = None
        self.account = None
        self.portfolioNAV = None
        self.benchmarkSymbol = None
        self.portfolioLoaded = False
        self.ordersLoaded = False
        self.optionContractsAvailable = False
        # for testing
        self.optionContractsAvailable = True
        self.lastCashAdjust = None
        self.lastNakedPutsSale = None
        self.nextTickerId = 1024
    
    def getNextTickerId(self):
        self.nextTickerId += 1
        return self.nextTickerId

    def nextOrderId(self):
            oid = self.nextValidOrderId
            self.nextValidOrderId += 1
            return oid

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

    def clearRequestId(self, reqId: int):
        self.getDbConnection()
        c = self.db.cursor()
        t = (reqId, )
        c.execute('UPDATE contract SET api_req_id = NULL WHERE api_req_id = ?', t)
        c.execute('SELECT COUNT(*) FROM contract WHERE api_req_id NOTNULL',)
        r = c.fetchone()
        count = int(r[0])
        c.close()
        self.db.commit()
        return count

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
    Get positions information (stock)
    """

    def getPortfolioStocksValue(self, account: str, stock: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, 'STK', )
        sql = 'SELECT SUM(position.quantity * contract.price / currency.rate) ' \
            'FROM position, portfolio, contract, currency ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency'
        if stock:
            t += (stock, )
            sql += ' AND contract.symbol = ?'
        c.execute(sql, t)
        r = c.fetchone()
        if r[0]:
            getPortfolioStocksValue = float(r[0])
        else:
            getPortfolioStocksValue = 0
        c.close()
        print('getPortfolioStocksValue:', getPortfolioStocksValue)
        return getPortfolioStocksValue

    """
    Get positions information (options)
    """

    def getShortCallPositionQuantity(self, account: str, stock: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, stock, 'C', )
        c.execute(
            'SELECT SUM(position.quantity * option.multiplier), MIN(option.multiplier) ' \
            'FROM position, portfolio, contract stock_contract, option ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.quantity < 0 ' \
            ' AND option.id  = position.contract_id ' \
            ' AND stock_contract.id = option.stock_id AND stock_contract.symbol = ?' \
            ' AND option.call_or_put = ?',
            t)
        r = c.fetchone()
        if r[0]:
            getShortCallPositionQuantity = int(r[0])
            multiplier = int(r[1])
        else:
            getShortCallPositionQuantity = 0
            multiplier = 100
        c.close()
        print('getShortCallPositionQuantity:', getShortCallPositionQuantity)
        return getShortCallPositionQuantity

    def getPortfolioOptionsValue(self, account: str, stock: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, 'OPT', )
        sql = 'SELECT SUM(position.quantity * contract.price * option.multiplier / currency.rate) ' \
            'FROM position, portfolio, contract, option, currency, contract stock_contract ' \
            'WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? ' \
            ' AND position.contract_id = contract.id ' \
            ' AND contract.secType = ? AND position.contract_id = option.id ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency' \
            ' AND stock_contract.id = option.stock_id'
        if stock != None:
            t += (stock, )
            sql += ' AND stock_contract.symbol = ?'
        c.execute(sql, t)
        r = c.fetchone()
        if r:
            getPortfolioOptionsValue = float(r[0])
        else:
            getPortfolioOptionsValue = 0
        c.close()
        print('getPortfolioOptionsValue(', account, stock, ') =>', getPortfolioOptionsValue)
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
        if r[0] != None:
            getNakedPutAmount = float(r[0])
        else:
            getNakedPutAmount = 0
        c.close()
        print('getNakedPutAmount:', getNakedPutAmount)
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

    """
    order book operations
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

    """
    Get order book information (stocks)
    """

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
                getStockQuantityOnOrderBook = float(r[0])
            elif action == 'SELL':
                getStockQuantityOnOrderBook = -float(r[0])
        else:
            getStockQuantityOnOrderBook = 0
        c.close()
        print('getStockQuantityOnOrderBook:', getStockQuantityOnOrderBook)
        return getStockQuantityOnOrderBook

    """
    Get order book information (options)
    """

    def getOptionsQuantityOnOrderBook(self, account: str, stock: str, putOrCall: str, action: str):
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
        print('getOptionsQuantityOnOrderBook:', getOptionsQuantityOnOrderBook)
        return getOptionsQuantityOnOrderBook

    def getOptionsAmountOnOrderBook(self, account: str, stock: str, putOrCall: str, action: str):
        self.getDbConnection()
        c = self.db.cursor()
        t = (account, putOrCall, action, 'Submitted', 'PreSubmitted')
        sql = 'SELECT SUM(open_order.remaining_qty * option.multiplier * option.strike / currency.rate) '\
            'FROM open_order, portfolio, option, contract, currency ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = option.id AND option.call_or_put = ? AND option.stock_id = contract.id ' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = portfolio.base_currency' \
            ' AND open_order.action_type = ?' \
            ' AND open_order.status IN (?, ?)'
        if stock:
            t += (stock, )
            sql = sql + ' AND contract.symbol = ? '
        c.execute(sql, t)
        r = c.fetchone()
        if r[0]:
            if action == 'BUY':
                getOptionsAmountOnOrderBook = float(r[0])
            elif action == 'SELL':
                getOptionsAmountOnOrderBook = -float(r[0])
        else:
            getOptionsAmountOnOrderBook = 0
        c.close()
        print('getOptionsAmountOnOrderBook(', account, stock, putOrCall, action, ') =>', getOptionsAmountOnOrderBook)
        return getOptionsAmountOnOrderBook

    """
    Trading functions
    """

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

    def sellNakedPuts(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded) or (not self.optionContractsAvailable):
            return
        seconds = time.time()
        # run every 10 minutes
        if (seconds < (self.lastNakedPutsSale + (10 * 60))):
            return
        self.lastNakedPutsSale = seconds

        # how much cash do we have?
        portfolio_nav = self.getTotalCashAmount(self.account)
        portfolio_nav += self.getPortfolioStocksValue(self.account, None)
        portfolio_nav += self.getPortfolioOptionsValue(self.account, None)
        print('portfolio_nav:', portfolio_nav)

        # how much did we engage with ALL short puts?
        naked_puts_engaged = self.getTotalNakedPutAmount(self.account)

        # how much short put we can sell, regarding global portfolio size
        puttable_amount = portfolio_nav * self.nakedPutsRatio + naked_puts_engaged
        print('puttable_amount:', puttable_amount)

        puttable_amount += self.getOptionsAmountOnOrderBook(self.account, None, 'P', 'SELL')
        print('puttable_amount:', puttable_amount)

        # select option contracts which match:
        #   implied volatility > historical volatility
        #   Put
        #   OTM
        #   strike < how much we can engage
        #   at least 80% success (delta < -0.2)
        #   premium at least $0.25
        t = ('P', puttable_amount/100, -0.2, 0.25, )
        self.getDbConnection()
        c = self.db.cursor()
        c.execute(
            'SELECT contract.con_id, '
            '  stock_contract.symbol, option.last_trade_date, option.strike, option.call_or_put, contract.symbol, '
            '  julianday(option.last_trade_date) - julianday(\'now\') + 1, contract.bid / option.strike / (julianday(option.last_trade_date) - julianday(\'now\') + 1) * 360, '
            '  contract.bid, contract.ask, stock_contract.price, option.implied_volatility, stock.historical_volatility, option.delta '
            ' FROM contract, option, stock, contract stock_contract'
            ' WHERE option.id = contract.id'
            '  AND stock.id = option.stock_id'
            '  AND stock_contract.id = stock.id'
            '  AND option.call_or_put = ? '
            '  AND option.implied_volatility > stock.historical_volatility'
            '  AND option.strike < stock_contract.price'
            '  AND option.strike < ?'
            '  AND option.delta >= ?'
            '  AND contract.bid >= ?',
            t)
        opt = c.fetchall()
        c.close()
        print(len(opt), 'contracts')
        # sort by annualized yield descending
        for rec in sorted(opt, key=cmp_to_key(lambda item1, item2: item2[7] - item1[7])):
            # verify that this symbol is in our wheel
#                print('symbol:', contract[1], 'of list:',  self.wheelSymbols)
#                print('at pos:', self.wheelSymbols.index(contract[1]))
            if rec[1] in self.wheelSymbols:
                engaged = self.getPortfolioStocksValue(self.account, rec[1]) - self.getNakedPutAmount(self.account, rec[1]) - self.getOptionsAmountOnOrderBook(self.account, rec[1], 'P', 'SELL')
                print('now:', engaged, engaged / portfolio_nav, 'engaged for stock', rec[1])
                engaged += rec[3] * 100 / self.getBaseToCurrencyRate(self.account, 'USD')
                print(engaged, engaged / portfolio_nav, 'engaged with this Put')
                # verify that we don't already have naked put position
                if engaged <= (portfolio_nav * self.wheelSymbols[rec[1]]):
                    print(self.wheelSymbols[rec[1]], engaged, (portfolio_nav * self.wheelSymbols[rec[1]]), rec)
                    contract = Contract()
                    contract.secType = "OPT"
#                        contract.currency = benchmarkCurrency
                    contract.exchange = "SMART"
                    contract.symbol = rec[1]
                    contract.lastTradeDateOrContractMonth = rec[2].replace('-', '')
                    contract.strike = rec[3]
                    contract.right = rec[4]
#                        contract.multiplier = "100"
                    price = round((rec[8] + rec[9]) / 2, 2)
                    print(price)
                    self.placeOrder(self.nextOrderId(), contract, TraderOrder.SellNakedPut(price))
                    return
            else:
                print(rec[5], 'ignored')

    def sellCoveredCallsIfPossible(self,
            contract: Contract, position: float,
            marketPrice: float, marketValue: float,
            averageCost: float, unrealizedPNL: float,
            realizedPNL: float, accountName: str):
        if (not self.portfolioLoaded) \
                or (not self.ordersLoaded) \
                or (not self.optionContractsAvailable) \
                or (position < 100) \
                or (contract.currency != 'USD'):
            return
        print('sellCoveredCallsIfPossible.', 'contract:', contract)
        if (contract.secType == 'STK'):
            stocks_on_sale = self.getStockQuantityOnOrderBook(accountName, contract.symbol, 'SELL')
            short_call_position = self.getShortCallPositionQuantity(accountName, contract.symbol)
            call_on_order_book = self.getOptionsQuantityOnOrderBook(accountName, contract.symbol, 'C', 'SELL')
            net_pos = position + stocks_on_sale + short_call_position + call_on_order_book
            print('net_pos:', net_pos)
            if net_pos >= 100:
                # select option contracts which match:
                #   Call
                #   OTM
                #   strike > PRU
                #   at least 85% success (delta < 0.15)
                #   premium at least $0.25
                #   underlying stock is current stock
                t = ('C', averageCost, 0.15, 0.25, contract.conId, )
                self.getDbConnection()
                c = self.db.cursor()
                c.execute(
                    'SELECT contract.con_id, '
                    '  stock_contract.symbol, option.last_trade_date, option.strike, option.call_or_put, contract.symbol, '
                    '  julianday(option.last_trade_date) - julianday(\'now\') + 1, contract.bid / option.strike / (julianday(option.last_trade_date) - julianday(\'now\') + 1) * 360, '
                    '  contract.bid, contract.ask, stock_contract.price, option.implied_volatility, stock.historical_volatility, option.delta '
                    ' FROM contract, option, stock, contract stock_contract'
                    ' WHERE option.id = contract.id'
                    '  AND stock.id = option.stock_id'
                    '  AND stock_contract.id = stock.id'
                    '  AND option.call_or_put = ? '
                    '  AND option.strike > stock_contract.price'
                    '  AND option.strike > ?'
                    '  AND option.delta <= ?'
                    '  AND contract.bid >= ?'
                    '  AND stock_contract.con_id = ?'
                    , t)
                # sort by annualized yield descending
                opt = sorted(c.fetchall(), key=cmp_to_key(lambda item1, item2: item2[7] - item1[7]))
                c.close()
                print(len(opt), 'contracts')
                if len(opt) > 0:
#                    for rec in opt:
#                        print(rec)
                    rec = opt[0]
                    contract = Contract()
                    contract.secType = "OPT"
    #                        contract.currency = benchmarkCurrency
                    contract.exchange = "SMART"
                    contract.symbol = rec[1]
                    contract.lastTradeDateOrContractMonth = rec[2].replace('-', '')
                    contract.strike = rec[3]
                    contract.right = rec[4]
    #                        contract.multiplier = "100"
                    price = round((rec[8] + rec[9]) / 2, 2)
                    print(price)
                    self.placeOrder(self.nextOrderId(), contract, TraderOrder.SellCoveredCall(price, math.floor(net_pos/100)))
        print('sellCoveredCallsIfPossible done.')

    @printWhenExecuting
    def rollOptionIfNeeded(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        print('rollOptionIfNeeded')

    def findWheelSymbolsInfo(self):
#        print('self.wheelSymbols:', self.wheelSymbols)
        seconds = time.time()
        # process one symbol every 1 minute
        if (seconds < self.nextWheelProcess):
            return
        print(
#            'self.wheelSymbolsToProcess:', self.wheelSymbolsToProcess,
            'self.wheelSymbolsProcessing:', self.wheelSymbolsProcessing,
            'self.wheelSymbolsExpirations:', len(self.wheelSymbolsExpirations),
#            self.wheelSymbolsExpirations,
            'self.wheelSymbolsProcessingStrikes:', len(self.wheelSymbolsProcessingStrikes),
#            self.wheelSymbolsProcessingStrikes,
#            'self.wheelSymbolsProcessed', self.wheelSymbolsProcessed
            )
        # Don't come back too often. As I am afraid that 1 secs is too small regarding granularity, I put 2
        self.nextWheelProcess = seconds + 2
        if self.wheelSymbolsProcessing != None:
            # We are processing all strikes for each expiration for one stock
            # self.wheelSymbolsProcessingSymbol: stock symbol being processed
            # self.wheelSymbolsProcessingStrikes: strikes list associated to process
            # self.wheelSymbolsProcessingExpirations: expirations list associated to process, one at a time
            today = date.today()
            exp = None
            while len(self.wheelSymbolsExpirations) > 0:
#                print('expirations:', self.wheelSymbolsExpirations)
                exp = self.wheelSymbolsExpirations.pop(0)
                expiration = datetime.date(int(exp[0:4]), int(exp[4:6]), int(exp[6:8]))
#                print('expiration selected:', exp, expiration, 'left:', self.wheelSymbolsExpirations)
                if (expiration - today).days < 50:
                    break
                exp = None
            if exp != None:
                price = self.getSymbolPrice(self.wheelSymbolsProcessing)
                num_requests = 0
                # atm: first strike index above price
                for atm in range(len(self.wheelSymbolsProcessingStrikes)):
                    if self.wheelSymbolsProcessingStrikes[atm] >= price:
                        break
#                print('atm:', atm)
                contract = Contract()
                contract.exchange = 'SMART'
                contract.secType = 'OPT'
                contract.lastTradeDateOrContractMonth = exp
                contract.symbol = self.wheelSymbolsProcessing
                # process at most xx strikes in each direction
                steps = math.ceil(len(self.wheelSymbolsProcessingStrikes) / 100)
#                print('steps:', steps)
                # should be 49 but as reqContractDetails callback will submit new request we will 
                # potentially overcome de 100 simultaneous requests limit
                # I lower substencially as TWS is running it's own querie that counts for the same limit
                for i in range(0, 35):
                    if (atm-i-1) >= 0:
                        contract.strike = self.wheelSymbolsProcessingStrikes[atm-i-1]
                        contract.right = 'P'
                        self.reqContractDetails(self.getNextTickerId(), contract)
                        num_requests += 1
                    if (atm+i) < len(self.wheelSymbolsProcessingStrikes):
                        contract.strike = self.wheelSymbolsProcessingStrikes[atm+i]
                        contract.right = 'C'
                        self.reqContractDetails(self.getNextTickerId(), contract)
                        num_requests += 1
                    # and no more than 15% distance
# to debug may be out of bounds                   if (self.wheelSymbolsProcessingStrikes[atm-i] < (price*0.85)) and (self.wheelSymbolsProcessingStrikes[atm+i] > (price*1.15)):
#                        break
                # IB API gives 11 seconds snapshots, and add 2 for safety
                self.nextWheelProcess = seconds + 11 + 2
                print(num_requests, 'reqContractDetails submitted')
            else:
                print('done with symbol:', self.wheelSymbolsProcessing)
                # we are finished with this symbol
                self.wheelSymbolsProcessed.append(self.wheelSymbolsProcessing)
                self.wheelSymbolsProcessing = None
                self.wheelSymbolsProcessingStrikes = []
                # immediately start with next one
                self.nextWheelProcess = 0
                self.findWheelSymbolsInfo()
        elif len(self.wheelSymbolsToProcess) > 0:
            self.wheelSymbolsProcessingSymbol = self.wheelSymbolsToProcess.pop()
            contract = Contract()
            contract.exchange = 'SMART'
            contract.secType = 'STK'
            contract.conId = self.getContractConId(self.wheelSymbolsProcessingSymbol)
            contract.symbol = self.wheelSymbolsProcessingSymbol
#            print(contract)
            self.reqContractDetails(self.getNextTickerId(), contract)
            self.reqSecDefOptParams(self.getNextTickerId(), contract.symbol, "", contract.secType, contract.conId)
        else:
            print('findWheelSymbolsInfo. All done!')
            # we are done for 60 minutes
            self.nextWheelProcess = seconds + (60 * 60)
            self.optionContractsAvailable = True
            # Then start again
            self.wheelSymbolsToProcess = list(self.wheelSymbols)
            self.wheelSymbolsProcessed = []

    """
    IB API wrappers
    """


    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        if errorCode == 200:
            # 'No security definition has been found for the request':
            self.clearRequestId(reqId)
        elif errorCode == 162:
            # Historical Market Data Service error message:HMDS query returned no data: PFSI@SMART Historical_Volatility
            self.clearRequestId(reqId)
        else:
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)
    # ! [error] self.XreqId2nErr[reqId] += 1

    @iswrapper
    # ! [tickprice]
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
#        print("TickPrice. TickerId:", reqId, "tickType:", tickType,
#            "Price:", price, "CanAutoExecute:", attrib.canAutoExecute,
#            "PastLimit:", attrib.pastLimit, end=' ')
#        if tickType == TickTypeEnum.BID or tickType == TickTypeEnum.ASK:
#            print("PreOpen:", attrib.preOpen)
#        else:
#            print()
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
        count = self.clearRequestId(reqId)
        if (count == 0):
            # continue data fetching if no more running requests
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
#            print("SecurityDefinitionOptionParameter.",
#                "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier,
#                "Expirations:", expirations, "Strikes:", str(strikes))
            self.wheelSymbolsExpirations = sorted(expirations)
            self.wheelSymbolsProcessingStrikes = sorted(strikes)
            self.wheelSymbolsProcessing = tradingClass
    # ! [securityDefinitionOptionParameter]

    @iswrapper
    # ! [securityDefinitionOptionParameterEnd]
    def securityDefinitionOptionParameterEnd(self, reqId: int):
        super().securityDefinitionOptionParameterEnd(reqId)
        # Just got options information, we should be able to start processing immediately
        self.nextWheelProcess = 0
        self.findWheelSymbolsInfo()
    # ! [securityDefinitionOptionParameterEnd]

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
            self.sellCoveredCallsIfPossible(contract, position, marketPrice, marketValue,
                averageCost, unrealizedPNL, realizedPNL, accountName)
    # ! [updateportfolio]

    @iswrapper
    # ! [accountdownloadend]
    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        self.portfolioLoaded = True
    # ! [accountdownloadend]

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
    # ! [historicaldata]
    def historicalData(self, reqId:int, bar: BarData):
        super().historicalData(reqId, bar)
        self.getDbConnection()
        c = self.db.cursor()
        t = (bar.close, reqId, )
        c.execute(
            'UPDATE stock '
            ' SET Historical_Volatility = ? '
            ' WHERE stock.id = (SELECT contract.id FROM contract where contract.api_req_id = ?)', 
            t)
        c.close()
        self.db.commit()
    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        count = self.clearRequestId(reqId)
        self.findWheelSymbolsInfo()
    # ! [historicaldataend]

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
#        print('contractDetails. reqId:', reqId, 'contract:', contractDetails.contract, 'saved reqMktData id:', nextReqId)
        queryTime = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")
        queryTime = (datetime.datetime.today()).strftime("%Y%m%d 00:00:00")
        queryTime = ""
        if contractDetails.contract.secType == 'STK':
            self.reqHistoricalData(nextReqId, contractDetails.contract, queryTime,
                "1 D", "1 day", "HISTORICAL_VOLATILITY", 0, 1, False, [])
        elif contractDetails.contract.secType == 'OPT':
            self.reqMktData(nextReqId, contractDetails.contract, "", True, False, [])
    # ! [contractdetails]

    @iswrapper
    # ! [managedaccounts]
    def managedAccounts(self, accountsList: str):
        super().managedAccounts(accountsList)
        if self.account:
            return
        else:
            # first time
            self.account = accountsList.split(",")[0]
            self.benchmarkSymbol = 'VT'
            self.nakedPutsRatio = 0.5
            self.wheelSymbols = {
                # 0.0 will sell only Calls on symbol, no more Put sale
                'SPG': 0.0, 'XOM': 0.0, 'IQ': 0.0, 'TME': 0.0,
                'PSEC': 0.005, 'PFSI': 0.005,
                # standard size 2%
                'CCL': 0.02, 'MGM': 0.02,
                'YNDX': 0.35,
                # Barrage Capital
                'KMI': 0.05, 'ATVI': 0.05,
                # 0.05 to 0.1 for high quality/blue chips
                'AAPL': 0.06, 'MSFT': 0.1, 'KO': 0.05,
                # 0.05 for ETF
                'RSX': 0.05, 'QQQ': 0.05, 'AIA': 0.05,  'FXI': 0.05,
                # High but the lowest for a single contract
                'TSM': 0.06, 'BIDU': 0.1, 'BABA': 0.1, 'SPY': 0.2, 
            }
            self.wheelSymbolsToProcess = list(self.wheelSymbols)
            self.wheelSymbolsProcessing = None
            self.wheelSymbolsProcessingStrikes = []
            self.wheelSymbolsExpirations = []
            self.wheelSymbolsProcessed = []

            self.lastCashAdjust = 0
            self.lastNakedPutsSale = 0
            self.nextWheelProcess = 0

            self.clearApiReqId()
            self.clearPortfolioBalances(self.account)
            self.clearPortfolioPositions(self.account)
            self.clearOpenOrders(self.account)

            self.reqMarketDataType(MarketDataTypeEnum.REALTIME)
            # start account updates
            self.reqAccountUpdates(True, self.account)
            # Requesting the next valid id. The parameter is always ignored.
            self.reqIds(-1)
            self.reqOpenOrders()
    # ! [managedaccounts]

    @iswrapper
    # ! [updateaccounttime]
    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        # perform regular tasks
        self.findWheelSymbolsInfo()
        self.sellNakedPuts()
        self.adjustCash()
    # ! [updateaccounttime]

    """
    Main Program
    """
    @iswrapper
    # ! [connectack]
    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    # ! [connectack]

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
    # ! [nextvalidid]

        # we can start now
        self.start()

    @printWhenExecuting
    def start(self):
        if self.started:
            return
        self.started = True
        # first retrieve account info
        self.reqManagedAccts()

    @printWhenExecuting
    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            self.done = True

    @printWhenExecuting
    def stop(self):
        # ! [cancelaaccountupdates]
        self.reqAccountUpdates(False, self.account)
        # ! [cancelaaccountupdates]
        self.clearApiReqId()
        if (self.db):
            self.db.close()
