"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""

import os.path
import time
import logging
import datetime
import argparse
import sqlite3
import math

from ibapi import wrapper
from ibapi import utils
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

from Testbed.Program import TestApp

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

    """ <summary>
	#/ A Midprice order is designed to split the difference between the bid and ask prices, and fill at the current midpoint of
	#/ the NBBO or better. Set an optional price cap to define the highest price (for a buy order) or the lowest price (for a sell
	#/ order) you are willing to accept. Requires TWS 975+. Smart-routing to US stocks only.
    </summary>"""
    @staticmethod
    def Midprice(action:str, quantity:float, priceCap:float):

        #! [midprice]
        order = Order()
        order.action = action
        order.orderType = "MIDPRICE"
        order.totalQuantity = quantity
        order.lmtPrice = priceCap # optional
        order.tif = "DAY"
        order.transmit = False
        #! [midprice]
        return order

class Trader(TestApp):

    def __init__(self):
        TestApp.__init__(self)
        self.db = None
        self.account = None
        self.NAV = None
        self.benchmarkSymbol = None
        self.benchmarkCurrency = None
        self.cashAvailableRatio = 1/100
        self.portfolioLoaded = False
        self.ordersLoaded = False
        self.lastRunDate = time.time()

    def getDbConnection(self):
        if self.db == None:
            self.db = sqlite3.connect('../db/var/db/data.db')
        return self.db;

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

    def normalizeSymbol(self, symbol):
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

    # RYL
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

    def createOrUpdatePosition(self, cid: int, position: float, averageCost: float, accountName: str):
        print('createOrUpdatePosition')
        self.getDbConnection()
        c = self.db.cursor()
        t = (accountName, )
        c.execute('SELECT id, base_currency FROM portfolio WHERE account = ?', t)
        r = c.fetchone()
        #print(r)
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

    def sellNakedPuts(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        print('sellNakedPuts')
        self.getDbConnection()
        c = self.db.cursor()
        t = (self.account, 'OPT', 'P', 'EUR', )
        c.execute(
            'SELECT SUM(position.quantity * option.strike * option.multiplier / currency.rate) FROM position, portfolio, contract, option, currency WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? AND position.contract_id = contract.id AND contract.secType = ? AND position.contract_id = option.id AND option.call_or_put = ? AND contract.currency = currency.currency AND currency.base = ?',
            t)
        r = c.fetchone()
        c.close()
        totalEngagements = float(r[0])
        freeSpace = self.NAV + totalEngagements
        if (freeSpace > 0):
            print('sellNakedPuts freeSpace:')
            print(freeSpace)

    def adjustCash(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        print('adjustCash')

        self.getDbConnection()
        c = self.db.cursor()

        # how much cash do we have?
        t = (self.account, 'EUR', )
        c.execute(
            'SELECT SUM(balance.quantity / currency.rate) FROM portfolio, balance, currency WHERE balance.portfolio_id = portfolio.id AND portfolio.account = ? AND balance.currency = currency.currency AND currency.base = ?',
            t)
        r = c.fetchone()
        total_cash = float(r[0])
        print('total cash:', total_cash)

        # how much do we need to cover ALL short puts?
        t = (self.account, 'OPT', 'P', 'EUR', )
        c.execute(
            'SELECT SUM(position.quantity * option.strike * option.multiplier / currency.rate) FROM position, portfolio, contract, option, currency WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? AND position.quantity < 0 AND position.contract_id = contract.id AND contract.secType = ? AND position.contract_id = option.id AND option.call_or_put = ? AND contract.currency = currency.currency AND currency.base = ?',
            t)
        r = c.fetchone()
        naked_puts_engaged = float(r[0])
        print('total naked put:', naked_puts_engaged)

        # how much do we need to cover ITM short puts?
        t = (self.account, 'OPT', 'P', 'EUR', )
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
            ' AND currency.base = ?',
            t)
        r = c.fetchone()
        naked_puts_amount = float(r[0])
        print('needed to secure naked put:', naked_puts_amount)

        # open Sell order quantity
        t = (self.account, self.benchmarkSymbol, 'STK', 'SELL', )
        c.execute(
            'SELECT SUM(total_qty) '\
            'FROM open_order, portfolio, contract ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = contract.id AND contract.symbol = ? AND contract.secType = ? ' \
            ' AND open_order.action_type = ?',
            t)
        r = c.fetchone()
        if r[0]:
            benchmark_on_sale = float(r[0])
        else:
            benchmark_on_sale = 0
        print('benchmark_on_sale:', benchmark_on_sale)
        # open Buy order quantity
        t = (self.account, self.benchmarkSymbol, 'STK', 'BUY', )
        c.execute(
            'SELECT SUM(total_qty) '\
            'FROM open_order, portfolio, contract ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = contract.id AND contract.symbol = ? AND contract.secType = ? ' \
            ' AND open_order.action_type = ?',
            t)
        r = c.fetchone()
        if r[0]:
            benchmark_on_buy = float(r[0])
        else:
            benchmark_on_buy = 0
        print('benchmark_on_buy:', benchmark_on_buy)

        # benchmark price in base
        t = (self.benchmarkSymbol, 'EUR', )
        c.execute(
            'SELECT (contract.price / currency.rate) ' \
            'FROM contract, currency WHERE contract.symbol = ?' \
            ' AND contract.currency = currency.currency ' \
            ' AND currency.base = ?',
            t)
        r = c.fetchone()
        benchmarkPrice = float(r[0])
        print('benchmarkPrice in base:', benchmarkPrice)

        to_adjust = (total_cash + naked_puts_amount) / benchmarkPrice
        print('to_adjust:', to_adjust)

        if (to_adjust >= 1):
            to_adjust -= benchmark_on_buy
            # Cancel all Sell orders
            t = (self.account, self.benchmarkSymbol, 'STK', 'SELL', )
        elif (to_adjust <= -1):
            to_adjust += benchmark_on_sale
            # Cancel all Buy orders
            t = (self.account, self.benchmarkSymbol, 'STK', 'BUY', )
        c.execute(
            'SELECT open_order.order_id '\
            'FROM open_order, portfolio, contract ' \
            'WHERE open_order.account_id = portfolio.id AND portfolio.account = ? ' \
            ' AND open_order.contract_id = contract.id AND contract.symbol = ? AND contract.secType = ? ' \
            ' AND open_order.action_type = ?',
            t)
        for r in c:
            print('canceling order:', r[0])
            self.cancelOrder(int(r[0]))
        print('to_adjust after adjustement:', to_adjust)
        contract = Contract()
        contract.symbol = self.benchmarkSymbol
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"
        if (to_adjust >= 1):
            print('toBuy: ', math.floor(to_adjust))
            self.placeOrder(self.nextOrderId(), contract, TraderOrder.Midprice("BUY", math.floor(to_adjust), 1))
        elif (to_adjust < 0):
            print('to sell: ', math.ceil(-to_adjust))
            self.placeOrder(self.nextOrderId(), contract, TraderOrder.Midprice("SELL", math.ceil(-to_adjust), 1000000))

        c.close()

    def sellCoveredCallsIfPossible(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return
        self.getDbConnection()
        c = self.db.cursor()
        cid = self.findOrCreateContract(contract)
        t = (marketPrice, cid)
        c.execute('UPDATE contract SET price = ?, bid = NULL, ask = NULL WHERE id = ?', t)
        if (contract.secType == 'STK'):
            portfolio_id = self.findPortfolio(accountName)
            # compute net position, remove CALL positions
            t = (accountName, cid, 'OPT', 'C', )
            c.execute(
                'SELECT SUM(position.quantity * option.multiplier), MIN(option.multiplier) FROM position, portfolio, contract, option WHERE position.portfolio_id = portfolio.id AND portfolio.account = ? AND position.contract_id = ? AND contract.secType = ? AND position.contract_id = option.id AND option.call_or_put = ?',
                t)
            r = c.fetchone()
            if r[0]:
                position -= int(r[0])
                multiplier = int(r[1])
                print('call positions adjust:', -int(r[0]))
            else:
                print('no call positions')
                multiplier = 100
            # compute net position, remove sell orders
            t = (portfolio_id, cid, 'STK', 'SELL', )
            c.execute(
                'SELECT SUM(total_qty) FROM open_order, contract WHERE open_order.account_id = ? AND open_order.contract_id = ? AND contract.id = open_order.contract_id AND contract.secType = ? AND open_order.action_type = ?',
                t)
            r = c.fetchone()
            if r[0]:
                position -= int(r[0])
                print('sell orders adjust:', -int(r[0]))
            else:
                print('no stock sell order')
            # compute net position, remove calls open orders
            t = (portfolio_id, cid, 'OPT', 'C', )
            c.execute(
                'SELECT SUM(total_qty * option.multiplier) FROM open_order, contract, option WHERE open_order.account_id = ? AND open_order.contract_id = ? AND contract.secType = ? AND open_order.contract_id = option.id AND option.call_or_put = ?',
                t)
            r = c.fetchone()
            if r[0]:
                position -= int(r[0])
                print('open orders adjust:', -int(r[0]))
            else:
                print('no call sell order')
            if position >= multiplier:
                print('*** can sell', math.floor(position / multiplier), 'calls')
        c.close()
        self.db.commit()

    def rollOptionIfNeeded(self):
        if (not self.portfolioLoaded) or (not self.ordersLoaded):
            return

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
#            if (self.NAV != None) and (key == 'CashBalance') and (currency == self.benchmarkCurrency):
#                self.adjustCash(val)
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
            self.NAV = float(val)
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
            c.execute('UPDATE contract SET price = ?, bid = NULL, ask = NULL WHERE id = ?', t)
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
                t = (portfolio_id, contract_id, order.permId, order.clientId, orderId, order.action, order.totalQuantity, order.cashQty, order.lmtPrice, order.auxPrice, orderState.status, )
                c.execute('INSERT INTO open_order(account_id, contract_id, perm_id, client_id, order_id, action_type, total_qty, cash_qty, lmt_price, aux_price, status) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', t)
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
        t = (status, orderId, )
        c.execute('UPDATE open_order SET status = ? WHERE order_id = ?', t)
        c.close()
        self.db.commit()
    # ! [orderstatus]

    @iswrapper
    # ! [updateaccounttime]
    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        seconds = time.time()
        if (seconds > (self.lastRunDate + (1 * 60))):
            self.lastRunDate = seconds
            self.adjustCash()
    # ! [updateaccounttime]

    def start(self):
        if self.started:
            return
        self.started = True
        # first retrieve account info
        self.reqManagedAccts()

    def stop(self):
        super().stop()
        # ! [cancelaaccountupdates]
        self.reqAccountUpdates(False, self.account)
        # ! [cancelaaccountupdates]
        if (self.db):
            self.db.close()

    @iswrapper
    # ! [managedaccounts]
    def managedAccounts(self, accountsList: str):
        if self.account:
            return
        super().managedAccounts(accountsList)

        self.benchmarkSymbol = 'VT'
        self.benchmarkCurrency = 'USD'
        self.clearPortfolioBalances(self.account)
        self.clearPortfolioPositions(self.account)
        self.clearOpenOrders(self.account)
        # start account updates
        self.reqAccountUpdates(True, self.account)
        # Requesting the next valid id
        # ! [reqids]
        # The parameter is always ignored.
        self.reqIds(-1)
        # ! [reqids]
        # Requesting all open orders
        # ! [reqallopenorders]
#        self.reqAllOpenOrders()
        # ! [reqallopenorders]
        # Taking over orders to be submitted via TWS
        # ! [reqautoopenorders]
#        self.reqAutoOpenOrders(True)
        # ! [reqautoopenorders]
        # Requesting this API client's orders
        # ! [reqopenorders]
        self.reqOpenOrders()
        # ! [reqopenorders]
    # ! [managedaccounts]

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

    # from inspect import signature as sig
    # import code code.interact(local=dict(globals(), **locals()))
    # sys.exit(1)

    # tc = TestClient(None)
    # tc.reqMktData(1101, ContractSamples.USStockAtSmart(), "", False, None)
    # print(tc.reqId2nReq)
    # sys.exit(1)

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
