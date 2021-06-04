"""
TBD
"""

from ibapi.order import * # @UnusedWildImport

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
    def BuyBenchmark(quantity: int):
        order = TraderOrder.Midprice("BUY", quantity, 1)
        # always true because of price cap that will prevent execution
        order.transmit = True
        return order

    @staticmethod
    def SellBenchmark(quantity: int):
        order = TraderOrder.Midprice("SELL", quantity, 1000000)
        # always true because of price cap that will prevent execution
        order.transmit = True
        return order

    @staticmethod
    def SellNakedPut(priceCap: float):
        order = TraderOrder.Order()
        order.action = 'SELL'
        order.orderType = "LMT"
        order.totalQuantity = 1
        order.lmtPrice = priceCap
        return order

    @staticmethod
    def SellCoveredCall(priceCap: float, quantity: int):
        order = TraderOrder.Order()
        order.action = 'SELL'
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = priceCap
        return order
