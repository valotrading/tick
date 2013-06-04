import pandas


class Scope(object):
    pass


def from_ob(ob):
    outer = Scope()
    outer.taq = empty()

    def to_taq(ob_event):
        if ob_event['Event'] == 'D':
            outer.taq = append(outer.taq, taq_date(ob_event))

    ob.apply(to_taq, axis=1)

    return outer.taq


def taq_date(ob_event):
    return {
        'Event':    'D',
        # Date
        'TimeZone': ob_event['TimeZone'],
        'Exchange': ob_event['Exchange'],
    }


def append(taq, taq_event):
    return taq.append(taq_event, ignore_index=True)


def empty():
    result = pandas.DataFrame()

    result['Event'        ] = []
    result['Date'         ] = []
    result['Time'         ] = []
    result['TimeZone'     ] = []
    result['Exchange'     ] = []
    result['Symbol'       ] = []
    result['ExecID'       ] = []
    result['TradeQuantity'] = []
    result['TradePrice'   ] = []
    result['TradeSide'    ] = []
    result['BidQuantity1' ] = []
    result['BidPrice1'    ] = []
    result['AskQuantity1' ] = []
    result['AskPrice1'    ] = []

    return result
