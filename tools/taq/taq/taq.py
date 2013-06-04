import pandas


class Scope(object):
    pass


def from_ob(ob):
    outer = Scope()
    outer.taq = empty()

    def to_taq(ob_event):
        if ob_event['Event'] == 'D':
            outer.taq = append(outer.taq, taq_date(ob_event))
        elif ob_event['Event'] == 'T':
            outer.taq = append(outer.taq, taq_trade(ob_event))
        elif ob_event['Event'] == 'B':
            outer.taq = append(outer.taq, taq_trade_break(ob_event))

    ob.apply(to_taq, axis=1)

    return outer.taq


def taq_date(ob_event):
    return {
        'Event':    'D',
        # Date
        'TimeZone': ob_event['TimeZone'],
        'Exchange': ob_event['Exchange'],
    }


def taq_trade(ob_event):
    return {
        'Event':         'T',
        'Time':          ob_event['Time'],
        'Exchange':      ob_event['Exchange'],
        'Symbol':        ob_event['Symbol'],
        'ExecID':        ob_event['ExecID'],
        'TradeQuantity': ob_event['Quantity'],
        'TradePrice':    ob_event['Price'],
        'TradeSide':     ob_event['Side'],
    }


def taq_trade_break(ob_event):
    return {
        'Event':    'B',
        'Time':     ob_event['Time'    ],
        'Exchange': ob_event['Exchange'],
        'Symbol':   ob_event['Symbol'  ],
        'ExecID':   ob_event['ExecID'  ],
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
