Trades and Quotes (TAQ) File Format Specification
=================================================

A Trades and Quotes (TAQ) file contains trade and quote events. The events are
contained in a tabular format, and the table may be stored, for example, in a
tab-separated values (TSV) file.


## Data Types

- **Character**: An ASCII character.
- **Date**: An ASCII string containing YYYY-MM-DD (ISO 8601).
- **Integer**: A 64-bit unsigned integer.
- **String**: A string of ASCII characters.
- **Decimal**: A 64-bit unsigned integer. The four least significant digits
  represent four decimal digits.
- **Identifier**: A 64-bit unsigned integer.


### TSV

When the table is stored in a TSV file, the following data representations are
used:

  - **Identifier**: An ASCII string containing the integer value in base-36
    encoding.
  - **Integer**: An ASCII string containing the integer value.
  - **Decimal**: An ASCII string containing the decimal value, including the
    decimal point.


## Columns

The table consists of the following columns. The number of quote levels is not
fixed: the table contains always at least the top level (1) and may contain an
arbitrary number of lower levels. See the events below for the interpretation
of the columns for a particular event type.


Name          | Type       | Description
--------------|------------|---------------------------------------
Event         | Character  |
Date          | Date       | YYYY-MM-DD (ISO 8601)
Time          | Integer    | Nanoseconds since midnight
TimeZone      | String     | IANA Time Zone Database
Exchange      | String     |
Symbol        | String     |
ExecID        | Identifier | The execution identifier
TradeQuantity | Integer    |
TradePrice    | Decimal    |
TradeSide     | Character  | "B" for buy or "S" for sell
TradeType     | Character  | See below
Status        | String     |
BidQuantity1  | Integer    | Quantity on the best bid level
BidPrice1     | Decimal    | The best bid price
...           | ...        |
...           | ...        |
BidQuantityN  | Integer    |
BidPriceN     | Decimal    |
AskQuantity1  | Integer    | Quantity on the best ask level
AskPrice1     | Decimal    | The best ask price
...           | ...        |
...           | ...        |
AskQuantityN  | Integer    |
AskPriceN     | Decimal    |


TradeType | Description
----------|------------------
R         | Regular
N         | Non-displayed
I         | Intermarket sweep


## Date

Start of a new day for an exchange. Timestamps for all symbols on the exchange
are reset to zero.

Column   | Required | Notes
---------|----------|------
Event    | Yes      | D
Date     | Yes      |
TimeZone | Yes      |
Exchange | Yes      |


## Trade

Execute an order partially or fully.

Column        | Required | Notes
--------------|----------|------
Event         | Yes      | T
Time          | Yes      |
Exchange      | Yes      |
Symbol        | Yes      |
ExecID        | No       |
TradeQuantity | Yes      |
TradePrice    | Yes      |
TradeSide     | No       |
TradeType     | Yes      |


## Trade Break

Mark the identifier execution as broken.

Column   | Required | Notes
---------|----------|-------------------------
Event    | Yes      | B
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
ExecID   | Yes      | as in the original order


## Quote

Update the order book.

Column       | Required | Notes
-------------|----------|------
Event        | Yes      | Q
Time         | Yes      |
Exchange     | Yes      |
Symbol       | Yes      |
BidQuantity1 | Yes      |
BidPrice1    | Yes      |
...          | ...      | ...
...          | ...      | ...
BidQuantityN | Yes      |
BidPriceN    | Yes      |
AskQuantity1 | Yes      |
AskPrice1    | Yes      |
...          | ...      | ...
...          | ...      | ...
AskQuantityN | Yes      |
AskPriceN    | Yes      |


## Status

Change the trading status.

Column   | Required | Notes
---------|----------|----------
Event    | Yes      | S
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
Status   | Yes      | see below


The status column contains one of the following characters.

Status | Description
-------|-----------------
H      | Halted
Q      | Quote only
T      | Trading
