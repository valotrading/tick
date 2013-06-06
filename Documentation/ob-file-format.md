OB File Format Specification
============================

An Order Book (OB) file contains order book events, such as Add Order or
Trade. The events are contained in a tabular format, and the table may be
stored, for example, in a tab-separated values (TSV) file.


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

The table consists of the following columns. See the events below for the
interpretation of the columns for a particular event type.

Name     | Type       | Description
---------|------------|-----------------------------------
Event    | Character  |
Date     | Date       |
Time     | Integer    | Nanoseconds since midnight
TimeZone | String     | IANA Time Zone Database
Exchange | String     | MIC (ISO 10383)
Symbol   | String     | As defined by the exchange
OrderID  | Identifier | The order identifier
ExecID   | Identifier | The execution identifier
Side     | Character  | "B" for buy or "S" for sell
Quantity | Integer    |
Price    | Decimal    |
Status   | String     |


## Date

Start of a new day for an exchange. Timestamps for all symbols on the exchange
are reset to zero.

Column   | Required | Notes
---------|----------|------
Event    | Yes      | D
Date     | Yes      |
TimeZone | Yes      |
Exchange | Yes      |


## Add Order

Add an order.

Column   | Required | Notes
---------|----------|------
Event    | Yes      | A
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
OrderID  | Yes      |
Side     | Yes      |
Quantity | Yes      |
Price    | Yes      |


## Cancel Order

Cancel the identified order partially or fully.

Column   | Required | Notes
---------|----------|-------------------------
Event    | Yes      | X
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
OrderID  | Yes      | as in the original order
Quantity | Yes      | the canceled quantity


## Execute Order

Execute the identified order partially or fully.

Column   | Required | Notes
---------|----------|-------------------------
Event    | Yes      | E
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
Side     | Yes      | as in the original order
OrderID  | Yes      | as in the original order
ExecID   | No       |
Quantity | Yes      | the executed quantity
Price    | Yes      | the execution price


## Trade

Execute a non-displayed or off-the-book order partially or fully.

Column   | Required | Notes
---------|----------|----------------------
Event    | Yes      | T
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
Side     | No       |
ExecID   | No       |
Quantity | Yes      | the executed quantity
Price    | Yes      | the execution price


## Trade Break

Mark the identified execution as broken.

Column   | Required | Notes
---------|----------|--------------------
Event    | Yes      | B
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
ExecID   | Yes      | as in the original execution


## Clear

Clear the order book for the specified symbol.

Column   | Required | Notes
---------|----------|------
Event    | Yes      | C
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |


## Status

Change the trading status for the specified symbol.

Column   | Required | Notes
---------|----------|----------
Event    | Yes      | S
Time     | Yes      |
Exchange | Yes      |
Symbol   | Yes      |
Status   | Yes      | see below


The status column contains one of the following characters.

Status | Description
-------|------------
H      | Halted
Q      | Quote only
T      | Trading
