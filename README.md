# Tick

Tick is a tool for inspecting and converting market data files. It uses
[Libtrading][Libtrading] for market data file parsing.

## Features

### Exchange Coverage

Exchange | OB
---------|----------
BATS     | PITCH

## Building Tick

### Requirements

Tick requires [Libtrading][Libtrading] to be installed on your system.

### Building from sources

To build and install Tick, run:

```
make install
```

The ``tick`` executable is installed to ``$HOME/bin`` by default.

[Libtrading]: http://www.libtrading.org/
