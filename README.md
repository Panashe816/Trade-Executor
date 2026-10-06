\# Trade Executor - Phase 6



Phase 6 is the demo-only trade execution component of the Telegram trading signal pipeline.



\## Architecture



Phase 4 parses and validates trading signals and publishes structured execution messages.



Phase 6:



1\. Listens only to the Phase 4 output channel.

2\. Reads the structured execution data produced by Phase 4.

3\. Does not re-parse the original Telegram signal.

4\. Validates the execution information.

5\. Monitors the configured MetaApi demo account.

6\. Waits for price to enter the permitted execution range.

7\. Executes the configured positions.

8\. Applies the supplied stop loss and take-profit targets.

9\. Verifies executed positions through MetaApi.

10\. Records execution and lifecycle information.



\## Current Trading Configuration



\- Platform: MetaTrader 5 through MetaApi

\- Account: Demo only

\- Symbol: XAUUSD\_i

\- Positions per signal: 2

\- Lot size: configured through environment variables

\- Entry extension: 30 pips / 3.0 price units

\- Signal validity: 2 hours

\- TP targets: supplied by Phase 4

\- Stop loss: supplied by Phase 4



\## Required Environment Variables



Sensitive configuration is stored in `.env` and must never be committed to GitHub.



The environment configuration includes:



\- MetaApi credentials

\- Telegram API credentials

\- Telegram session

\- Phase 4 output channel configuration

\- Trading configuration



\## Installation



Install the required Python packages:



```bash

pip install -r requirements.txt

