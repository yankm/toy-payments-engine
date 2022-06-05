# Toy Payments Engine

## Overview
A simple toy payments engine, capable of processing a series of transactions from a CSV file.


## Design
The engine was designed considering the following assumptions:
* There can be a maximum of 65535 accounts.
* No centralized storage supporting transactions is available.
* Transactions involving multiple clients (e.g. sending money) are not part of a product.

Given the assumptions, the basic idea is to process transactions of different clients independently in parallel in an environment that supports work stealing (to allow better load uniformity).
We could use `rayon` thread pools for that, but given that input, CSVs may potentially come from concurrent TCP streams someday, we need something concurrent-friendly as well to be future-proof.
So `tokio` runtime was chosen.

The engine uses an actor-like model, for each account, there is an `AccountWorker` that holds an account and related transactions/disputes and processes account transactions.
`PaymentsEngine` manages `AccountWorker`s and is acting as a gateway for incoming transactions. It also holds the ids of all processed transactions to avoid processing the same transaction twice (it's impossible to do that on the worker side since it has only part of the data).
This approach provides great isolation that enhances concurrency (nothing is shared) and security (e.g. client 1 cannot open a dispute for a transaction belonging to client 2, because the dispute request will be routed
to client 1 worker that has no clue about transactions of the client 2.). No locks are used, and communication between tasks is done via channels.


## Implementation details
* Account maximum possible funds is [Decimal::MAX](https://docs.rs/rust_decimal/1.24.0/rust_decimal/prelude/struct.Decimal.html#associatedconstant.MAX). Transactions that result in account total or held balance past this value will be discarded.
* Engine stores only `total` and `held` amounts for the account and calculates `available` balance dynamically. IMO, `total` should be stored in any case, and storing `held` instead of `available` makes more sense from the business logic side. This way also allows the engine to avoid overflow checks since subtraction is used to calculate the available balance.
* Since the engine state is not persisted, it doesn't store otherwise important data like created_at/updated_at timestamps for transactions and disputes, history of updates, etc.
* Internally I didn't use "resolve" semantics for a dispute as in input since charged back disputes can also be considered resolved and this introduces ambiguity. Instead, the word "cancel" is used.
* Disputes are supported only for deposits. Withdrawal disputes are not supported, since the engine cannot hold/chargeback funds that have been withdrawn from the account.
* No dispute history is stored. Once a dispute is resolved (without chargeback), it's removed from the storage.
* A transaction can have at most one active dispute at a time. But once resolved (without chargeback), other disputes can be opened.
* All operations are restricted for locked clients: any transaction or dispute will fail.
* Account operations are not atomic. E.g. if the engine is shut down during the `hold_funds` operation, the account will end up in a corrupted state. But that should be okay since all the data is lost anyway during a shutdown.
* New clients are created regardless of a transaction validity, e.g. client will be created for a dispute for a non-existing transaction. Note that for invalid transactions (e.g. missing amount in deposit/withdrawal or unsupported amount precision) clients won't be created, because these transactions do not reach the engine.
* All major streaming parts (producer, engine, and worker) do not abort on errors.


## Testing
Main pieces are covered by unit tests, also goldenfile testing (see `testdata`) is used to verify that engine produces the expected output.

## Known issues
* If eorker fails to process a transaction, engine will still consider it processed, i.e. the next transaction with the same id will be dropped even if correct. We need workers to report back to engine on success.
