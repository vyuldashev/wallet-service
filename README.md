## Overview of changes

1. I fixed an issue with concurrent withdrawals which led to negative balances because in the original code there was no SELECT FOR UPDATE or atomic UPDATE.
2. I added tests for different critical scenarios (although there are much more of them).
3. Transfer handler was also fixed to handle concurrent transfers. Both wallets with specified currencies are locked, and then the balances are updated, so there is no race.
4. I introduced typed UUIDs into project so the database compares bytes not string which may be in different cases.
5. Amounts are normalized to four decimal places. Amounts are discussed more in the dedicated section.
6. API mentioned support for multiple currencies. Project lacked implementation of this feature, so I added it.
7. Idempotency was added to deposit, transfer and withdraw methods. One request_id can be used only once. There is much more to explore in this area, but I added just a basic implementation. 

## Amounts

From my experience, it is better to store amounts in the smallest possible unit. 
For example, if you are working with USD better to store in cents so nothing is lost. In Go, we can use `math` package for precise calculations, particularly `*big.Int`.
We also need to validate currencies, store their decimal number values and do the conversions of amounts in code instead of in the database.
