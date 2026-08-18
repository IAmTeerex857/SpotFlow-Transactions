# Gadsfy Payment Analysis: August 10-16, 2026

## Summary

| Metric | Total |
|---|---:|
| Transactions | 2,121 |
| Successful | 363 |
| Failed | 1,691 |
| Abandoned | 67 |
| Success rate | 17.1% |
| Failure rate | 79.7% |
| Failed USD-equivalent volume | $21,223.19 |

## Daily Breakdown

| Date | Transactions | Successful | Failed | Failure rate | Failure currencies |
|---|---:|---:|---:|---:|---|
| Aug 10 | 746 | 122 | 591 | 79.2% | XAF 278, UGX 311, NGN 2 |
| Aug 11 | 607 | 114 | 466 | 76.8% | XAF 385, UGX 75, NGN 6 |
| Aug 12 | 263 | 45 | 211 | 80.2% | XAF 199, UGX 9, NGN 3 |
| Aug 13 | 96 | 8 | 88 | 91.7% | XAF 87, UGX 1 |
| Aug 14 | 209 | 34 | 175 | 83.7% | XAF 173, UGX 2 |
| Aug 15 | 105 | 20 | 85 | 81.0% | XAF 84, UGX 1 |
| Aug 16 | 95 | 20 | 75 | 78.9% | XAF 74, UGX 1 |

August 13 was the weakest day, with only eight successful transactions and a 91.7% failure rate.

## Currency Breakdown

| Currency | Transactions | Successful | Failed | Abandoned | Failure rate | Failed local amount | USD equivalent |
|---|---:|---:|---:|---:|---:|---:|---:|
| XAF | 1,548 | 268 | 1,280 | 0 | 82.7% | XAF 9,710,705 | $15,243.96 |
| UGX | 463 | 63 | 400 | 0 | 86.4% | UGX 23,279,533 | $5,894.58 |
| NGN | 110 | 32 | 11 | 67 | 10.0% | NGN 121,524.44 | $84.65 |

UGX had the highest failure rate. XAF generated the largest number and value of failed transactions.

## Failure Messages

| Failure message | Total | XAF | UGX | NGN |
|---|---:|---:|---:|---:|
| Customer does not have enough funds | 1,141 | 1,084 | 57 | 0 |
| Insufficient funds or payment limit reached | 248 | 0 | 248 | 0 |
| Customer did not authorize in time / operator timeout | 192 | 179 | 13 | 0 |
| Airtel Uganda gave no failure reason | 61 | 0 | 61 | 0 |
| Inactive MTN Uganda account | 16 | 0 | 16 | 0 |
| Mobile-money wallet locked | 11 | 10 | 1 | 0 |
| Error processing payment | 6 | 0 | 0 | 6 |
| Inactive MTN Cameroon account | 5 | 5 | 0 | 0 |
| Blank provider message | 4 | 0 | 0 | 4 |
| Inactive Airtel Uganda account | 2 | 0 | 2 | 0 |
| MTN Cameroon gave no failure reason | 2 | 2 | 0 | 0 |
| Please enter your PIN | 1 | 0 | 0 | 1 |
| Uganda MSISDN too short | 1 | 0 | 1 | 0 |
| MTN Uganda gave no failure reason | 1 | 0 | 1 | 0 |

## Key Findings

- Insufficient-funds messages caused 1,389 failures, or 82.1% of all failures.
- Including authorization timeouts, customer funding and authorization issues caused 93.5% of failures.
- Cameroon/XAF generated 1,280 failures and $15,243.96 in failed value.
- Uganda/UGX had the highest currency-level failure rate at 86.4%.
- Gadsfy transaction volume declined from 746 transactions on August 10 to 95 on August 16.
- Despite lower traffic, the failure rate remained close to 80%.
- NGN performed much better, although 60.9% of NGN attempts were abandoned rather than completed.
