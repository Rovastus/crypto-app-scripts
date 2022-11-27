# crypto-app-scripts

Scripts for creating the exports files which are used by crypto-app.

## Export columns

| Column name | Column description                                                                                             |
| ----------- | -------------------------------------------------------------------------------------------------------------- |
| UTC_Time    | The time when the event occurred in the UTC timezone                                                           |
| Operation   | The name of operation e.g. Transaction, Transfer or Earn                                                       |
| Description | More info regarding the event e.g. Binance earn program and etc.                                               |
| Data        | Necessary info regarding the event, more info can be found in the section _Export Data column json structures_ |

## Export Data column json structures

**Transaction**:

- JSON fields
  | Field name | Field description |
  | ------------- | ------------- |
  | buy | The amount which was bought |
  | buyCoin | The sticker of the coin which was bought |
  | price | The amount which was sold |
  | priceCoin | The sticker of the coin which was sold |
  | fee | The amount which was paid as fee |
  | feeCoin | The sticker of the coin which was paid as fee |

- Example
  ```javascript
    {
      "buy": 10,
      "buyCoin": "BTC",
      "price": 10,
      "priceCoin": "EUR",
      "fee": 1,
      "feeCoin": "BTC",
    }
  ```

**Transfer (Fee for transfering funds)**:

- JSON Fields
  | Field name | Field description |
  | ------------- | ------------- |
  | fee | The amount which was paid as fee |
  | feeCoin | The sticker of the coin which was paid as fee |

- Example
  ```javascript
    {
      "fee": 1,
      "feeCoin": "BTC",
    }
  ```

**Earn**:

- JSON Fields
  | Field name | Field description |
  | ------------- | ------------- |
  | amount | The amount which was earned |
  | coin | The sticker of the coin which was earned |
- Example
  ```javascript
    {
    "amount": 10,
    "coin": "DOT"
    }
  ```

## Testing

```
python -m unittest discover src "*_tests.py"
```
