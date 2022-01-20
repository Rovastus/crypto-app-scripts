# crypto-app-scripts

## Export data column json

### Deposit

```javascript
{
  "amount": 10,
  "coin": "BTC"
}
```

### Withdrawal

```javascript
{
  "amount": 10,
  "coin": "BTC"
}
```

### Transaction

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

### Transfer

```javascript
{
  "fee": 1,
  "feeCoin": "BTC",
}
```

### Earn

```javascript
{
  "amount": 10,
  "coin": "DOT"
}
```

## Python test run

```
python -m unittest discover src "*_test.py"
```
