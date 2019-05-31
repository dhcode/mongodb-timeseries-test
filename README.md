# MongoDB Time series test

A project to find the most efficient way to store time series data in MongoDB.

## Run

Run the database (mongod must be available in your PATH):

    npm run mongodb

To let the test scenarios run:

    npm run test1
    
## Buckets

### Time series bucket simple

Each document has a timestamp as _id that is floored to e.g. the base of an hour. Is has a static size.
E.g. hourly bucket in second precision means 86400 entries per document per property.

    {
        _id: '2018-01-01T19:00:00Z',
        prop1: [
            0,
            0,
            ...
            0
        ]
    }

### Time series bucket variant

Same as the time series bucket simple. But it has additional sum properties. This makes aggregation queries much more efficient.

    {
        _id: '2018-01-01T19:00:00Z',
        _s: {
            prop1: 0
        },
        prop1: [
            0,
            0,
            ...
            0
        ]
    }

## Results

### Hour bucket with seconds precision

* 1 Year of data (simple bucket, 1 property)
  * size: 291.7MB 
  * storage: 158.2MB 
  * index: 144.0KB
  * Query times:
    * 24h sec received: 86400 dur: 392
    * 24h sec2 [2] received: 86400 dur: 205
    * 48h min received: 2880 dur: 215
    * 48h sec2 [2] received: 2880 dur: 92
    * 30d hour received: 720 dur: 2900
    * 30d hour [2] received: 720 dur: 1201


* 1 Year of data (variant bucket, 1 property)
  * size: 291.8MB 
  * storage: 152.9MB 
  * index: 136.0KB
  * Query times:
    * 24h sec received: 86400 dur: 323
    * 48h min received: 2880 dur: 231
    * 30d hour received: 720 dur: 6

### Minute bucket with seconds precision

* 1 Year of data (simple bucket, 1 property)
  * size: 248.6MB 
  * storage: 44.4MB 
  * index: 5.9MB
  * Query times:
    * 24h sec received: 86400 dur: 338
    * 24h sec2 [2] received: 86400 dur: 178
    * 48h min received: 2880 dur: 217
    * 48h sec2 [2] received: 2880 dur: 105
    * 30d hour received: 721 dur: 3070
    * 30d hour [2] received: 721 dur: 1214
      
* 1 Year of data (variant bucket, 1 property)
  * size: 256.6MB 
  * storage: 42.8MB 
  * index: 5.9MB
  * Query times:
    * 24h sec received: 86400 dur: 325
    * 48h min received: 2880 dur: 15
    * 30d hour received: 721 dur: 152
    
[2] use find instead of aggregate and aggregate on application side
