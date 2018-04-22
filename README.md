# MongoDB Time series test

A project to find the most efficient way to store time series data in MongoDB.

## Run

To let the test scenarios run:

    npm run test1
    
## Results

### Nested minute - second precision

    updates: 525599 
    size: 853.6MB 
    storage: 256.1MB 
    index: 5.9MB 
    avg: 1703 
    dur: 454684
    
### Nested hour - second precision

    updates: 525599 
    size: 893.3MB 
    storage: 251.0MB 
    index: 148.0KB 
    avg: 106923 
    dur: 289776
    
### Nested hour - minute - second precision

    updates: 525599 
    size: 855.3MB 
    storage: 271.1MB 
    index: 144.0KB 
    avg: 102383 
    dur: 252280
    
### Simple hour - second precision

    updates: 525599 
    size: 291.7MB 
    storage: 113.2MB 
    index: 128.0KB 
    avg: 34916 
    dur: 215115
    
Smallest index
    
### Simple minute - second precision

    updates: 525599 
    size: 248.6MB *
    storage: 35.2MB * 
    index: 5.6MB 
    avg: 496 
    dur: 428019

Small storage, larger index
