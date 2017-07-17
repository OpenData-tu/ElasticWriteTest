# TU Open Data ElasticsearchWrite Test


This is a simple test to run multiple times a elasticsearch bulk insert, to test the scalability of the backend.

## Features

## Use and start of API

! Elasticsearch has to be running. Standard HOST is localhost:9200.
```
npm install
node .\src\app.js
```

## Env varibles


ES_URL e.g. '127.0.0.1:9200' the url of the elasticsearch node

ES_INDEX e.g "testindex" the index name to use. It is going to be inserted at data-<"indexname">

BATCH_SIZE -> the this of the batch you want to create, max 150.000

REPEATS -> how many times you want to insert the data