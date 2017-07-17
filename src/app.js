"use strict";

const fs = require('fs');

let host = process.env.ES_URL || '127.0.0.1:9200';
let es_index = process.env.ES_INDEX || "testdata";

let sizeOfBulk = process.env.BATCH_SIZE || 150000; //max 150.000
let repeatsOfInsertation = process.env.REPEATS || 2;
let numInserts = 0;

let timeNeeded = 0;
let successfulInsertations = 0;

const elasticsearch = require('elasticsearch');
const esClient = new elasticsearch.Client({
    host: '127.0.0.1:9200',
    log: 'warning'
});

esClient.ping({
      requestTimeout: 30000,
    }, (error) => {
      if (error) {
        console.error('Elasticsearch cluster is down!');
      } else {
        console.log('ElasticSearch running at ' + host);
        setupElsticSearch().then(()=>{
            runBenchmark();
        }).catch(err => console.log(err));
      }
    });


function runBenchmark(){

    console.log('Test starts now!');

    let index = "data-" + es_index;
    let jsonData = generateBulkpackege(index, "data", sizeOfBulk)
    indexItems(jsonData).then(()=> {
        console.log("finished, summary:");
        console.log(successfulInsertations + " Datapoints inserted");
        console.log(timeNeeded + "ms needed");
        console.log(successfulInsertations / (timeNeeded/1000.0) + " Datapoints per second");
    });

    
}

function indexItems(jsonData){
    return new Promise((resolve,reject)=>{
            esClient.bulk({body: jsonData})
        .then(res => {
            numInserts++;
            let errorCount = 0;
            res.items.forEach(item => {
                if (item.index && item.index.error){
                    console.log(++errorCount, item.index.error);
                }
            });
            console.log("Time to index:" +  res.took);
            console.log('Successfully indexed %d out of %d items!', res.items.length - errorCount,res.items.length);
            timeNeeded += res.took;
            successfulInsertations += (res.items.length - errorCount);
            if(numInserts < repeatsOfInsertation){
                indexItems(jsonData).then(()=> resolve());
            }else{
                resolve();
            }
            

        })
        .catch(console.err);

    })


}




 function setupElsticSearch() {
     return new Promise((resolve, reject) =>{
         console.log("setup");
    esClient.indices.getTemplate({
          name: 'datasource_all'
        }).then(() => {            
          console.log("Mapping has allready been set!");
          resolve("Mapping has allready been set!");          
        }).catch(() => {
          esClient.indices.putTemplate({
            name: 'datasource_all',
            body: {
              "template": "data-*",
              "order": 1,
              "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 3
              },
              "mappings": {
                "_default_": {
                  "_all": {
                    "enabled": false
                  }
                },
                "data": {
                  "properties": {
                    "device": {
                      "type": "keyword"
                    },
                    "location": {
                      "type": "geo_point"
                    },
                    "timestamp": {
                      "type": "date"
                    },
                    "timestamp_record": {
                      "type": "date"
                    },
                    "license": {
                      "type": "text"
                    }
                  }
                }
              }
            }
          })
          .then(()=> {
              console.log("mapping successful set");
              resolve("mapping successful set");
            })
          .catch(()=> {
              console.log("error while setting the mapping");
              reject("error while setting the mapping");
            });
        });

     })
    
      
  }


function generateBulkpackege(index, type, count){
    let data = [];
    let bulkBody = [];

    let locations = [{
        "lat": 52.5239,
        "lon": 13.4573
    },{        
        "lat": 55.0239,
        "lon": 12.7573
    },{
        "lat": 48.8539,
        "lon": 10.4673
    },{
        "lat": 49.3539,
        "lon": 8.1296
    },{
        "lat": 53.03539,
        "lon": 8.7696
    }];
    let dat = new Date();
    dat.setHours(dat.getHours() + 10);
    console.log((datePlus(2).toISOString()));
    

    let bucket = count/locations.length;

    for(let i=0;i<count;i++){        
        data.push({
                    "source_id": "luftdaten_info",
                    "device": "141",
                    "timestamp": datePlus(i).toISOString(),
                    "location": locations[Math.floor(i/bucket)],
                    "license": "find out",
                    "sensors": {
                        "pressure": {
                            "sensor": "BME280",
                            "observation_value": 97740.48
                        },
                        "altitude": {
                            "sensor": "BME280",
                            "observation_value": null
                        },
                        "pressure_seallevel": {
                            "sensor": "BME280",
                            "observation_value": null
                        },
                        "temperature": {
                            "sensor": "BME280",
                            "observation_value": Number.parseFloat(((Math.sin((Math.PI / (count*1.35)) * i) + 1) * 10 +  10 + ((Math.sin((Math.PI / 180) * i) + 1) / 4) + Math.random()*0.25).toFixed(4))
                        },
                        "humidity": {
                            "sensor": "BME280",
                            "observation_value": 76.34
                        }
                    },
                    "extra": {
                        "location": "65"
                    }
                });

    }  

    data.forEach(item => {
        bulkBody.push({
            index: {
                _index: index,
                _type: type
            }
        });

        bulkBody.push(item);
    });

    return bulkBody;

      
}

function datePlus(plus) {
    let dat = new Date();
    dat.setHours(dat.getHours() + plus);
    return dat;
}