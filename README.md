# RXJenerator

### Intro
This load generator is based on the 2.x Couchbase Java SDK.

### Instructions to build

Build using ```mvn package```

Run using ```java -jar target/RXJenerator-1.0.jar```

The default settings will:
* generate 50% writes and 50% reads
* by default it will connect to ```localhost``` and use the ```default``` bucket
* You can force back pressure exceptions if you want using ```-forcebackpressure```


### TODO 
* Defining the ration of reads and writes
* Defining your own data sources




