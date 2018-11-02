# prestoeventlistener
Implementation to collect queryInfo in S3 using presto event listener.
1. Create a directory $PRESTO_HOME/plugin/event-listener and upload the presto-event-listener-1.0-SNAPSHOT.jar there
2. Create a new file at $PRESTO_HOME/etc/event-listener.properties with following details:
    
    event-listener.name=event-listener
    
    accessKey=** 
    
    secretKey=**
    
    bucket=<bucket-name>
    
    tableLocationKey=<location-in-bucket>
    
3. Restart presto server
4. Create a new table at s3 location using Hive:
    
    CREATE EXTERNAL TABLE `eventlistener`(
    
  `queryid` string, 
  
  `query` string, 
  
  `createtime` timestamp, 
  
  `start_time` timestamp, 
  
  `analysistime` bigint, 
  
  `executionstarttime` timestamp, 
  
  `endtime` timestamp, 
  
  `state` string, 
  
  `error` string, 
  
  `inputtables` string, 
  
  `inputcolumns` string, 
  
  `walltime` bigint, 
  
  `cputime` bigint, 
  
  `totalbytes` bigint, 
  
  `totalmemory` double, 
  
  `queryinfojson` string)

PARTITIONED BY (dt date, hr string)
  
ROW FORMAT DELIMITED 

  FIELDS TERMINATED BY '|' 
  
STORED AS INPUTFORMAT 

  'org.apache.hadoop.mapred.TextInputFormat' 
  
OUTPUTFORMAT 

  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  
LOCATION

  's3://\<bucket\>/\<location-in-bucket\>'

5.Recover partitions
    ALTER TABLE eventlistener RECOVER PARTITIONS;
