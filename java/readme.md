# azure-cosmos-db-throughput : java

Code samples related to Cosmos DB Throughput and Throughput Control

## Links

- https://devblogs.microsoft.com/cosmosdb/latest-nosql-java-ecosystem-updates-2023-q1-q2/
  - https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-java?tabs=passwordlesssync%2Csign-in-azure-cli%2Csync-throughput#use-throughput-control
  - https://devblogs.microsoft.com/cosmosdb/introducing-priority-based-execution-in-azure-cosmos-db-preview/
  - https://github.com/Azure-Samples/azure-cosmos-java-sql-api-samples/blob/da39bf4e57c6d814c478d10eed5c2905a764d667/src/main/java/com/azure/cosmos/examples/throughputcontrol/async/ThroughputControlQuickstartAsync.java#L233

- https://devblogs.microsoft.com/cosmosdb/azure-cosmos-db-java-ecosystem/

---

## Logging

- https://learn.microsoft.com/en-us/azure/cosmos-db/monitor-resource-logs?tabs=azure-portal
- https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/diagnostic-queries?tabs=resource-specific
- https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/kql-quick-reference
- https://learn.microsoft.com/en-us/azure/cosmos-db/insights-overview

### Normalized RU Consumption Metric

- https://learn.microsoft.com/en-us/azure/cosmos-db/monitor-normalized-request-units

> The Normalized RU Consumption metric is a metric between 0% to 100% that is used to help
> measure the utilization of provisioned throughput on a database or container. 
> The metric is emitted at 1 minute intervals and is defined as the maximum RU/s utilization 
> across all partition key ranges in the time interval.

### CDBPartitionKeyRUConsumption

#### By PartitionKey 

```
CDBPartitionKeyRUConsumption
| where DatabaseName == "dev" and CollectionName == "test"
| where TimeGenerated between (datetime("2023-11-07 12:00:00")..datetime("2023-11-07 20:00:00"))
| summarize max(RequestCharge) by bin(TimeGenerated, 1s), PartitionKey
| order by TimeGenerated
```

#### By PartitionKeyRangeId

```
CDBPartitionKeyRUConsumption
| where DatabaseName == "dev" and CollectionName == "local"
| where TimeGenerated between (datetime("2023-11-07 12:00:00")..datetime("2023-11-07 20:00:00"))
| summarize max(RequestCharge) by bin(TimeGenerated, 1s), 
| order by TimeGenerated
```

3:06pm  2023-11-07T20:09:21Z

GMT 8:16pm = 3:16pm EST as of 11/7 (5-hour diff)

```
15:09:18.306 [main] WARN  App - ================================================================================
15:09:18.313 [main] WARN  App - Iteration 0 dbname: dev, cname: test, pct: 0.15, team: all, batters: 61664
15:09:18.313 [main] WARN  App - loadCosmosLocalThroughput - pct: 0.15, groupName: local1699387758313
15:09:18.344 [main] WARN  App - starting executeBulkOperations, operation count: 61664
15:09:20.295 [cosmos-rntbd-nio-2-2] INFO  SessionContainer - Registering a new collection resourceId [gm8hANAgVj0=] in SessionTokens
15:11:17.402 [cosmos-parallel-2] INFO  RxDocumentClientImpl - Getting database account endpoint from https://gbbcjcdbnosql.documents.azure.com:443/
15:12:35.214 [bulk-executor-bounded-elastic-6] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-1[n/a]
15:12:35.216 [bulk-executor-bounded-elastic-7] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-1[n/a]
15:12:35.216 [bulk-executor-bounded-elastic-7] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-1[n/a]
15:12:35.216 [main] WARN  App - completed executeBulkOperations in 196872
15:12:35.216 [main] WARN  App - sleeping for 180000 ms
15:15:35.226 [main] WARN  App - awake after sleep

CDBPartitionKeyRUConsumption
| where DatabaseName == "dev" and CollectionName == "test"
| where TimeGenerated between (datetime("2023-11-07 20:09:18")..datetime("2023-11-07 20:12:35"))
| summarize max(RequestCharge) by bin(TimeGenerated, 1s), PartitionKey
| order by TimeGenerated asc


15:15:35.226 [main] WARN  App - ================================================================================
15:15:35.226 [main] WARN  App - Iteration 1 dbname: dev, cname: test, pct: 0.3, team: all, batters: 61664
15:15:35.226 [main] WARN  App - loadCosmosLocalThroughput - pct: 0.3, groupName: local1699388135226
15:15:35.238 [main] WARN  App - starting executeBulkOperations, operation count: 61664
15:16:17.665 [cosmos-parallel-28] INFO  RxDocumentClientImpl - Getting database account endpoint from https://gbbcjcdbnosql.documents.azure.com:443/
15:18:08.197 [bulk-executor-bounded-elastic-13] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-2[n/a]
15:18:08.197 [bulk-executor-bounded-elastic-14] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-2[n/a]
15:18:08.197 [bulk-executor-bounded-elastic-14] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-2[n/a]
15:18:08.197 [main] WARN  App - completed executeBulkOperations in 152959
15:18:08.197 [main] WARN  App - sleeping for 180000 ms
15:21:08.208 [main] WARN  App - awake after sleep

CDBPartitionKeyRUConsumption
| where DatabaseName == "dev" and CollectionName == "test"
| where TimeGenerated between (datetime("2023-11-07 20:15:35")..datetime("2023-11-07 20:18:08"))
| summarize max(RequestCharge) by bin(TimeGenerated, 1s), PartitionKey
| order by TimeGenerated asc


15:21:08.208 [main] WARN  App - ================================================================================
15:21:08.208 [main] WARN  App - Iteration 2 dbname: dev, cname: test, pct: 0.45, team: all, batters: 61664
15:21:08.208 [main] WARN  App - loadCosmosLocalThroughput - pct: 0.45, groupName: local1699388468208
15:21:08.215 [main] WARN  App - starting executeBulkOperations, operation count: 61664
15:21:17.741 [cosmos-parallel-7] INFO  RxDocumentClientImpl - Getting database account endpoint from https://gbbcjcdbnosql.documents.azure.com:443/
15:23:40.960 [bulk-executor-bounded-elastic-20] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-3[n/a]
15:23:40.961 [bulk-executor-bounded-elastic-21] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-3[n/a]
15:23:40.961 [bulk-executor-bounded-elastic-21] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-3[n/a]
15:23:40.961 [main] WARN  App - completed executeBulkOperations in 152746
15:23:40.961 [main] WARN  App - sleeping for 180000 ms
15:26:17.781 [cosmos-parallel-2] INFO  RxDocumentClientImpl - Getting database account endpoint from https://gbbcjcdbnosql.documents.azure.com:443/
15:26:40.968 [main] WARN  App - awake after sleep


15:26:40.968 [main] WARN  App - ================================================================================
15:26:40.968 [main] WARN  App - Iteration 3 dbname: dev, cname: test, pct: 0.6, team: all, batters: 61664
15:26:40.968 [main] WARN  App - loadCosmosLocalThroughput - pct: 0.6, groupName: local1699388800968
15:26:40.989 [main] WARN  App - starting executeBulkOperations, operation count: 61664
15:29:13.136 [bulk-executor-bounded-elastic-27] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-4[n/a]
15:29:13.136 [bulk-executor-bounded-elastic-28] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-4[n/a]
15:29:13.136 [bulk-executor-bounded-elastic-28] INFO  BulkExecutor - Closing all sinks, Context: BulkExecutor-4[n/a]
15:29:13.136 [main] WARN  App - completed executeBulkOperations in 152147
```


```
CDBPartitionKeyRUConsumption
| where DatabaseName == "dev" and CollectionName == "test"
| where TimeGenerated between (datetime("2023-11-07 20:09:18")..datetime("2023-11-07 20:12:18"))
| summarize max(RequestCharge) by bin(TimeGenerated, 1s), PartitionKeyRangeId
| order by TimeGenerated
```

### Other KQL Queries

```
CDBPartitionKeyRUConsumption
| where TimeGenerated >= now(-30m)
| where DatabaseName == "dev" and CollectionName == "local"
| summarize sum(todouble(RequestCharge)) by toint(PartitionKeyRangeId)
| render columnchart
```

```
CDBPartitionKeyRUConsumption
| where TimeGenerated >= now(-30m)
| where DatabaseName == "dev" and CollectionName == "local"
| summarize sum(todouble(RequestCharge)) by PartitionKey, PartitionKeyRangeId
| render columnchart
```


```
CDBPartitionKeyStatistics
| project RegionName, DatabaseName, CollectionName, PartitionKey, SizeKb

CDBPartitionKeyStatistics
| where DatabaseName == "dev" and CollectionName == "test"
| project RegionName, DatabaseName, CollectionName, PartitionKey, SizeKb
```


