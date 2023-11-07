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
| where DatabaseName == "dev" and CollectionName == "local"
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

3:06pm

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
| where DatabaseName == "dev" and CollectionName == "local"
| project RegionName, DatabaseName, CollectionName, PartitionKey, SizeKb
```


