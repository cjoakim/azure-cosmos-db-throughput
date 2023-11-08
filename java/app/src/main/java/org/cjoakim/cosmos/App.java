package org.cjoakim.cosmos;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cjoakim.cosmos.model.BaseballBatter;
import org.cjoakim.cosmos.util.FileUtil;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class App {
    private static final String FLAG_TYPE_LOCAL = "--local";
    private static final String FLAG_TYPE_GLOBAL = "--global";
    private static final String FLAG_TYPE_PRIORITY = "--priority";
    private static final String FLAG_PRIORITY_LOW = "--priority-low";
    private static final String FLAG_PRIORITY_HIGH = "--priority-high";
    private static final String FLAG_REQUEST_UNITS = "--ru";
    private static final String FLAG_PERCENT = "--pct";

    private static final long ONE_MINUTE = 1000 * 60;

    private static final String GLOBAL_CONTAINER = "GlobalThoughputController";
    private static final String BASEBALL_BATTERS_CSV_FILE = "../../data/seanhahman-baseballdatabank-2023.1/core/Batting.csv";
    private static String[] commandLineArgs = null;

    private static Logger logger = LogManager.getLogger(App.class);

    /**
     * Entry point to the program.  It is driven by command-line args; see build.gradle.
     */
    public static void main(String[] args) {

        setCommandLineArgs(args);
        String function = args[0];

        switch (function) {
            case "throughput_test":
                String type   = getTestType(args[1]);
                String dbname = args[2];
                String cname  = args[3];
                String team   = args[4];
                int batchSize = Integer.parseInt(args[5]);

                CosmosAsyncClient client = buildAsyncClient();
                CosmosAsyncDatabase database = client.getDatabase(dbname);
                CosmosAsyncContainer container = database.getContainer(cname);
                List<BaseballBatter> batters = readFilterBatters(team);

                if (type.equalsIgnoreCase(FLAG_TYPE_GLOBAL)) {
                    createGlobalThroughputContainer(client, dbname);
                    loadCosmosGlobalThroughput(client, dbname, container, batters, batchSize);
                } else {
                    loadCosmosNonGlobalThroughput(container, batters, batchSize);
                }
                break;
            case "gmt_time_generated_kql":
                // This case is just for ad-hoc development and testing of method gmtTimeGeneratedKql
                Date now = new Date();
                logger.warn("kql: " + gmtTimeGeneratedKql(now, now));
                break;
            default:
                logger.error("undefined command-line function: " + function);
        }
    }

    private static void loadCosmosNonGlobalThroughput(CosmosAsyncContainer container, List<BaseballBatter> batters, int batchSize) {

        ThroughputControlGroupConfig groupConfig = buildThroughputControlGroupConfig();
        container.enableLocalThroughputControlGroup(groupConfig);

        List<CosmosItemOperation> operations = buildBatterBulkUpsertOperations(batters);
        executeBulkOperations(operations, container, batchSize);
    }

    private static void loadCosmosGlobalThroughput(
            CosmosAsyncClient client, String dbname, CosmosAsyncContainer container, List<BaseballBatter> batters, int batchSize) {

        CosmosAsyncDatabase database = client.getDatabase(dbname);

        ThroughputControlGroupConfig groupConfig = buildThroughputControlGroupConfig();

        GlobalThroughputControlConfig globalControlConfig =
                client.createGlobalThroughputControlConfigBuilder(database.getId(), GLOBAL_CONTAINER)
                        .setControlItemRenewInterval(Duration.ofSeconds(5))
                        .setControlItemExpireInterval(Duration.ofSeconds(20))
                        .build();
        container.enableGlobalThroughputControlGroup(groupConfig, globalControlConfig);

        List<CosmosItemOperation> operations = buildBatterBulkUpsertOperations(batters);
        executeBulkOperations(operations, container, batchSize);
    }

    /**
     * Create the GlobalThoughPutController container if necessary.
     */
    private static void createGlobalThroughputContainer(CosmosAsyncClient client, String dbname) {

        logger.warn("createGlobalThroughputContainer ...");
        CosmosContainerProperties throughputContainerProperties =
                new CosmosContainerProperties(GLOBAL_CONTAINER, "/groupId")
                        .setDefaultTimeToLiveInSeconds(-1);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(4000);
        client.getDatabase(dbname).createContainerIfNotExists(throughputContainerProperties, throughputProperties).block();
    }

    private static CosmosAsyncClient buildAsyncClient() {
        String uri = getEnvVar("AZURE_COSMOSDB_NOSQL_URI");
        String key = getEnvVar("AZURE_COSMOSDB_NOSQL_RW_KEY1");
        String[] regions = getEnvVar("AZURE_COSMOSDB_NOSQL_SERVERLESS_PREF_REGIONS").split("[,]", 0);

        return new CosmosClientBuilder()
                .endpoint(uri)
                .key(key)
                .preferredRegions(Arrays.asList(regions))
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();
    }

    private static List<CosmosItemOperation> buildBatterBulkUpsertOperations(List<BaseballBatter> batters) {
        List<CosmosItemOperation> operations = new ArrayList<>();
        for (int i = 0; i < batters.size(); i++) {
            BaseballBatter bb = batters.get(i);
            operations.add(CosmosBulkOperations.getUpsertItemOperation(bb, new PartitionKey(bb.getPk())));
        }
        return operations;
    }

    /**
     * Execute the given bulk operations on the given container.  Return the elapsed MS.
     */
    private static long executeBulkOperations(List<CosmosItemOperation> allOperations, CosmosAsyncContainer container, int batchSize) {
        logger.warn("starting executeBulkOperations, operation count: " + allOperations.size());
        long start = System.currentTimeMillis();
        Date startDate = new Date();
        boolean continueToProcess = true;
        int batchIndex = -1;

        while (continueToProcess) {
            // Execute the bulk load in smaller batches so as to take advantage of throughput control
            batchIndex++;
            ArrayList<CosmosItemOperation> nextBatch = getNextBatch(allOperations, batchSize, batchIndex);

            if (nextBatch.size() < 1) {
                continueToProcess = false;
            } else {
                logger.warn("executeBulkOperations - executing batchIndex: " + batchIndex + " with " + nextBatch.size() + " operations");
                CosmosBulkExecutionOptions opts = new CosmosBulkExecutionOptions();
                opts.getThresholdsState();
                CosmosBulkOperationResponse<Object> resp =
                        container.executeBulkOperations(Flux.fromIterable(nextBatch), opts).blockLast();
            }
            if (batchIndex > 10000) {
                continueToProcess = false; // terminate a runaway loop
            }
        }
        long finish = System.currentTimeMillis();
        Date finishDate = new Date();
        long elapsed = finish - start;
        logger.warn("completed executeBulkOperations in " + elapsed);
        logger.warn("kql: " + gmtTimeGeneratedKql(startDate, finishDate));
        logCommandLineArgs();
        return elapsed;
    }

    /**
     * Build and return a ThroughputControlGroupConfig per command-line arguments.
     */
    private static ThroughputControlGroupConfig buildThroughputControlGroupConfig() {

        // set these per the command-line args in order to create the appropriate ThroughputControlGroupConfig
        String groupName = "default";
        int requestUnits = 0;
        float percentOfAvailableRU = 0;
        boolean priorityLow = false;
        boolean priorityHigh = false;

        // First scan the command-line args and set the above local variables
        for (int i = 0; i < commandLineArgs.length; i++) {
            String arg = commandLineArgs[i];
            try {
                switch (arg) {
                    case FLAG_PRIORITY_LOW:
                        priorityLow = true;
                        groupName = "low";
                        break;
                    case FLAG_PRIORITY_HIGH:
                        priorityHigh = true;
                        groupName = "high";
                        break;
                    case FLAG_REQUEST_UNITS:
                        requestUnits = Integer.parseInt(commandLineArgs[i + 1]);
                        groupName = "rus";
                        break;
                    case FLAG_PERCENT:
                        percentOfAvailableRU = Float.parseFloat(commandLineArgs[i + 1]);
                        groupName = "pct";
                        break;
                }
            } catch (NumberFormatException e) {
                logger.error("buildThroughputControlGroupConfig - error processing arg: " + arg);
            }
        }

        // Next, create and return the appropriate ThroughputControlGroupConfig based on the command-line args

        if (priorityLow) {
            logger.error("buildThroughputControlGroupConfig - creating low priority instance");
            return new ThroughputControlGroupConfigBuilder()
                    .groupName(groupName)
                    .priorityLevel(PriorityLevel.LOW)
                    .build();
        }
        if (priorityHigh) {
            logger.error("buildThroughputControlGroupConfig - creating high priority instance");
            return new ThroughputControlGroupConfigBuilder()
                    .groupName(groupName)
                    .priorityLevel(PriorityLevel.HIGH)
                    .build();
        }
        if (requestUnits > 0) {
            logger.error("buildThroughputControlGroupConfig - creating request units instance: " + requestUnits);
            return new ThroughputControlGroupConfigBuilder()
                    .setGroupName(groupName)
                    .setTargetThroughput(requestUnits)
                    .build();
        }
        if (percentOfAvailableRU > 0) {
            logger.error("buildThroughputControlGroupConfig - creating percent of available RU instance: " + percentOfAvailableRU);
            return new ThroughputControlGroupConfigBuilder()
                    .setGroupName(groupName)
                    .setTargetThroughputThreshold(percentOfAvailableRU)
                    .build();
        }

        logger.error("buildThroughputControlGroupConfig - defaulting to low priority instance");
        return new ThroughputControlGroupConfigBuilder()
                .groupName("low")
                .priorityLevel(PriorityLevel.LOW)
                .build();
    }

    private static ArrayList<CosmosItemOperation> getNextBatch(List<CosmosItemOperation> allOperations, int batchSize, int batchIndex) {
        ArrayList<CosmosItemOperation> thisBatch = new ArrayList<CosmosItemOperation>();
        int minIndex = batchIndex * batchSize;
        int maxIndex = minIndex + batchSize;
        // TODO - check this logic for off-by-one errors
        for (int i = 0; i < allOperations.size(); i++) {
            if (i >= minIndex) {
                if (i < maxIndex) {
                    thisBatch.add(allOperations.get(i));
                }
            }
        }
        return thisBatch;
    }

    // ========== Cosmos DB methods above, others below ==========

    private static void setCommandLineArgs(String[] args) {
        commandLineArgs = args;
        logCommandLineArgs();
    }

    private static void logCommandLineArgs() {
        for (int i = 0; i < commandLineArgs.length; i++) {
            logger.warn("cli arg: " + i + " --> " + commandLineArgs[i]);
        }
    }

    private static String getTestType(String arg) {
        if (arg.equalsIgnoreCase(FLAG_TYPE_LOCAL)) {
            return FLAG_TYPE_LOCAL;
        } else if (arg.equalsIgnoreCase(FLAG_TYPE_GLOBAL)) {
            return FLAG_TYPE_GLOBAL;
        } else if (arg.equalsIgnoreCase(FLAG_TYPE_PRIORITY)) {
            return FLAG_TYPE_PRIORITY;
        }
        return null;
    }

    private static String getEnvVar(String name) {
        return System.getenv(name);
    }

    private static void sleep(long ms) {
        logger.warn("sleeping for " + ms + " ms");
        try {
            Thread.sleep(ms);
            logger.warn("awake after sleep");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<BaseballBatter> readFilterBatters(String team) {
        List<BaseballBatter> batters = readBaseballBatters();
        return filterBatters(batters, team);
    }

    /**
     * Read the Batting.csv file in this repo and return a corresponding List of BaseballBatter objects.
     */
    private static List<BaseballBatter> readBaseballBatters() {
        ObjectMapper mapper = new ObjectMapper();
        List<BaseballBatter> batters = new ArrayList<BaseballBatter>();
        try {
            FileUtil fu = new FileUtil();
            List<String> lines = fu.readLines(BASEBALL_BATTERS_CSV_FILE);
            logger.warn("input file read; lines: " + lines.size());
            String[] headerFields = null;

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (i == 0) {
                    headerFields = line.split("[,]", 0);
                    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(headerFields));
                    // [ "playerID", "yearID", "stint", "teamID", "lgID", "G", "AB", "R", "H", "2B", "3B", "HR", "RBI", "SB", "CS", "BB", "SO", "IBB", "HBP", "SH", "SF", "GIDP" ]
                } else {
                    BaseballBatter bb = new BaseballBatter(headerFields, line);
                    if (bb.isValid()) {
                        bb.setPk("mlb");  // <-- for hot-partition test
                        batters.add(bb);
                        if (i < 4) {
                            //logger.warn("csv line: " + line);
                            //System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(bb));
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.fatal("unable to read input file " + BASEBALL_BATTERS_CSV_FILE);
        }
        logger.warn("batters read: " + batters.size());
        return batters;
    }

    /**
     * Filter the list of Batters.  Debut year > 1950 with a mininum number of games.
     */
    private static List<BaseballBatter> filterBatters(List<BaseballBatter> batters, String team) {
        logger.warn("filterBatters input size: " + batters.size());
        List<BaseballBatter> filtered = new ArrayList<BaseballBatter>();
        for (int i = 0; i < batters.size(); i++) {
            BaseballBatter bb = batters.get(i);
            if ((team.equalsIgnoreCase("all") || (team.equalsIgnoreCase(bb.getTeamID())))) {
                if (bb.getYear() >= 1950) {
                    if (bb.getGames() > 10) {
                        filtered.add(bb);
                    }
                }
            }
        }
        logger.warn("filterBatters output size: " + filtered.size());
        return filtered;
    }

    private static String gmtTimeGeneratedKql(Date startDate, Date finishDate) {
        SimpleDateFormat sdf =
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

        // | where TimeGenerated between (datetime("2023-11-07 12:00:00")..datetime("2023-11-07 20:00:00"))
        StringBuffer sb = new StringBuffer();
        sb.append("| where TimeGenerated between (datetime(\"");
        sb.append(sdf.format(startDate));
        sb.append("\")..datetime(\"");
        sb.append(sdf.format(finishDate));
        sb.append("\"))");
        return sb.toString();
    }
}
