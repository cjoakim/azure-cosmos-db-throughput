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

    public static final String BASEBALL_BATTERS_CSV_FILE = "../../data/seanhahman-baseballdatabank-2023.1/core/Batting.csv";
    public static final long MS_PER_MINUTE = 1000 * 60;
    public static final long MINUTES = 1;
    public static final long SLEEP_MS = MS_PER_MINUTE * MINUTES;

    private static Logger logger = LogManager.getLogger(App.class);

    /**
     * Entry point to the program.  It is driven by command-line args; see build.gradle.
     */
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            logger.warn("cli arg: " + i + " --> " + args[i]);
        }
        String function = args[0];
        //Float[] percentages = { 0.15f, 0.30f, 0.45f, 0.60f };
        int[]   ruValues    = { 100, 200, 400, 1200 };

        switch (function) {
            case "gmt_time_generated_kql":
                Date now = new Date();
                logger.warn("kql: " + gmtTimeGeneratedKql(now, now));
                break;
            case "load_cosmos_baseball_batters":
                String type = args[1]; // local or global
                String dbname = args[2];
                String cname = args[3];
                String team = args[4];
                int batchSize = Integer.parseInt(args[5]);

                CosmosAsyncClient client = buildAsyncClient();
                CosmosAsyncDatabase database = client.getDatabase(dbname);
                CosmosAsyncContainer container = database.getContainer(cname);
                List<BaseballBatter> batters = readFilterBatters(team);

                for (int r = 0; r < ruValues.length; r++) {
                    sleep(SLEEP_MS); // sleep for 2-minutes between iterations or previous test
                    int rus = ruValues[r];
                    logger.warn("================================================================================");
                    logger.warn("Iteration " + r + " dbname: " + dbname + ", cname: " + cname + ", rus: " + rus + ", team: " + team + ", batters: " + batters.size() + ", batchSize: " + batchSize);
                    if (type.equalsIgnoreCase("global")) {
                        loadCosmosGlobalThroughput(client, dbname, container, rus, batters, batchSize);
                    }
                    else {
                        loadCosmosLocalThroughput(client, container, rus, batters, batchSize);
                    }
                }
                break;
            default:
                logger.error("undefined command-line function: " + function);
        }
    }

    private static void loadCosmosLocalThroughput(
            CosmosAsyncClient client, CosmosAsyncContainer container, int rus, List<BaseballBatter> batters, int batchSize) {

        String groupName = "local"; // + System.currentTimeMillis();
        logger.warn("loadCosmosLocalThroughput - target rus: " + rus + ", groupName: " + groupName);

        ThroughputControlGroupConfig groupConfig =
                new ThroughputControlGroupConfigBuilder()
                        .setGroupName(groupName)
                        .setTargetThroughput(rus)
                        .build();
                        // .setTargetThroughputThreshold(pct)
        container.enableLocalThroughputControlGroup(groupConfig);

        List<CosmosItemOperation> operations = buildBatterBulkUpsertOperations(batters);
        executeBulkOperations(operations, container, batchSize);
    }

    private static void loadCosmosGlobalThroughput(
            CosmosAsyncClient client, String dbname, CosmosAsyncContainer container, int rus, List<BaseballBatter> batters, int batchSize) {

        String groupName = "global1";
        String globalContainer = "GlobalThoughPutController";
        logger.warn("loadCosmosGlobalThroughput - rus: " + rus + ", groupName: " + groupName);

        // Create the GlobalThoughPutController container if necessary
        CosmosContainerProperties throughputContainerProperties =
                new CosmosContainerProperties(globalContainer, "/groupId")
                        .setDefaultTimeToLiveInSeconds(-1);
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(4000);
        client.getDatabase(dbname).createContainerIfNotExists(throughputContainerProperties, throughputProperties).block();

        ThroughputControlGroupConfig groupGlobalConfig =
                new ThroughputControlGroupConfigBuilder()
                        .groupName(groupName)
                        .setTargetThroughput(rus)
                        .build();
        //                         .targetThroughputThreshold(pct)

        CosmosAsyncDatabase database = client.getDatabase(dbname);

        GlobalThroughputControlConfig globalControlConfig =
                client.createGlobalThroughputControlConfigBuilder(database.getId(), globalContainer)
                        .setControlItemRenewInterval(Duration.ofSeconds(5))
                        .setControlItemExpireInterval(Duration.ofSeconds(20))
                        .build();
        container.enableGlobalThroughputControlGroup(groupGlobalConfig, globalControlConfig);

        List<CosmosItemOperation> operations = buildBatterBulkUpsertOperations(batters);
        executeBulkOperations(operations, container, batchSize);
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
            }
            else {
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
        return elapsed;
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

    // ========== Environment and IO methods below, Cosmos DB above ==========

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
