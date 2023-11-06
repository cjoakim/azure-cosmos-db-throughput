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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class App {

    public static final String BASEBALL_BATTERS_CSV_FILE = "../../data/seanhahman-baseballdatabank-2023.1/core/Batting.csv";
    private static Logger logger = LogManager.getLogger(App.class);

    /**
     * Entry point to the program.  It is driven by command-line args.
     */
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            logger.warn("cli arg: " + i + " --> " + args[i]);
        }
        String function = args[0];
        String throughputControlType = args[1];  // local or global
        Float[] percentages = {0.10f, 0.50f, 1.00f};

        switch (function) {
            case "load_cosmos_baseball_batters":
                // args 'load_cosmos_baseball_batters', 'local', 'dev', 'unittests', 'all'
                String type = args[1]; // local or global
                String dbname = args[2];
                String cname = args[3];
                String team = args[4];

                CosmosAsyncClient client = buildAsyncClient();
                CosmosAsyncDatabase database = client.getDatabase(dbname);
                CosmosAsyncContainer container = database.getContainer(cname);
                List<BaseballBatter> batters = readFilterBatters(team);

                for (int p = 0; p < percentages.length; p++) {
                    Float pct = percentages[p];
                    logger.warn("================================================================================");
                    logger.warn("Iteration " + p + " dbname: " + dbname + ", cname: " + cname + ", pct: " + pct + ", team: " + team + ", batters: " + batters.size());
                    if (type.equalsIgnoreCase("global")) {

                    } else {
                        loadCosmosLocalThroughput(client, container, pct, batters);
                    }
                    if (p < (percentages.length - 1)) {
                        long ms = 1000 * 60 * 2;  // 2-minutes
                        logger.warn("sleeping between iterations for " + ms + " ms");
                        try {
                            Thread.sleep(ms);
                            logger.warn("awake after sleep");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                break;
            default:
                logger.error("undefined command-line function: " + function);
        }
    }

    private static void loadCosmosLocalThroughput(
            CosmosAsyncClient client, CosmosAsyncContainer container, Float pct, List<BaseballBatter> batters) {

        logger.warn("loadCosmosLocalThroughput - pct: " + pct);

        ThroughputControlGroupConfig groupConfig =
                new ThroughputControlGroupConfigBuilder()
                        .setGroupName("local1")
                        .setTargetThroughputThreshold(pct)
                        .build();
        container.enableLocalThroughputControlGroup(groupConfig);

        List<CosmosItemOperation> operations = buildBatterBulkUpsertOperations(batters);
        executeBulkOperations(operations, container);
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
    private static long executeBulkOperations(List<CosmosItemOperation> operations, CosmosAsyncContainer container) {
        logger.warn("starting executeBulkOperations, operation count: " + operations.size());
        long start = System.currentTimeMillis();
        container.executeBulkOperations(Flux.fromIterable(operations)).blockLast();
        long finish = System.currentTimeMillis();
        long elapsed = finish - start;
        logger.warn("completed executeBulkOperations in " + elapsed);
        return elapsed;
    }

    // ========== Environment and IO methods below, Cosmos DB above ==========

    private static String getEnvVar(String name) {
        return System.getenv(name);
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
}

//// https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-java?tabs=passwordlesssync%2Csign-in-azure-cli%2Casync-throughput#use-throughput-control
//// The SDK will use the lower of the given targetThroughputThreshold and targetThroughput values.
//        if (false) {
//                ThroughputControlGroupConfig groupConfig =
//                new ThroughputControlGroupConfigBuilder()
//                .groupName("global")
//                .targetThroughputThreshold(Float.parseFloat(pct))
//                .targetThroughput(100)
//                .build();
//
//                GlobalThroughputControlConfig globalControlConfig =
//                client.createGlobalThroughputControlConfigBuilder("ThroughputControlDatabase", "ThroughputControl")
//                .setControlItemRenewInterval(Duration.ofSeconds(5))
//                .setControlItemExpireInterval(Duration.ofSeconds(11))
//                .build();
//                }

// Create the ThroughputControl container if necessary
//        CosmosContainerProperties throughputContainerProperties =
//                new CosmosContainerProperties("ThroughputControl", "/groupId")
//                        .setDefaultTimeToLiveInSeconds(-1);
//        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
//        database.createContainerIfNotExists(throughputContainerProperties, throughputProperties).block();
