package org.cjoakim.cosmos;

import com.azure.cosmos.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cjoakim.cosmos.model.BaseballBatter;
import org.cjoakim.cosmos.util.FileUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

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

        switch(function) {
            case "load_cosmos_baseball_batters":
                loadCosmos(args);
                break;
            default:
                logger.error("undefined command-line function: " + function);
        }
    }

    private static void loadCosmos(String[] args) {
        String dbname = args[1];
        String cname = args[2];
        String pk = args[3];
        String pct = args[4];
        String team = args[5];
        List<BaseballBatter> batters = readBaseballBatters();

        if (batters.size() < 1) {
            logger.error("Batters list is empty");
            return;
        }
        String uri = getEnvVar("AZURE_COSMOSDB_NOSQL_URI");
        String key = getEnvVar("AZURE_COSMOSDB_NOSQL_RW_KEY1");
        String[] regions = getEnvVar("AZURE_COSMOSDB_NOSQL_SERVERLESS_PREF_REGIONS").split("[,]", 0);

        CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint(uri)
                .key(key)
                .preferredRegions(Arrays.asList(regions))
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();

        CosmosAsyncDatabase database = client.getDatabase(dbname);
        CosmosAsyncContainer container = database.getContainer(cname);
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
                }
                else {
                    BaseballBatter bb = new BaseballBatter(headerFields, line);
                    if (bb.isValid()) {
                        batters.add(bb);
                        if (i < 4) {
                            logger.warn("csv line: " + line);
                            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(bb));
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            logger.fatal("unable to read input file " + BASEBALL_BATTERS_CSV_FILE);
        }
        logger.warn("batters read: " + batters.size());
        return batters;
    }

    private static String getEnvVar(String name) {

        return System.getenv(name);
    }
}
