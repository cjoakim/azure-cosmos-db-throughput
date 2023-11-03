package org.cjoakim.cosmos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cjoakim.cosmos.model.BaseballBatter;
import org.cjoakim.cosmos.util.FileUtil;

import java.io.IOException;
import java.util.ArrayList;
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
        String db = args[1];
        String container = args[2];
        String pk = args[3];
        String pct = args[4];
        String team = args[5];
        List<BaseballBatter> batters = readBaseballBatters();

        if (batters.size() < 1) {
            logger.error("Batters list is empty");
            return;
        }
    }

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
                }
                else {
                    BaseballBatter bb = new BaseballBatter(headerFields, line);
                    if (bb.isValid()) {
                        batters.add(bb);
                        if (i < 10) {
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
}
