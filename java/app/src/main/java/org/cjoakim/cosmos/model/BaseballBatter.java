package org.cjoakim.cosmos.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

@NoArgsConstructor
@Data
public class BaseballBatter {

    private static Logger logger = LogManager.getLogger(BaseballBatter.class);

    // Instance variables - from the CSV file
    String id;
    String pk;
    String playerID;
    int    year;
    String stint;
    String teamID;
    String leagueID;
    int    games = -1;
    int    atBats = -1;
    int    runs = -1;
    int    hits = -1;
    int    doubles = -1;
    int    triples = -1;
    int    homeRuns = -1;
    int    rbi = -1;
    int    stolenBases = -1;
    int    caughtStealing = -1;
    int    baseOnBalls = -1;
    int    strikeouts = -1;
    int    intentionalBB = -1;
    int    hitByPitch = -1;
    boolean exception;

    public BaseballBatter(String[] fields, String csvLine) {
        super();
        String[] values = csvLine.split("[,]", 0);
        if (fields.length == values.length) {
            for (int i = 0; i < values.length; i++) {
                String value = values[i].strip();
                try {
                    switch (i) {
                        case 0:
                            setId(UUID.randomUUID().toString());
                            setPlayerID(value);
                            setPk(value);
                            break;
                        case 1:
                            setYear(Integer.parseInt(value));
                            break;
                        case 2:
                            setStint(value);
                            break;
                        case 3:
                            setTeamID(value);
                            break;
                        case 4:
                            setLeagueID(value);
                            break;
                        case 5:
                            setGames(Integer.parseInt(value));
                            break;
                        case 6:
                            setAtBats(Integer.parseInt(value));
                            break;
                        case 7:
                            setRuns(Integer.parseInt(value));
                            break;
                        case 8:
                            setHits(Integer.parseInt(value));
                            break;
                        case 9:
                            setDoubles(Integer.parseInt(value));
                            break;
                        case 10:
                            setTriples(Integer.parseInt(value));
                            break;
                        case 11:
                            setHomeRuns(Integer.parseInt(value));
                            break;
                        case 12:
                            setRbi(Integer.parseInt(value));
                            break;
                        case 13:
                            setStolenBases(Integer.parseInt(value));
                            break;
                        case 14:
                            try {
                                setCaughtStealing(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                setCaughtStealing(0);
                            }
                            break;
                        case 15:
                            try {
                                setBaseOnBalls(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                setBaseOnBalls(0);
                            }
                            break;
                        case 16:
                            try {
                                setStrikeouts(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                setStrikeouts(0);
                            }
                            break;
                        case 17:
                            try {
                                setIntentionalBB(Integer.parseInt(value));
                            }
                            catch (NumberFormatException e) {
                                setIntentionalBB(0);
                            }
                            break;
                        case 18:
                            try {
                                setHitByPitch(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                setHitByPitch(0);
                            }
                            break;
                    }
                } catch (Exception e) {
                    setException(true);
                    logger.error("error on csv line index " + i + " value: <" + value + ">");
                }
            }
        }
    }

    public boolean isValid() {

        return games > 0;
    }
}
