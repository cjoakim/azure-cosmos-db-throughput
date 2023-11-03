package org.cjoakim.cosmos.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * This class implements common File IO operations.
 * Chris Joakim, Microsoft, 2023
 */

public class FileUtil {

    // Class variables
    private static Logger logger = LogManager.getLogger(FileUtil.class);

    public FileUtil() {
        super();
    }

    public boolean isDirectory(String name) {
        File dirOrFile = new File(name);
        return dirOrFile.isDirectory();
    }

    public boolean isFile(String name) {
        File dirOrFile = new File(name);
        return dirOrFile.isFile();
    }

    public ArrayList<String> listFiles(String directory, String pattern) {
        ArrayList<String> filteredFilenames = new ArrayList<String>();
        File dir = new File(directory);
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                if (f.isFile()) {
                    if (pattern == null) {
                        filteredFilenames.add(f.getPath());
                    }
                    else {
                        if (f.getName().contains(pattern)) {
                            filteredFilenames.add(f.getPath());
                        }
                    }
                }
            }

        }
        return filteredFilenames;
    }

    public List<String> readLines(String infile) throws IOException {
        List<String> lines = new ArrayList<String>();
        File file = new File(infile);
        Scanner sc = new Scanner(file);
        while (sc.hasNextLine()) {
            lines.add(sc.nextLine().trim());
        }
        return lines;
    }

    public Map<String, Object> readJsonMap(String infile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(Paths.get(infile).toFile(), Map.class);
    }

    public ArrayList<Map<String, Object>> readJsonArray(String infile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(Paths.get(infile).toFile(), ArrayList.class);
    }

    public boolean deleteFile(String filename) {
        File dirOrFile = new File(filename);
        if (dirOrFile.isFile()) {
            return dirOrFile.delete();
        }
        else {
            return false;
        }
    }

    public void writeJson(Object obj, String outfile, boolean pretty, boolean verbose) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        if (pretty) {
            json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
            writeTextFile(outfile, json, verbose);
        }
        else {
            json = mapper.writeValueAsString(obj);
            writeTextFile(outfile, json, verbose);
            if (verbose) {
                logger.warn("file written: " + outfile);
            }
        }
    }

    public void writeTextFile(String outfile, String text, boolean verbose) throws Exception {
        FileWriter fw = null;
        try {
            fw = new FileWriter(outfile);
            fw.write(text);
            if (verbose) {
                logger.warn("file written: " + outfile);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        finally {
            if (fw != null) {
                fw.close();
            }
        }
    }

    public String baseName(File f) {
        return f.getName();
    }

    public String baseNameNoSuffix(File f) {
        return baseName(f).split("\\.")[0];
    }

    public String immediateParentDir(File f) {
        return new File(f.getParent()).getName().toString();
    }
}
