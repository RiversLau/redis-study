package com.zhaoxiang.redis.redis_in_action.chapter05;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Author: Rivers
 * Date: 2017/8/10 06:13
 */
public class LookUpIP {

    public static void main(String[] args) {

        new LookUpIP().execute();
    }

    public void execute() {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        testLookUp(conn);
    }

    public void testLookUp(Jedis conn) {

        String userDir = System.getProperty("user.dir");
        System.out.println(userDir);

        File blocks = new File(userDir + "/src/main/resources/GeoLiteCity-Blocks.csv");
        File locations = new File(userDir + "/src/main/resources/GeoLiteCity-Location.csv");

        if (!blocks.exists()) {
            return;
        }

        if (!locations.exists()) {
            return;
        }

        importIP2Redis(conn, blocks);
    }

    public void importIP2Redis(Jedis conn, File file) {

        FileReader fileReader = null;
        try {
            fileReader = new FileReader(file);
            CSVParser parser = new CSVParser(fileReader, CSVFormat.DEFAULT);
            int count = 0;
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                String name = record.get(0);
                System.out.println(name);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
