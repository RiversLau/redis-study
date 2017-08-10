package com.zhaoxiang.redis.redis_in_action.chapter05;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

/**
 * Author: Rivers
 * Date: 2017/8/8 22:24
 */
public class LogRecent {

    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARNING = "warning";
    public static final String ERROR = "error";
    public static final String CRITICAL = "critical";

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP =
            new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");
    private static final SimpleDateFormat ISO_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    static{
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static void main(String[] args) {

        new LogRecent().execute();
    }

    public void execute() {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        testLogRecent(conn);
        testLogCommon(conn);
    }

    public void testLogCommon(Jedis conn) {

        for (int i = 0; i < 6; i++) {
            for (int j = 0; j < i; j++) {
                logCommon(conn, "test", "message-" + i);
            }
        }
    }

    public void testLogRecent(Jedis conn) {

        for (int i = 0; i < 5; i++) {
            logRecent(conn, "test", "This is message " + i);
        }

        List<String> logs = conn.lrange("recent:test:info", 0, -1);
        for (String log : logs) {
            System.out.println(log);
        }
    }

    public void logCommon(Jedis conn, String name, String message) {

        logCommon(conn, name, message, INFO, 5000);
    }

    public void logCommon(Jedis conn, String name, String message, String severity, int timeout) {

        String commonDest = "common:" + name + ":" + severity;
        String startKey = commonDest + ":start";

        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());
            String existing = conn.get(startKey);

            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                trans.rename(commonDest, commonDest + ":last");
                trans.rename(startKey, commonDest + "pstart");
                trans.set(startKey, hourStart);
            }

            trans.zincrby(commonDest, 1, message);

            String recent = "recent:" + name + ":" + severity;
            trans.lpush(recent, TIMESTAMP.format(new Date()) + " " + message);
            trans.ltrim(recent, 0, 99);

            List<Object> result = trans.exec();
            if (result != null) {
                continue;
            }
            return;
        }
    }

    public void logRecent(Jedis conn, String name, String message) {
        logRecent(conn, name, message, INFO);
    }

    public void logRecent(Jedis conn, String name, String message, String severity) {

        String destination = "recent:" + name + ":" + severity;
        Pipeline pipeline = conn.pipelined();
        pipeline.lpush(destination, TIMESTAMP.format(new Date()) + " " + message);
        pipeline.ltrim(destination, 0 ,99);
        pipeline.sync();
    }
}
