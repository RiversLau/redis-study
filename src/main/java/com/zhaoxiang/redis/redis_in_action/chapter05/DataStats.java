package com.zhaoxiang.redis.redis_in_action.chapter05;

import redis.clients.jedis.*;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author: Rivers
 * Date: 2017/8/9 20:50
 */
public class DataStats {

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP =
            new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");
    private static final SimpleDateFormat ISO_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    public static void main(String[] args) throws InterruptedException {

        new DataStats().execute();
    }

    public void execute() throws InterruptedException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        testStats(conn);
        testAccessTimer(conn);
    }

    public void testStats(Jedis conn) {

        List<Object> result = null;
        for (int i = 0; i < 5; i++) {
            double value = (Math.random() * 11) + 5;
            result = updateStats(conn, "temp", "example", value);
        }

        System.out.println(result);
        Map<String, Double> stats = getStats(conn, "temp", "example");
        System.out.println(stats);
    }

    public Map<String, Double> getStats(Jedis conn, String context, String type) {

        String key = "stats:" + context + ":" + type;
        Map<String, Double> result = new HashMap<String, Double>();
        Set<Tuple> tuples = conn.zrangeWithScores(key, 0, -1);
        for (Tuple t : tuples) {
            result.put(t.getElement(), t.getScore());
        }
        result.put("avg", result.get("sum") / result.get("count"));
        double numerator = result.get("sumsq") - Math.pow(result.get("sum"), 2) / result.get("count");
        double count = result.get("count");

        result.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), 5));
        return result;
    }

    public List<Object> updateStats(Jedis conn, String context, String type, Double value) {

        int timeout = 5000;
        String destination = "stats:" + context + ":" + type;
        String startKey = destination + ":start";

        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());

            String existing = conn.get(startKey);
            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                trans.rename(destination, destination + ":last");
                trans.rename(startKey, destination + ":pstart");
                trans.set(startKey, hourStart);
            }

            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            trans.zadd(tkey1, value, "min");
            trans.zadd(tkey2, value, "max");

            trans.zunionstore(destination, new ZParams().aggregate(ZParams.Aggregate.MIN), destination, tkey1);
            trans.zunionstore(destination, new ZParams().aggregate(ZParams.Aggregate.MAX), destination, tkey2);

            trans.del(tkey1, tkey2);
            trans.zincrby(destination, 1, "count");
            trans.zincrby(destination, value, "sum");
            trans.zincrby(destination, value * value, "sumsq");

            List<Object> result = trans.exec();
            if (result == null) {
                continue;
            }

            return result.subList(result.size() - 3, result.size());
        }
        return null;
    }

    public void testAccessTimer(Jedis conn) throws InterruptedException {

        AccessTimer timer = new AccessTimer(conn);
        for (int i = 0; i < 10; i++) {
            timer.start();
            Thread.sleep((int)((0.5 + Math.random()) * 1000));
            timer.stop("req-" + i);
        }

        Set<Tuple> atimes = conn.zrangeWithScores("slowest:AccessTime", 0, -1);
        for (Tuple tuple : atimes) {
            System.out.println(tuple.getElement() + ":" + tuple.getScore());
        }

        assert atimes.size() >= 10;
    }

    public class AccessTimer {

        private Jedis conn;
        private long start;

        public AccessTimer(Jedis conn) {
            this.conn = conn;
        }

        public void start() {
            start = System.currentTimeMillis();
        }

        public void stop(String context) {

            long delta = System.currentTimeMillis() - start;
            List<Object> stats = updateStats(conn, context, "AccessTime", delta / 1000.0);
            double avg = (Double)stats.get(1) / (Double)stats.get(0);

            Transaction trans = conn.multi();
            trans.zadd("slowest:AccessTime", avg, context);
            trans.zremrangeByRank("slowest:AccessTime", 0, -101);
            trans.exec();
        }
    }
}
