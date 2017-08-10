package com.zhaoxiang.redis.redis_in_action.chapter05;

import org.javatuples.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * Author: Rivers
 * Date: 2017/8/9 05:54
 */
public class Counter {

    public static void main(String[] args) throws InterruptedException {

        new Counter().execute();
    }

    public void execute() throws InterruptedException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        testCounter(conn);
    }

    public void testCounter(Jedis conn) throws InterruptedException {

        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 10; i++) {
            int count = (int) ((Math.random() * 5) + 1);
            updateCounter(conn, "test", count, now + i);
        }

        List<Pair<Integer, Integer>> counter = getCounter(conn, "test", 1);
        for (Pair<Integer, Integer> count : counter) {
            System.out.println(" " + count);
        }

        counter = getCounter(conn, "test", 5);
        for (Pair<Integer, Integer> count : counter) {
            System.out.println(" " + count);
        }

        CleanCountersThread thread = new CleanCountersThread(conn, 0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
        counter = getCounter(conn, "test", 86400);
        assert counter.size() == 0;
    }

    private static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};
    public void updateCounter(Jedis conn, String name, int count, long now) {

        Transaction trans = conn.multi();
        for (int pre : PRECISION) {
            long pnow = (now / pre) * pre;
            String hash = String.valueOf(pre) + ":" + name;
            trans.zadd("known:", 0, hash);
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    public List<Pair<Integer, Integer>> getCounter(Jedis conn, String name, int precision) {

        String hash = String.valueOf(precision) + ":" + name;
        Map<String, String> data = conn.hgetAll("count:" + hash);
        ArrayList<Pair<Integer, Integer>> results = new ArrayList<Pair<Integer, Integer>>();

        for (Map.Entry<String, String> entry : data.entrySet()) {
            results.add(new Pair<Integer, Integer>(
                    Integer.parseInt(entry.getKey()),
                    Integer.parseInt(entry.getValue())));
        }
        Collections.sort(results);
        return results;
    }

    public class CleanCountersThread extends Thread {

        private Jedis conn;
        private int sampleCount = 100;
        private long timeOffset;

        private boolean quit;

        public CleanCountersThread(Jedis conn, int sampleCount, long timeOffset) {
            this.conn = conn;
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            int passes = 0;
            while (!quit) {
                long start = System.currentTimeMillis() + timeOffset;
                int index = 0;
                while (index < conn.zcard("known:")) {
                    Set<String> hashSet = conn.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0) {
                        break;
                    }

                    String hash = hashSet.iterator().next();
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(":")));
                    int bprec = (int) Math.floor(prec / 60);
                    if (bprec == 0) {
                        bprec = 1;
                    }

                    if ((passes % bprec) != 0) {
                        continue;
                    }

                    String hkey = "count:" + hash;
                    String cutoff = String.valueOf((System.currentTimeMillis() + timeOffset) / 1000 - sampleCount * prec);
                    ArrayList<String> samples = new ArrayList<String>(conn.hkeys(hkey));
                    Collections.sort(samples);

                    int remove = bisectRight(samples, cutoff);
                    if (remove != 0) {
                        conn.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        if (remove == samples.size()) {
                            conn.watch(hkey);
                            if (conn.hlen(hkey) == 0) {
                                Transaction trans = conn.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            } else {
                                conn.unwatch();
                            }
                        }
                    }
                }
                passes++;
                long duration = Math.min(System.currentTimeMillis() + timeOffset - start + 1000, 60000);
                try {
                    sleep(Math.max(60000-duration, 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public int bisectRight(List<String> samples, String key) {

            int index = Collections.binarySearch(samples, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }
}
