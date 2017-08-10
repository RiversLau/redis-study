package com.zhaoxiang.redis.redis_in_action.chapter02;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Author: Rivers
 * Date: 2017/8/5 16:50
 */
public class CookieSample {

    public static void main(String[] args) throws InterruptedException {

        new CookieSample().execute();
    }

    public void execute() throws InterruptedException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(1);

        loginCookies(conn);
        shoppingCart(conn);
        cacheRequest(conn);
        cacheRows(conn);
    }

    public void loginCookies(Jedis conn) throws InterruptedException {

        System.out.println("===== login cookies =====");
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "username", "itemX");
        System.out.println("User [username] just logged-in/update-token, token is " + token);

        String r = checkToken(conn, token);
        System.out.println(r);
        assert  r != null;

        CleanSessionThread thread = new CleanSessionThread(conn, 0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The sessions thread is still alive?");
        }
        long s = conn.hlen("login:");
        System.out.println("The current number of session is " + s);
        assert  s == 0;
    }

    public void shoppingCart(Jedis conn) throws InterruptedException {

        System.out.println("===== shopping cart =====");
        String token = UUID.randomUUID().toString();
        updateToken(conn, token, "username", "itemX");
        addToCart(conn, token, "itemY", 3);

        Map<String, String> cartMap = conn.hgetAll("cart:" + token);
        for (Map.Entry<String, String> entry : cartMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        assert cartMap.size() >= 1;

        System.out.println("Clean shopping cart sessions");
        CleanFullSessionThread thread = new CleanFullSessionThread(conn, 0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The sessions thread is still alive?");
        }

        long s = conn.hlen("login:");
        assert s == 0;
    }

    public void cacheRequest(Jedis conn) {

        System.out.println("===== cache request =====");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback() {
            public String callBack(String request) {
                return "content for " + request;
            }
        };

        updateToken(conn, token, "username", "itemX");
        String url = "http://www.yoxiang.me/?item=itemX";
        String result = cacheRequest(conn, url, callback);
        System.out.println("We got initial content : " + result);

        assert result != null;

        String result2 = cacheRequest(conn, url, null);
        System.out.println("We got the same response : " + result2);

        assert result.equals(result2);

        assert !canCache(conn, "htt://www.yoxiang.com/");
        assert !canCache(conn, "http://www.yoxiang.com/?item=itemX&_=123456");
    }

    public void cacheRows(Jedis conn) throws InterruptedException {

        System.out.println("===== cache rows =====");
        scheduleRowSchedule(conn, "itemX", 5);

        Set<Tuple> tupleSet = conn.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : tupleSet) {
            System.out.println(tuple.getElement() + " : " + tuple.getScore());
        }

        assert tupleSet.size() != 0;

        CacheRowThread thread = new CacheRowThread(conn);
        thread.start();

        Thread.sleep(1000);
        String r = conn.get("inv:itemX");
        assert r != null;

        Thread.sleep(5000);
        String r2 = conn.get("inv:itemX");
        assert r2 != null;
        assert !r2.equals(r);

        scheduleRowSchedule(conn, "itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The sessions thread is still alive?");
        }
    }

    public String checkToken(Jedis conn, String token) {
        return conn.hget("login:", token);
    }

    public void updateToken(Jedis conn, String token, String user, String item) {

        long now = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", now, token);

        if (item != null) {
            conn.zadd("viewed:" + token, now, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    public void addToCart(Jedis conn, String token, String item, int count) {

        if (count <= 0) {
            conn.hdel("cart:" + token, item);
        } else {
            conn.hset("cart:" + token, item, String.valueOf(count));
        }
    }

    public String cacheRequest(Jedis conn, String request, Callback callback) {

        if (!canCache(conn, request)) {
            return callback != null ? callback.callBack(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);
        if (content == null) {
            content = callback.callBack(request);
            conn.setex(pageKey, 300, content);
        }
        return content;
    }

    public boolean canCache(Jedis conn, String request) {

        try {
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<String, String>();
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=");
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }
            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    public String extractItemId(Map<String, String> params) {

        return params.get("item");
    }

    public boolean isDynamic(Map<String, String> params) {

        return params.containsKey("_");
    }

    public String hashRequest(String request) {

        return String.valueOf(request.hashCode());
    }

    public void scheduleRowSchedule(Jedis conn, String item, int delay) {

        conn.zadd("delay:", delay, item);
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, item);
    }

    public class CleanSessionThread extends Thread {

        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionThread(Jedis conn, int limit) {

            this.conn = conn;
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                    sessionKeys.add("cart:" + token);
                }

                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", tokens);
                conn.zrem("recent:", tokens);
            }
        }
    }

    public class CleanFullSessionThread extends Thread {

        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanFullSessionThread(Jedis conn, int limit) {
            this.conn = conn;
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = conn.zrange("recent:", 0, endIndex);
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String sess : sessions) {
                    sessionKeys.add("viewed:" + sess);
                    sessionKeys.add("cart:" + sess);
                }

                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }
        }
    }

    public class CacheRowThread extends Thread {

        private Jedis conn;
        private boolean quit;

        public CacheRowThread(Jedis conn) {
            this.conn = conn;
        }

        public void quit() {
            quit = true;
        }

        public void run() {

            Gson gson = new Gson();
            while (!quit) {
                Set<Tuple> tupleSet = conn.zrangeWithScores("schedule:", 0, 0);
                Tuple next = tupleSet.size() > 0 ? tupleSet.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if (next == null || next.getScore() > now) {
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                String rowId = next.getElement();
                double delay = conn.zscore("delay:", rowId);
                if (delay <= 0) {
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }

                Inventory row = Inventory.get(rowId);
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    public static class Inventory {
        private String id;
        private String data;
        private long time;

        private Inventory(String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}
