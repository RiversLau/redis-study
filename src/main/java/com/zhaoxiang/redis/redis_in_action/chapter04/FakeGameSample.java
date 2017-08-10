package com.zhaoxiang.redis.redis_in_action.chapter04;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Author: Rivers
 * Date: 2017/8/8 05:37
 */
public class FakeGameSample {

    public static void main(String[] args) {

        new FakeGameSample().execute();
    }

    public void execute() {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        listItems(conn, false);
        purchaseItem(conn);
        benchmarkUpdateToken(conn);
    }

    public void benchmarkUpdateToken(Jedis conn) {

        System.out.println("===== benchmark update token =====");
        benchmarkUpdateToken(conn, 5);
    }

    public void benchmarkUpdateToken(Jedis conn, int duration) {

        Class[] args = new Class[] {Jedis.class, String.class, String.class, String.class};
        try {
            Method[] methods = new Method[] {
                    this.getClass().getDeclaredMethod("updateToken", args),
                    this.getClass().getDeclaredMethod("updateTokenPipeline", args)
            };
            for (Method method : methods) {
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + duration * 1000;
                while (System.currentTimeMillis() < end) {
                    count++;
                    method.invoke(this, conn, "token", "user", "item");
                }
                long delta = System.currentTimeMillis() - start;
                System.out.println(
                        method.getName() + " " +
                                count + " " +
                                (delta / 1000) + " " +
                                (count / (delta / 1000)));
            }
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void updateToken(Jedis conn, String token, String user, String item) {

        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, token);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, token);
        }
    }

    public void updateTokenPipeline(Jedis conn, String token ,String user, String item) {

        long timestamp = System.currentTimeMillis() / 1000;
        Pipeline pipe = conn.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null) {
            pipe.zadd("viewed:" + token, timestamp, token);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, token);
        }
        pipe.exec();
    }

    public void purchaseItem(Jedis conn) {

        System.out.println("===== purchase item =====");
        listItems(conn, true);

        conn.hset("users:userY", "funds", "125");
        Map<String, String> result = conn.hgetAll("users:userY");
        for (Map.Entry<String, String> entry : result.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        assert result.size() > 0;
        assert result.get("funds") != null;

        boolean p = purchaseItem(conn, "userY", "itemX", "userX", 10);
        assert p;

        result = conn.hgetAll("users:userY");
        for (Map.Entry<String, String> entry : result.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        String buyer = "userY";
        Set<String> i = conn.smembers("inventory:" + buyer);
        for (String str : i) {
            System.out.println(str);
        }

        assert i.size() > 0;
        assert i.contains("itemX");
        assert conn.zscore("market:", "itemX.userX") == null;
    }

    public boolean purchaseItem(Jedis conn, String buyerId, String itemId, String sellerId, double lprice) {

        String buyer = "users:" + buyerId;
        String seller = "sellerId:" + sellerId;
        String item = itemId + "." + sellerId;
        String inventory = "inventory:" + buyerId;

        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            conn.watch("market:" + buyer);

            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));

            if (price != lprice || price > funds) {
                conn.unwatch();
                return false;
            }

            Transaction trans = conn.multi();
            trans.hincrBy(seller, "funds", (long) price);
            trans.hincrBy(buyer, "funds", (long) -price);
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);
            List<Object> results = trans.exec();
            if (results == null) {
                return false;
            }
            return true;
        }
        return false;
    }

    public void listItems(Jedis conn, boolean nest) {

        if (!nest) {
            System.out.println("===== list item =====");
        }

        String seller = "userX";
        String item = "itemX";
        conn.sadd("inventory:" + seller, item);
        Set<String> invertorySet = conn.smembers("invertory:" + seller);
        for (String str : invertorySet) {
            System.out.println(str);
        }

        boolean l = listItem(conn, item, seller, 10);
        assert l;

        Set<Tuple> tuples = conn.zrangeWithScores("market:", 0, -1);
        for (Tuple tuple : tuples) {
            System.out.println(tuple.getElement() + ":" + tuple.getScore());
        }
    }

    public boolean listItem(Jedis conn, String itemId, String seller, double price) {

        String inventory = "inventory:" + seller;
        String item = itemId + "." + seller;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
            conn.watch(inventory);
            if (!conn.sismember(inventory, itemId)) {
                conn.unwatch();
                return false;
            }

            Transaction trans = conn.multi();
            trans.zadd("market:", price, item);
            trans.srem(inventory, itemId);
            List<Object> results = trans.exec();
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }
}
