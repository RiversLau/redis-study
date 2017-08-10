package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Author: Rivers
 * Date: 2017/8/6 20:41
 */
public class ZSetCommand {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        conn.zadd("zset-key", 3, "a");
        conn.zadd("zset-key", 2, "b");
        conn.zadd("zset-key", 1, "c");
        Map<String, Double> pairs = new HashMap<String, Double>();
        pairs.put("d", 5.0);
        pairs.put("e", 8.0);
        conn.zadd("zset-key", pairs);

        Long size = conn.zcard("zset-key");
        System.out.println(size);

        Double cValue = conn.zincrby("zset-key", 10, "c");
        System.out.println(cValue);
        Double bScore = conn.zscore("zset-key", "b");
        System.out.println(bScore);

        long cRank = conn.zrank("zset-key", "c");
        System.out.println(cRank);

        long count = conn.zcount("zset-key", 0, 4);
        System.out.println(count);

        long bremove = conn.zrem("zset-key", "b");
        System.out.println(bremove);

        Set<Tuple> tupleSet = conn.zrangeWithScores("zset-key", 0, -1);
        for (Tuple t : tupleSet) {
            System.out.println(t.getElement() + " : " + t.getScore());
        }
    }
}
