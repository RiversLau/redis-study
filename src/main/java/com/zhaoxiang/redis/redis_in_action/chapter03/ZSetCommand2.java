package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Author: Rivers
 * Date: 2017/8/6 21:04
 */
public class ZSetCommand2 {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        Map<String, Double> z1 = new HashMap<String, Double>();
        z1.put("a", 1.0);
        z1.put("b", 2.0);
        z1.put("c", 3.0);
        conn.zadd("zset-1", z1);

        Map<String, Double> z2 = new HashMap<String, Double>();
        z2.put("d", 0.0);
        z2.put("b", 4.0);
        z2.put("c", 1.0);
        conn.zadd("zset-2", z2);

        conn.zinterstore("zset-i", "zset-1", "zset-2");
        Set<Tuple> iSet = conn.zrangeWithScores("zset-i", 0, -1);
        for (Tuple tuple : iSet) {
            System.out.println(tuple.getElement() + " : " + tuple.getScore());
        }

//        Long result = conn.zunionstore("zset-u", "zset-1", "zset-2");
        Long result =conn.zunionstore("zset-u", new ZParams().aggregate(ZParams.Aggregate.MIN), "zset-1", "zset-2");
        System.out.println(result);

        Set<Tuple> uSet = conn.zrangeWithScores("zset-u", 0, -1);
        for (Tuple tuple : uSet) {
            System.out.println(tuple.getElement() + " : " + tuple.getScore());
        }

        conn.sadd("set-1", "a", "d");
        long result2 = conn.zunionstore("zset-u2", "zset-1", "zset-2", "set-1");
        System.out.println(result2);

        Set<Tuple> uSet2 = conn.zrangeWithScores("zset-u2", 0, -1);
        for (Tuple tuple : uSet2) {
            System.out.println(tuple.getElement() + " : " + tuple.getScore());
        }
    }
}
