package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Author: Rivers
 * Date: 2017/8/6 20:36
 */
public class HashCommand2 {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        boolean flag = conn.hexists("hash-key", "k2");
        System.out.println(flag);

        Set<String> keys = conn.hkeys("hash-key");
        for (String key : keys) {
            System.out.println(key);
        }

        Long result = conn.hincrBy("hash-key", "num", 4);
        System.out.println(result);
        Double result2 = conn.hincrByFloat("hash-key", "num", 4.5);
        System.out.println(result2);
    }
}
