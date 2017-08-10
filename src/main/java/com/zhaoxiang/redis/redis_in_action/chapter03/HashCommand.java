package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: Rivers
 * Date: 2017/8/6 20:28
 */
public class HashCommand {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        Map<String, String> pairs = new HashMap<String, String>();
        pairs.put("k1", "v1");
        pairs.put("k2", "v2");
        pairs.put("k3", "v3");
        conn.hmset("hash-key", pairs);

        List<String> result = conn.hmget("hash-key", "k1", "k2");
        for (String str : result) {
            System.out.println(str);
        }

        long length = conn.hlen("hash-key");
        System.out.println(length);

        conn.hdel("hash-key", "k1", "k3");
        length = conn.hlen("hash-key");
        System.out.println(length);
    }
}
