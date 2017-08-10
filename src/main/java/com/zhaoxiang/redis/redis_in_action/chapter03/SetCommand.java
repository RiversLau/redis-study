package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Author: Rivers
 * Date: 2017/8/6 20:05
 */
public class SetCommand {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

//        Long result = conn.sadd("set-key", "a", "b", "c");
//        System.out.println(result);
//        Long result1 = conn.srem("set-key", "c", "d");
//        System.out.println(result1);

//        Long result = conn.srem("set-key", "c", "d");
//        System.out.println(result);

        Set<String> values = conn.smembers("set-key");
        for (String str : values) {
            System.out.println(str);
        }

        Long result = conn.smove("set-key", "set-key2", "a");
        System.out.println(result);
        Long result1 = conn.smove("set-key", "set-key2", "a");
        System.out.println(result1);

        Set<String> values2 = conn.smembers("set-key2");
        for (String str : values2) {
            System.out.println(str);
        }
    }
}
