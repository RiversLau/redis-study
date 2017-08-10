package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Author: Rivers
 * Date: 2017/8/6 16:21
 */
public class LinkedListCommand {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

//        conn.rpush("redis", "v2.6", "v2.9", "v3.2", "v4.0");
//        conn.lpush("redis", "version");
//        List<String> result = conn.lrange("redis", 0, -1);
//        for (String value : result) {
//            System.out.println(value);
//        }

//        String result1 = conn.lpop("redis");
//        System.out.println(result1);
//
//        List<String> range = conn.lrange("redis", 0, -1);
//        for (String value : range) {
//            System.out.println(value);
//        }

//        String result2 = conn.lpop("redis");
//        System.out.println(result2);
//
//        conn.rpush("redis", "a", "b", "c");
//        List<String> range = conn.lrange("redis", 0, -1);
//        for (String value : range) {
//            System.out.println(value);
//        }

        conn.ltrim("redis", 1, -1);
        List<String> range = conn.lrange("redis", 0, -1);
        for (String value : range) {
            System.out.println(value);
        }

//        conn.rpush("list", "item1");
//        conn.rpush("list", "item2");
//        conn.rpush("list2", "item3");
//        String result = conn.brpoplpush("list2", "list", 1);
//        System.out.println(result);

//        conn.brpoplpush("list2", "list", 1);
//        List<String> result = conn.lrange("list", 0, -1);
//        for (String str : result) {
//            System.out.println(str);
//        }
//
//        String result2 = conn.brpoplpush("list", "list2", 1);
//        System.out.println(result2);

        List<String> result = conn.blpop(1, "list", "list2");
        for (String str : result) {
            System.out.println(str);
        }
    }
}
