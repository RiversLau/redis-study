package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

/**
 * Author: Rivers
 * Date: 2017/8/6 15:52
 */
public class StringCommand2 {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        conn.set("hello", "hello");
        Long result = conn.append("hello", " world");
        System.out.println(result);
        String result2 =  conn.get("hello");
        String result3 = conn.getrange("hello", 4, 11);
        System.out.println(result2);
        System.out.println(result3);

        conn.setrange("hello", 5, " nice to meet you");
        String result4 = conn.get("hello");
        System.out.println(result4);

        boolean result5 = conn.getbit("hello", 3);
        System.out.println(result5);
        conn.setbit("hello", 3, "0");
        String result6 = conn.get("hello");
        System.out.println(result6);

        Long result7 = conn.bitcount("hello", 0, 4);
        System.out.println(result7);
    }
}
