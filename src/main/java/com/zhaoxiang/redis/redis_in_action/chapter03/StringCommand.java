package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

/**
 * Author: Rivers
 * Date: 2017/8/6 15:43
 */
public class StringCommand {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        String foo = conn.get("foo");
        System.out.println(foo);
        Long result = conn.incr("foo");
        System.out.println(result);
        Long result2 = conn.incrBy("foo", 10);
        System.out.println(result2);
        Long result3 = conn.decrBy("foo", 12);
        System.out.println(result3);

        String result4 = conn.set("foo", "13");
        System.out.println(result4);

        Double result5 = conn.incrByFloat("foo", 2.35);
        System.out.println(result5);
    }
}
