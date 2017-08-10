package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Author: Rivers
 * Date: 2017/8/6 20:20
 */
public class SetCommand2 {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        conn.sadd("skey1", "a", "b", "c", "d");
        conn.sadd("skey2", "c", "d", "e", "f");

        Set<String> diffes = conn.sdiff("skey2", "skey1");
        for (String diff : diffes) {
            System.out.println(diff);
        }

        Set<String> inters = conn.sinter("skey2", "skey1");
        for (String inter : inters) {
            System.out.println(inter);
        }

        Set<String> unions = conn.sunion("skey2", "skey1");
        for (String un : unions) {
            System.out.println(un);
        }
    }
}
