package com.zhaoxiang.redis.redis_in_action.chapter03;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;

import java.util.List;

/**
 * Author: Rivers
 * Date: 2017/8/6 21:42
 */
public class SortCommand {

    public static void main(String[] args) {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        conn.del("sort-input");

        conn.rpush("sort-input", "23", "15", "110", "7");
        List<String> result = conn.sort("sort-input");
        for (String str : result) {
            System.out.println(str);
        }

        SortingParams params = new SortingParams();
        params.alpha();
        List<String> result1 = conn.sort("sort-input", params);
        for (String str : result1) {
            System.out.println(str);
        }

        conn.hset("d-7", "field", "5");
        conn.hset("d-15", "field", "1");
        conn.hset("d-23", "field", "9");
        conn.hset("d-110", "field", "3");

        SortingParams params2 = new SortingParams();
        params2.by("d-*->field");
        List<String> result2 = conn.sort("sort-input", params2);
        for (String str : result2) {
            System.out.println(str);
        }

        SortingParams param3 = new SortingParams();
        param3.by("d-*->field");
        param3.get("d-*->field");
        List<String> result3 =  conn.sort("sort-input", param3);
        for (String str : result3) {
            System.out.println(str);
        }
    }
}
