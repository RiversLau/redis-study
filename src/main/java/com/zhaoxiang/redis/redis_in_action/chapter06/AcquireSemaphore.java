package com.zhaoxiang.redis.redis_in_action.chapter06;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.List;
import java.util.UUID;

/**
 * Author: Rivers
 * Date: 2017/8/12 10:41
 */
public class AcquireSemaphore {

    public static void main(String[] args) throws InterruptedException {

        new AcquireSemaphore().execute();
    }

    public void execute() throws InterruptedException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        conn.del("testsem", "testsem:owner", "testsem:counter");
        for (int i = 0; i < 3; i++) {
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
        }

        assert acquireFairSemaphore(conn, "testsem", 3, 1000) == null;

        Thread.sleep(2000);

        String id = acquireFairSemaphore(conn, "testsem", 3, 1000);
        assert id != null;

        assert releaseFairSemaphore(conn, "testsem", id);

        for (int i = 0; i < 3; i++) {
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
        }

        conn.del("testsem", "testsem:owner", "testsem:counter");
    }

    private boolean releaseFairSemaphore(Jedis conn, String semname, String semaphoreId) {

        Transaction trans = conn.multi();
        trans.zrem(semname, semaphoreId);
        trans.zrem(semname + ":owner", semaphoreId);
        List<Object> results = trans.exec();
        return (Long)results.get(results.size() - 1) == 1;
    }

    public String acquireFairSemaphore(Jedis conn, String semname, int limit, long timeout) {

        String id = UUID.randomUUID().toString();
        String czset = semname + ":owner";
        String ctr = semname + ":counter";

        long now = System.currentTimeMillis();
        Transaction trans = conn.multi();
        trans.zremrangeByScore(semname.getBytes(), "-inf".getBytes(), String.valueOf(now - timeout).getBytes());
        ZParams params = new ZParams();
        params.weightsByDouble(1.0, 0.0);
        trans.zinterstore(czset, params, czset, semname);
        trans.incr(ctr);

        List<Object> result = trans.exec();
        int counter = ((Long)result.get(result.size() - 1)).intValue();

        trans = conn.multi();
        trans.zadd(semname, now, id);
        trans.zadd(czset, counter, id);
        trans.zrank(czset, id);
        result = trans.exec();
        int r = ((Long)result.get(result.size() - 1)).intValue();
        if (r < limit) {
            return id;
        }

        trans = conn.multi();
        trans.zrem(semname, id);
        trans.zrem(czset, id);
        trans.exec();
        return null;
    }
}
