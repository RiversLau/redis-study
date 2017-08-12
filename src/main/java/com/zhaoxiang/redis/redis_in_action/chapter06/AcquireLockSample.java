package com.zhaoxiang.redis.redis_in_action.chapter06;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

/**
 * Author: Rivers
 * Date: 2017/8/12 09:42
 */
public class AcquireLockSample {

    public static void main(String[] args) throws InterruptedException {

        new AcquireLockSample().execute();
    }

    public void execute() throws InterruptedException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        conn.del("lock:testlock");
        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
        System.out.println("Got lock");
        assert acquireLockWithTimeout(conn, "testlock", 10, 1000) == null;
        System.out.println("Failed to get lock");

        Thread.sleep(2000);
        String lockId = acquireLockWithTimeout(conn, "testlock", 1000, 1000);
        assert lockId != null;
        System.out.println("Got lock");
        assert releaseLock(conn, "testlock", lockId);

        assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;

        conn.del("lock:testlock");
    }

    public void purchaseItemWithLock(Jedis conn, String buyerId, String itemId, String sellerId) {

        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + "." + sellerId;
        String inventory = "inventory:" + buyerId;

        String lockId = acquireLock(conn, "market:");
        if (lockId != null) {
            try {

            } finally {
                releaseLock(conn, "testLock", lockId);
            }
        }
    }

    public String acquireLock(Jedis conn, String lockName) {
        return acquireLock(conn, lockName, 10000);
    }

    public String acquireLock(Jedis conn, String lockName, long acquireTimeout) {

        String id = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + acquireTimeout;

        while (System.currentTimeMillis() < end) {
            if (conn.setnx("lock" + lockName, id) == 1) {
                return id;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    public String acquireLockWithTimeout(Jedis conn, String lockName, long acquireTimeout, long lockTimeout) {

        String id = UUID.randomUUID().toString();
        String lockKey = "lock:" + lockName;
        int lockExpire = (int)(lockTimeout / 1000);

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockKey, id) == 1) {
                conn.expire(lockKey, lockExpire);
                return id;
            }
            if (conn.ttl(lockKey) == -1) {
                conn.expire(lockKey, lockExpire);
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    public boolean releaseLock(Jedis conn, String lockName, String lockId) {

        String lockKey = "lock:" + lockName;
        while (true) {
            conn.watch(lockKey);
            if (lockId.equals(conn.get(lockKey))) {
                Transaction trans = conn.multi();
                trans.del(lockKey);
                List<Object> result = trans.exec();
                if (result == null) {
                    continue;
                }
                return true;
            }
            conn.unwatch();
            break;
        }
        return false;
    }
}
