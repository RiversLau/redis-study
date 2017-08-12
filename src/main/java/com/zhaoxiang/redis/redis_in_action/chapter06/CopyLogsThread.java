package com.zhaoxiang.redis.redis_in_action.chapter06;

import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Author: Rivers
 * Date: 2017/8/12 16:48
 */
public class CopyLogsThread extends Thread {

    private Jedis conn;
    private File path;
    private String channel;
    private int count;
    private long limit;

    public CopyLogsThread(Jedis conn, File path, String channel, int count, long limit) {
        this.conn = conn;
        this.path = path;
        this.channel = channel;
        this.count = count;
        this.limit = limit;
    }

    public void run() {

        Deque<File> waiting = new ArrayDeque<File>();
        long bytesInRedis = 0;


    }
}
