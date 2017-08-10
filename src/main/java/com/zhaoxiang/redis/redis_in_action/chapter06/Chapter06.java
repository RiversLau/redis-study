package com.zhaoxiang.redis.redis_in_action.chapter06;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Author: Rivers
 * Date: 2017/8/10 23:22
 */
public class Chapter06 {

    public static void main(String[] args) {

        new Chapter06().run();
    }

    public void run() {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

        testAddUpdateContact(conn);
    }

    private void testAddUpdateContact(Jedis conn) {

        conn.del("recent:user");
        for (int i = 0; i < 10; i++) {
            addUpdateContact(conn, "user", "contact-" + ((int)Math.floor(i / 3)) + "-" + i);
        }

        List<String> contactList = conn.lrange("recent:user", 0, -1);
        for (String contact : contactList) {
            System.out.println(contact);
        }
        System.out.println("===================");

        addUpdateContact(conn, "user", "contact-1-4");
        contactList = conn.lrange("recent:user", 0, 2);
        for (String contact : contactList) {
            System.out.println(contact);
        }
        System.out.println("===================");

        removeContact(conn, "user", "contact-2-6");
        contactList = conn.lrange("recent:user", 0, -1);
        for (String contact : contactList) {
            System.out.println(contact);
        }
        System.out.println("===================");

        List<String> all = conn.lrange("recent:user", 0, -1);
        contactList = fetchAutoCompleteList(conn, "user", "c");
        assert all.equals(contactList);
        List<String> equiv = new ArrayList<String>();
        for (String contact : all) {
            if (contact.startsWith("contact-2-")) {
                equiv.add(contact);
            }
        }

        contactList = fetchAutoCompleteList(conn, "user", "contact-2-");
        Collections.sort(equiv);
        Collections.sort(contactList);
        assert equiv.equals(contactList);

        conn.del("recent:user");
    }

    public void addUpdateContact(Jedis conn, String user, String contact) {

        String acList = "recent:" + user;
        Transaction trans = conn.multi();
        trans.lrem(acList, 0, contact);
        trans.lpush(acList, contact);
        trans.ltrim(acList, 0, 99);
        trans.exec();
    }

    public void removeContact(Jedis conn, String user, String contact) {

        conn.lrem("recent:" + user, 0, contact);
    }

    public List<String> fetchAutoCompleteList(Jedis conn, String user, String prefix) {

        List<String> candidates = conn.lrange("recent:" + user, 0, -1);
        List<String> matches = new ArrayList<String>();
        for (String candidate : candidates) {
            if (candidate.toLowerCase().startsWith(prefix));
            matches.add(candidate);
        }
        return matches;
    }
}
