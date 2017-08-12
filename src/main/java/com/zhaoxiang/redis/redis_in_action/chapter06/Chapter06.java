package com.zhaoxiang.redis.redis_in_action.chapter06;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.*;

/**
 * Author: Rivers
 * Date: 2017/8/10 23:22
 */
public class Chapter06 {

    public static void main(String[] args) throws InterruptedException {

        new Chapter06().run();
    }

    public void run() throws InterruptedException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

//        testAddUpdateContact(conn);
//        testAddressBookAutoComplete(conn);
//        testDelayedTasks(conn);
        testMultiRecipientMessaging(conn);
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

    public void testAddressBookAutoComplete(Jedis conn) {

        System.out.println("============ address book auto complete ============");

        conn.del("members:test");
        String[] prefixRange = findPrefixRange("je");
        System.out.println(prefixRange[0] + ":" + prefixRange[1]);

        for (String name : new String[]{"jeff", "jenny", "jack", "jennifer"}) {
            joinGuild(conn, "test", name);
        }

        Set<String> stringSet = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(stringSet);

        leaveGuild(conn, "test", "jeff");
        stringSet = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(stringSet);

        conn.del("members:test");
    }

    public void testDelayedTasks(Jedis conn) throws InterruptedException {

        System.out.println("========= test delayed tasks ==========");

        conn.del("queue:tqueue", "delayed:");
        for (long delay : new long[]{0, 500, 0, 1000}) {
            assert executeLater(conn, "tqueue", "testfn", new ArrayList<String>(), delay) != null;
        }

        long length = conn.llen("queue:tqueue");
        assert length == 2;

        PollQueueThread thread = new PollQueueThread(conn);
        thread.start();
        Thread.sleep(2000);
        thread.quit();
        thread.join();
        length = conn.llen("queue:tqueue");
        assert length == 4;
        conn.del("queue:tqueue", "delayed:");
    }

    public void testMultiRecipientMessaging(Jedis conn) {

        conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");

        Set<String> recipientSet = new HashSet<String>();
        recipientSet.add("jeff");
        recipientSet.add("jenny");

        String chatId = createChat(conn, "joe", recipientSet, "message one");
        for (int i = 0; i < 3; i++) {
            sendMessage(conn, chatId, "joe", "Hello " + i);
        }

        List<ChatMessage> msgList1 = fetchPendingMessage(conn, "jeff");
        List<ChatMessage> msgList2 = fetchPendingMessage(conn, "jenny");
        assert msgList1.size() == msgList2.size();

        for (ChatMessage cm : msgList1) {
            System.out.println("ChatId = " + cm.getChatId());
            System.out.println("Messages = " + cm.getMessages());
        }
    }

    public String createChat(Jedis conn, String sender, Set<String> recipients, String message) {

        String chatId = String.valueOf(conn.incr("ids:chat:"));
        return createChat(conn, sender, recipients, message, chatId);
    }

    public String createChat(Jedis conn, String sender, Set<String> recipients, String message, String chatId) {

        recipients.add(sender);

        Transaction trans = conn.multi();
        for (String user : recipients) {
            trans.zadd("chat:" + chatId, 0, user);
            trans.zadd("seen:" + user, 0, chatId);
        }
        trans.exec();
        return sendMessage(conn, chatId, sender, message);
    }

    public String sendMessage(Jedis conn, String chatId, String sender, String message) {

        long msgId = conn.incr("ids:" + chatId);
        HashMap<String, Object> values = new HashMap<String, Object>();
        values.put("id", msgId);
        values.put("ts", System.currentTimeMillis());
        values.put("sender", sender);
        values.put("message", message);
        String packed = new Gson().toJson(values);
        conn.zadd("msgs:" + chatId, msgId, packed);
        return chatId;
    }

    public List<ChatMessage> fetchPendingMessage(Jedis conn, String recipient) {

        Set<Tuple> seenSet = conn.zrangeWithScores("seen:" + recipient, 0, -1);
        List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

        Transaction trans = conn.multi();
        for (Tuple tuple : seenList) {
            String chatId = tuple.getElement();
            int seenId = (int)tuple.getScore();
            trans.zrangeByScore("msgs:" + chatId, String.valueOf(seenId + 1), "inf");
        }
        List<Object> results = trans.exec();

        Gson gson = new Gson();
        Iterator<Tuple> seenIterator = seenList.iterator();
        Iterator<Object> resultsIterator = results.iterator();

        List<ChatMessage> msgList = new ArrayList<ChatMessage>();
        List<Object[]> seenUpdates = new ArrayList<Object[]>();
        List<Object[]> msgRemoves = new ArrayList<Object[]>();
        while (seenIterator.hasNext()) {
            Tuple seen = seenIterator.next();
            Set<String> msgStr = (Set<String>)resultsIterator.next();
            if (msgStr.size() == 0) {
                continue;
            }
            int seenId = 0;
            String chatId = seen.getElement();
            List<Map<String, Object>> msg = new ArrayList<Map<String, Object>>();
            for (String msgJson : msgStr) {
                Map<String, Object> m = gson.fromJson(msgJson, new TypeToken<Map<String, Object>>(){}.getType());
                int msgId = ((Double)m.get("id")).intValue();
                if (msgId > seenId) {
                    seenId = msgId;
                }
                m.put("id", msgId);
                msg.add(m);
            }

            conn.zadd("chat:" + chatId, seenId, recipient);
            seenUpdates.add(new Object[]{"seen:" + recipient, seenId, chatId});

            Set<Tuple> minIdSet = conn.zrangeWithScores("chat:" + chatId, 0, 0);
            if (minIdSet.size() > 0) {
                msgRemoves.add(new Object[]{"msgs:" + chatId, minIdSet.iterator().next().getScore()});
            }

            msgList.add(new ChatMessage(chatId, msg));
        }

        trans = conn.multi();
        for (Object[] seenUpdate : seenUpdates) {
            trans.zadd((String)seenUpdate[0], (Integer)seenUpdate[1], (String)seenUpdate[2]);
        }
        for (Object[] msgRemove : msgRemoves) {
            trans.zremrangeByScore((String)msgRemove[0], 0, ((Double)msgRemove[1]).intValue());
        }
        trans.exec();

        return msgList;
    }

    public String executeLater(Jedis conn, String queue, String name, List<String> args, long delay) {

        Gson gson = new Gson();
        String id = UUID.randomUUID().toString();
        String itemArgs = gson.toJson(args);
        String item = gson.toJson(new String[]{id, queue, name, itemArgs});
        if (delay > 0) {
            conn.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
            conn.rpush("queue:", item);
        }
        return id;
    }

    public Set<String> autocompleteOnPrefix(Jedis conn, String guild, String prefix) {

        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];

        String id = UUID.randomUUID().toString();
        start += id;
        end += id;
        String zsetName = "members:" + guild;

        conn.zadd(zsetName, 0, start);
        conn.zadd(zsetName, 0, end);

        Set<String> result;
        while (true) {
            conn.watch(zsetName);
            int sindex = conn.zrank(zsetName, start).intValue();
            int eindex = conn.zrank(zsetName, end).intValue();
            int erange = Math.min(sindex + 9, eindex - 2);

            Transaction trans = conn.multi();
            trans.zrem(zsetName, start);
            trans.zrem(zsetName, end);
            trans.zrange(zsetName, sindex, erange);
            List<Object> r = trans.exec();
            if (r != null) {
                result = (Set<String>) r.get(r.size() - 1);
                break;
            }
        }

        for (Iterator<String> iterator = result.iterator(); iterator.hasNext(); ) {
            if (iterator.next().indexOf("{") != -1) {
                iterator.remove();
            }
        }
        return result;
    }

    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
    public String[] findPrefixRange(String prefix) {

        int pos = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() -1));
        char suffix = VALID_CHARACTERS.charAt(pos > 0 ? pos - 1 : 0);

        String start = prefix.substring(0, prefix.length() - 1) + suffix + "{";
        String end = prefix + "{";
        return new String[]{start, end};
    }

    /**
     * 加入公会
     * @param conn
     * @param guild
     * @param name
     */
    public void joinGuild(Jedis conn, String guild, String name) {

        conn.zadd("members:" + guild, 0, name);
    }

    /**
     * 移除公会
     * @param conn
     * @param guild
     * @param name
     */
    public void leaveGuild(Jedis conn, String guild, String name) {

        conn.zrem("members:" + guild, name);
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

    public class PollQueueThread extends Thread {

        private Jedis conn;
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread(Jedis conn) {
            this.conn = conn;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                Set<Tuple> items = conn.zrangeWithScores("delayed:", 0, 0);
                Tuple item = items.size() > 0 ? items.iterator().next() : null;
                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    try {
                        sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                String json = item.getElement();
                String[] values = gson.fromJson(json, String[].class);
                String id = values[0];
                String queue = values[1];

                if (conn.zrem("delayed:", json) == 1) {
                    conn.rpush("queue:" + queue, id);
                }
            }
        }
    }
}
