package com.zhaoxiang.redis.redis_in_action.chapter06;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zhaoxiang.redis.redis_in_action.chapter02.Callback;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Author: Rivers
 * Date: 2017/8/10 23:22
 */
public class Chapter06 {

    public static void main(String[] args) throws InterruptedException, IOException {

        new Chapter06().run();
    }

    public void run() throws InterruptedException, IOException {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);

//        testAddUpdateContact(conn);
//        testAddressBookAutoComplete(conn);
//        testDelayedTasks(conn);
//        testMultiRecipientMessaging(conn);
        testFileDistribution(conn);
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

    public void testFileDistribution(Jedis conn) throws IOException, InterruptedException {

        String[] keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0){
            conn.del(keys);
        }
        conn.del("msgs:test:", "seen:0", "seen:source", "ids:test:", "chat:test:");

        File f1 = File.createTempFile("temp_redis_1", ".txt");
        f1.deleteOnExit();

        Writer writer = new FileWriter(f1);
        writer.write("one line\n");
        writer.close();

        File f2 = File.createTempFile("temp_redis_2", ".txt");
        f2.deleteOnExit();
        writer = new FileWriter(f2);

        for (int i = 0; i < 100; i++) {
            writer.write("many lines " + i + "\n");
        }
        writer.close();

        File f3 = File.createTempFile("temp_redis_3", ".txt.gz");
        f3.deleteOnExit();
        writer = new FileWriter(f3);
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            writer.write("random line " + Long.toHexString(random.nextLong()) + "\n");
        }
        writer.close();

        long size = f3.length();
        File path = f1.getParentFile();
        CopyLogsThread thread = new CopyLogsThread(conn, path, "test:", 1, size);
        thread.start();

        Thread.sleep(250);
        TestCallback  callback = new TestCallback();
        processLogsFromRedis(conn, "0", callback);
        assert callback.counts.get(0) == 1;
        assert callback.counts.get(1) == 100;
        assert callback.counts.get(2) == 1000;

        thread.join();

        keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0) {
            conn.del(keys);
        }
        conn.del("msgs:test:", "seen:0", "seen:source", "ids:test:", "chat:test:");
    }

    public void processLogsFromRedis(Jedis conn, String id, Callback callback) throws IOException, InterruptedException {

        while (true) {
            List<ChatMessage> fdata = fetchPendingMessage(conn, id);

            for (ChatMessage msg : fdata) {
                for (Map<String, Object> m : msg.getMessages()) {
                    String logFile = (String) m.get("message");
                    if (":done".equals(logFile)) {
                        return;
                    }
                    if (logFile == null || logFile.length() == 0) {
                        continue;
                    }
                    InputStream in = new RedisInputStream(conn, msg.getChatId() + logFile);
                    if (logFile.endsWith(".gz")) {
                        in = new GZIPInputStream(in);
                    }

                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try {
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            callback.callBack(line);
                        }
                        callback.callBack(null);
                    } finally {
                        reader.close();
                    }

                    conn.incr(msg.getChatId() + logFile + ":done");
                }
            }
            if (fdata.size() == 0) {
                Thread.sleep(100);
            }
        }
    }

    public class RedisInputStream extends InputStream {

        private Jedis conn;
        private String key;
        private int pos;

        public RedisInputStream(Jedis conn, String key) {
            this.conn = conn;
            this.key = key;
        }

        public int available() {
            long len = conn.strlen(key);
            return (int)(len - pos);
        }

        public int read() {
            byte[] block = conn.substr(key.getBytes(), pos, pos);
            if (block == null || block.length == 0) {
                return -1;
            }
            pos++;
            return block[0] & 0xff;
        }

        public int read(byte[] buf, int off, int len) {
            byte[] block = conn.substr(key.getBytes(), pos, pos + (len - off - 1));
            if (block == null || block.length == 0) {
                return -1;
            }
            System.arraycopy(block, 0, buf, off, block.length);
            pos += block.length;
            return block.length;
        }

        public void close() {

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

            Set<String> recipients = new HashSet<String>();
            for (int i = 0; i < 10; i++) {
                recipients.add(String.valueOf(i));
            }

            createChat(conn, "source", recipients, "", channel);
            File[] logFiles = path.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith("temp_redis");
                }
            });

            Arrays.sort(logFiles);

            for (File logFile : logFiles) {
                long fsize = logFile.length();
                while ((bytesInRedis + fsize) > limit) {
                    long cleaned = clean(waiting, count);
                    if (cleaned != 0) {
                        bytesInRedis -= cleaned;
                    } else {
                        try {
                            sleep(250);
                        } catch (InterruptedException e) {
                            Thread.interrupted();
                        }
                    }
                }

                BufferedInputStream in = null;
                try {
                    in = new BufferedInputStream(new FileInputStream(logFile));
                    int read = 0;
                    byte[] buffer = new byte[8192];
                    while ((read = in.read(buffer, 0, buffer.length)) != -1) {
                        if (buffer.length != read) {
                            byte[] bytes = new byte[read];
                            System.arraycopy(buffer, 0, bytes, 0, read);
                            conn.append((channel + logFile).getBytes(), bytes);
                        } else {
                            conn.append((channel + logFile).getBytes(), buffer);
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                sendMessage(conn, channel, "source", logFile.toString());

                bytesInRedis += fsize;
                waiting.addLast(logFile);
            }

            sendMessage(conn, channel, "source", ":done");
            while (waiting.size() > 0) {
                long cleaned = clean(waiting, count);
                if (cleaned != 0) {
                    bytesInRedis -= cleaned;
                } else {
                    try {
                        sleep(250);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private long clean(Deque<File> waiting, int count) {
            if (waiting.size() == 0) {
                return 0;
            }
            File w0 = waiting.getFirst();
            if (String.valueOf(count).equals(conn.get(channel + w0 + ":done"))) {
                conn.del(channel + w0, channel + w0 + ":done");
                return waiting.removeFirst().length();
            }
            return 0;
        }
    }

    public class TestCallback implements Callback {

        private int index;
        public List<Integer> counts = new ArrayList<>();

        @Override
        public String callBack(String line) {
            if (line == null) {
                index++;
            }
            while (counts.size() == index) {
                counts.add(0);
            }
            counts.set(index, counts.get(index) + 1);
            return null;
        }
    }
}
