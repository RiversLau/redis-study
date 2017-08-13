package com.zhaoxiang.redis.redis_in_action.chapter07;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: Rivers
 * Date: 2017/8/13 05:41
 */
public class Chapter07 {

    private static final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");
    private static final Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");

    private static final Set<String> STOP_WORDS = new HashSet<>();
    static {
        for (String word :
                ("able about across after all almost also am among " +
                        "an and any are as at be because been but by can " +
                        "cannot could dear did do does either else ever " +
                        "every for from get got had has have he her hers " +
                        "him his how however if in into is it its just " +
                        "least let like likely may me might most must my " +
                        "neither no nor not of off often on only or other " +
                        "our own rather said say says she should since so " +
                        "some than that the their them then there these " +
                        "they this tis to too twas us wants was we were " +
                        "what when where which while who whom why will " +
                        "with would yet you your").split(" "))
        {
            STOP_WORDS.add(word);
        }
    }

    private static String CONTENT =
            "this is some random content, look at how it is indexed.";

    public static void main(String[] args) {

        new Chapter07().execute();
    }

    public void execute() {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");
        conn.select(15);
        conn.flushDB();

        testIndexDocument(conn);
        testSetOperations(conn);
        testParseQuery(conn);
        testParseAndSearch(conn);
        testSearchAndSort(conn);
        testSearchAndZsort(conn);
        conn.flushDB();

        testStringtoScore(conn);
    }

    public void testIndexDocument(Jedis conn) {

        Set<String> tokens = tokenize(CONTENT);
        System.out.println("Tokens are " + Arrays.toString(tokens.toArray()));

        int count = indexDocument(conn, "test", CONTENT);
        assert count == tokens.size();

        Set<String> test = new HashSet<>();
        test.add("test");
        for (String t : tokens) {
            Set<String> members = conn.smembers("idx:" + t);
            assert test.equals(members);
        }
    }

    public void testSetOperations(Jedis conn) {

        indexDocument(conn, "test", CONTENT);

        Set<String> test = new HashSet<>();
        test.add("test");

        Transaction trans = conn.multi();
        String id = intersect(trans, 30, "content", "indexed");
        trans.exec();
        assert test.equals(conn.smembers("idx:" + id));

        trans = conn.multi();
        id = intersect(trans, 30, "content", "ignored");
        trans.exec();
        assert conn.smembers("idx:" + id).isEmpty();

        trans = conn.multi();
        id = union(trans, 30, "content", "ignored");
        trans.exec();
        assert test.equals(conn.smembers("idx:" + id));

        trans = conn.multi();
        id = difference(trans, 30, "content", "ignored");
        trans.exec();
        assert test.equals(conn.smembers("idx:" + id));

        trans = conn.multi();
        id = difference(trans, 30, "content", "indexed");
        trans.exec();
        assert conn.smembers("idx:" + id).isEmpty();
    }

    public void testParseQuery(Jedis conn) {

        String queryString = "test query without stopwords";
        Query query = parse(queryString);

        String[] words = queryString.split(" ");
        for (int i = 0; i < words.length; i++) {
            List<String> word = new ArrayList<>();
            word.add(words[i]);
            assert word.equals(query.all.get(i));
        }
        assert query.unwanted.isEmpty();

        queryString = "test +query without -stopwords";
        query = parse(queryString);
        assert "test".equals(query.all.get(0).get(0));
        assert "query".equals(query.all.get(0).get(1));
        assert "without".equals(query.all.get(1).get(0));
        assert "stopwords".equals(query.unwanted.toArray()[0]);
    }

    public void testParseAndSearch(Jedis conn) {

        indexDocument(conn, "test", CONTENT);

        Set<String> test = new HashSet<>();
        test.add("test");

        String id = parseAndSearch(conn, "content", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = parseAndSearch(conn, "content indexed random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = parseAndSearch(conn, "content +indexed random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = parseAndSearch(conn, "content indexed +random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = parseAndSearch(conn, "content indexed -random", 30);
        assert conn.smembers("idx:" + id).isEmpty();

        id = parseAndSearch(conn, "content indexed +random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        System.out.println("Which passed!");
    }

    public void testSearchAndSort(Jedis conn) {

        indexDocument(conn, "test", CONTENT);
        indexDocument(conn, "test2", CONTENT);

        HashMap<String, String> values = new HashMap<>();
        values.put("updated", "12345");
        values.put("id", "10");
        conn.hmset("kb:doc:test", values);

        values.put("updated", "54321");
        values.put("id", "1");
        conn.hmset("kb:doc:test2", values);

        SearchResult result = searchAndSort(conn, "content", "-updated");
        assert "test2".equals(result.results.get(0));
        assert "test".equals(result.results.get(1));

        result = searchAndSort(conn, "content", "-id");
        assert "test".equals(result.results.get(0));
        assert "test2".equals(result.results.get(1));

        System.out.println("Which passed!");
    }

    public void testSearchAndZsort(Jedis conn) {

        indexDocument(conn, "test", CONTENT);
        indexDocument(conn, "test2", CONTENT);

        conn.zadd("idx:sort:update", 12345, "test");
        conn.zadd("idx:sort:update", 54321, "test2");
        conn.zadd("idx:sort:votes", 10, "test");
        conn.zadd("idx:sort:votes", 1, "test2");

        Map<String,Integer> weights = new HashMap<>();
        weights.put("update", 1);
        weights.put("vote", 0);
        SearchResult result = searchAndZsort(conn, "content", false, weights);
        assert "test".equals(result.results.get(0));
        assert "test2".equals(result.results.get(1));

        weights.put("update", 0);
        weights.put("vote", 1);
        result = searchAndZsort(conn, "content", false, weights);
        assert "test2".equals(result.results.get(0));
        assert "test".equals(result.results.get(1));
        System.out.println("Which passed!");
    }

    public void testStringtoScore(Jedis conn) {

        String[] words = "these are some words that will be sorted".split(" ");

        List<WordScore> pairs = new ArrayList<>();
        for (String word : words) {
            pairs.add(new WordScore(word, stringToScore(word)));
        }
        List<WordScore> pairs2 = new ArrayList<WordScore>(pairs);
        Collections.sort(pairs);
        Collections.sort(pairs2, new Comparator<WordScore>(){
            public int compare(WordScore o1, WordScore o2){
                long diff = o1.score - o2.score;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });
        assert pairs.equals(pairs2);

        Map<Integer,Integer> lower = new HashMap<Integer,Integer>();
        lower.put(-1, -1);
        int start = (int)'a';
        int end = (int)'z';
        for (int i = start ; i <= end; i++){
            lower.put(i, i - start);
        }

        words = "these are some words that will be sorted".split(" ");
        pairs = new ArrayList<WordScore>();
        for (String word : words) {
            pairs.add(new WordScore(word, stringToScoreGeneric(word, lower)));
        }
        pairs2 = new ArrayList<WordScore>(pairs);
        Collections.sort(pairs);
        Collections.sort(pairs2, new Comparator<WordScore>(){
            public int compare(WordScore o1, WordScore o2){
                long diff = o1.score - o2.score;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });
        assert pairs.equals(pairs2);

        Map<String,String> values = new HashMap<String,String>();
        values.put("test", "value");
        values.put("test2", "other");
        zaddString(conn, "key", values);
        assert conn.zscore("key", "test") == stringToScore("value");
        assert conn.zscore("key", "test2") == stringToScore("other");
    }

    public Set<String> tokenize(String content) {

        Set<String> words = new HashSet<>();
        Matcher matcher = WORDS_RE.matcher(content);
        while (matcher.find()) {
            String word = matcher.group().trim();
            if (word.length() > 2 && !STOP_WORDS.contains(word)) {
                words.add(word);
            }
        }
        return words;
    }

    public int indexDocument(Jedis conn, String docid, String content) {

        Set<String> words = tokenize(content);
        Transaction trans = conn.multi();
        for (String word : words) {
            trans.sadd("idx:" + word, docid);
        }

        return trans.exec().size();
    }

    private String setCommon(
            Transaction trans, String method, int ttl, String... items)
    {
        String[] keys = new String[items.length];
        for (int i = 0; i < items.length; i++){
            keys[i] = "idx:" + items[i];
        }

        String id = UUID.randomUUID().toString();
        if (SINTERSTORE.equals(method)) {
            trans.sinterstore("idx:" + id, keys);
        }
        if (SUNIONSTORE.equals(method)) {
            trans.sunionstore("idx:" + id, keys);
        }
        if (SDIFFSTORE.equals(method)) {
            trans.sdiffstore("idx:" + id, keys);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }

    public String zsetCommon(Transaction trans, String method, int ttl, ZParams params, String... sets) {

        String[] keys = new String[sets.length];
        for (int i = 0; i < sets.length; i++){
            keys[i] = "idx:" + sets[i];
        }

        String id = UUID.randomUUID().toString();
        if (ZINTERSTORE.equals(method)) {
            trans.zinterstore("idx:" + id, params, sets);
        }
        if (ZUNIONSTORE.equals(method)) {
            trans.zunionstore("idx:" + id, params, sets);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }

    private static final String SINTERSTORE = "sinterstore";
    public String intersect(Transaction trans, int ttl, String... items) {
        return setCommon(trans, SINTERSTORE, ttl, items);
    }

    private static final String SUNIONSTORE = "sunionstore";
    public String union(Transaction trans, int ttl, String... items) {
        return setCommon(trans, SUNIONSTORE, ttl, items);
    }

    private static final String SDIFFSTORE = "sdiffstore";
    public String difference(Transaction trans, int ttl, String... items) {
        return setCommon(trans, SDIFFSTORE, ttl, items);
    }

    private static final String ZINTERSTORE = "zinterstore";
    public String zintersect(Transaction trans, int ttl, ZParams params, String... sets) {

        return zsetCommon(trans, ZINTERSTORE, ttl, params, sets);
    }

    private static final String ZUNIONSTORE = "zunionstore";
    public String zunion(Transaction trans, int ttl, ZParams params, String... sets) {

        return zsetCommon(trans, ZUNIONSTORE, ttl, params, sets);
    }

    public Query parse(String queryString) {

        Query query = new Query();
        Set<String> current = new HashSet<>();
        Matcher matcher = QUERY_RE.matcher(queryString);
        while (matcher.find()) {
            String word = matcher.group().trim();
            char prefix = word.charAt(0);
            if (prefix == '+' || prefix == '-') {
                word = word.substring(1);
            }
            if (word.length() < 2 || STOP_WORDS.contains(word)) {
                continue;
            }
            if (prefix == '-') {
                query.unwanted.add(word);
                continue;
            }
            if (!current.isEmpty() && prefix != '+') {
                query.all.add(new ArrayList<>(current));
                current.clear();
            }
            current.add(word);
        }

        if (!current.isEmpty()) {
            query.all.add(new ArrayList<>(current));
        }
        return query;
    }

    public String parseAndSearch(Jedis conn, String queryString, int ttl) {

        Query query = parse(queryString);
        if (query.all.isEmpty()) {
            return null;
        }

        List<String> toIntersect = new ArrayList<>();
        for (List<String> syn : query.all) {
            if (syn.size() > 1) {
                Transaction trans = conn.multi();
                toIntersect.add(union(trans, ttl, syn.toArray(new String[syn.size()])));
                trans.exec();
            } else {
                toIntersect.add(syn.get(0));
            }
        }
        String interResult;
        if (toIntersect.size() > 1) {
            Transaction trans = conn.multi();
            interResult = intersect(trans, ttl, toIntersect.toArray(new String[toIntersect.size()]));
            trans.exec();
        } else {
            interResult = toIntersect.get(0);
        }

        if (!query.unwanted.isEmpty()) {
            String[] keys = query.unwanted.toArray(new String[query.unwanted.size() + 1]);
            keys[keys.length - 1] = interResult;
            Transaction trans = conn.multi();
            interResult = difference(trans, ttl, keys);
            trans.exec();
        }
        return interResult;
    }

    public SearchResult searchAndSort(Jedis conn, String queryString, String sort) {

        boolean desc = sort.startsWith("-");
        if (desc) {
            sort = sort.substring(1);
        }

        boolean alpha = !"updated".equals(sort) && !"id".equals(sort);
        String by = "kb:doc:*->" + sort;

        String id = parseAndSearch(conn, queryString, 30);

        Transaction trans = conn.multi();
        trans.scard("idx:" + id);
        SortingParams params = new SortingParams();
        if (desc) {
            params.desc();
        }
        if (alpha) {
            params.alpha();
        }
        params.by(by);
        params.limit(0, 20);
        trans.sort("idx:" + id, params);
        List<Object> results = trans.exec();

        return new SearchResult(id, ((Long)results.get(0)).longValue(), (List<String>)results.get(1));
    }

    public SearchResult searchAndZsort(Jedis conn, String queryString, boolean desc, Map<String, Integer> weights) {

        int ttl = 300;
        int start = 0;
        int num = 20;
        String id = parseAndSearch(conn, queryString, ttl);

        int updateWeight = weights.containsKey("update") ? weights.get("update") : 1;
        int voteWeight = weights.containsKey("vote") ? weights.get("vote") : 0;

        String[] keys = new String[]{id, "sort:update", "sort:votes"};
        Transaction trans = conn.multi();
        id = zintersect(trans, ttl, new ZParams().weightsByDouble(0, updateWeight, voteWeight), keys);
        trans.zcard("idx:" + id);
        if (desc) {
            trans.zrevrange("idx:" + id, start, start + num - 1);
        } else {
            trans.zrange("idx:" + id, start, start + num - 1);
        }

        List<Object> results = trans.exec();
        return new SearchResult(id, ((Long)results.get(results.size() - 2)).longValue(),
                new ArrayList<>((Set<String>)results.get(results.size() - 1)));
    }

    public long stringToScore(String word) {

        return stringToScore(word, false);
    }

    public long stringToScore(String string, boolean ignoreCase) {

        if (ignoreCase){
            string = string.toLowerCase();
        }

        List<Integer> pieces = new ArrayList<Integer>();
        for (int i = 0; i < Math.min(string.length(), 6); i++) {
            pieces.add((int)string.charAt(i));
        }
        while (pieces.size() < 6){
            pieces.add(-1);
        }

        long score = 0;
        for (int piece : pieces) {
            score = score * 257 + piece + 1;
        }

        return score * 2 + (string.length() > 6 ? 1 : 0);
    }

    public long stringToScoreGeneric(String string, Map<Integer,Integer> mapping) {
        int length = (int)(52 / (Math.log(mapping.size()) / Math.log(2)));

        List<Integer> pieces = new ArrayList<Integer>();
        for (int i = 0; i < Math.min(string.length(), length); i++) {
            pieces.add((int)string.charAt(i));
        }
        while (pieces.size() < 6){
            pieces.add(-1);
        }

        long score = 0;
        for (int piece : pieces) {
            int value = mapping.get(piece);
            score = score * mapping.size() + value + 1;
        }

        return score * 2 + (string.length() > 6 ? 1 : 0);
    }

    public long zaddString(Jedis conn, String name, Map<String,String> values) {

        Map<String, Double> pieces = new HashMap<>(values.size());
        for (Map.Entry<String,String> entry : values.entrySet()) {
            pieces.put(entry.getKey(), (double)stringToScore(entry.getValue()));
        }

        return conn.zadd(name, pieces);
    }

    public class Query {

        public final List<List<String>> all = new ArrayList<>();
        public final Set<String> unwanted = new HashSet<>();
    }

    public class SearchResult {

        public final String id;
        public final long total;
        public final List<String> results;

        public SearchResult(String id, long total, List<String> results) {
            this.id = id;
            this.total = total;
            this.results = results;
        }
    }

    public class WordScore implements Comparable<WordScore> {

        private final String word;
        private final long score;

        public WordScore(String word, long score) {
            this.word = word;
            this.score = score;
        }

        public boolean equals(Object other) {
            if (!(other instanceof WordScore)) {
                return false;
            }
            WordScore w2 = (WordScore) other;
            return this.word.equals(w2.word) && this.score == w2.score;
        }

        @Override
        public int compareTo(WordScore o) {

            if (this.word.equals(o.word)) {
                long diff = this.score - o.score;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
            return this.word.compareTo(o.word);
        }

        public String toString(){
            return word + '=' + score;
        }
    }
}
