package com.zhaoxiang.redis.redis_in_action.chapter01;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * Author: Rivers
 * Date: 2017/8/5 08:31
 */
public class RedditSample {

    /**
     * 文章投票有效期
     */
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;

    /**
     * 投一次票，该文章增加的分数
     */
    private static final int VOTE_SCORE = 432;

    /**
     * 每页文章数
     */
    private static final int ARTICLE_PER_PAGE = 25;

    public static void main(String[] args) {

        new RedditSample().execute();
    }

    public void execute() {

        Jedis conn = new Jedis("119.23.26.77", 6379);
        conn.auth("zhaoxiang@85&35");

        String articleId = postArticle(conn, "rivers", "Hello Redis", "http://www.yoxiang.me");
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println(" " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println("===========================================");

        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("Now, the article vote num is " + votes);

        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);

        System.out.println("===========================================");

        addGroups(conn, articleId, new String[]{"new-group"});
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
    }

    /**
     * 发布文章
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {

        String articleId = String.valueOf(conn.incr("article:"));

        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        Map<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("url", link);
        articleData.put("poster", user);
        articleData.put("time", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);

        conn.zadd("score:", now + VOTE_SCORE, article);
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 投票
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article) {

        long cutoff = System.currentTimeMillis() / 1000 - ONE_WEEK_IN_SECONDS;
        // 超过一周，不能再投票
        if (conn.zscore("time:", article) < cutoff) {
            return;
        }

        String articleId = article.substring(article.indexOf(":") + 1);
        // 添加投票用户
        conn.sadd("voted:" + articleId, user);
        // 增加文章分数以及投票数
        conn.zincrby("score:", VOTE_SCORE, article);
        conn.hincrBy(article, "votes", 1);
    }

    /**
     * 获取文章-分页
     * @param conn
     * @param page
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    /**
     * 获取文章-分页，按照字段排序
     * @param conn
     * @param page
     * @param order
     * @return
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page, String order) {

        int start = (page - 1) * ARTICLE_PER_PAGE;
        int end = start + ARTICLE_PER_PAGE - 1;

        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }
        return articles;
    }

    /**
     * 打印文章信息
     * @param articles
     */
    public void printArticles(List<Map<String, String>> articles) {

        for (Map<String, String> article : articles) {
            System.out.println("===== print article start");
            System.out.println(">>>>>> id : " + article.get("id"));
            for (Map.Entry<String, String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")) {
                    continue;
                }
                System.out.println(">>>>>> " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    public void addGroups(Jedis conn, String articleId, String[] toAdd) {

        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {

        return getGroupArticles(conn, group, page, "score:");
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {

        String key = order + group;
        if (!conn.exists(key)) {
            ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, zParams, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }
}
