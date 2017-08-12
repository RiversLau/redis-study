package com.zhaoxiang.redis.redis_in_action.chapter06;

import java.util.List;
import java.util.Map;

/**
 * Author: Rivers
 * Date: 2017/8/12 15:43
 */
public class ChatMessage {

    private String chatId;
    private List<Map<String, Object>> messages;

    public ChatMessage(String chatId, List<Map<String, Object>> messages) {
        this.chatId = chatId;
        this.messages = messages;
    }

    public String getChatId() {
        return chatId;
    }

    public void setChatId(String chatId) {
        this.chatId = chatId;
    }

    public List<Map<String, Object>> getMessages() {
        return messages;
    }

    public void setMessages(List<Map<String, Object>> messages) {
        this.messages = messages;
    }

    public boolean equals(Object other) {

        if (!(other instanceof ChatMessage)){
            return false;
        }
        ChatMessage otherCm = (ChatMessage)other;
        return chatId.equals(otherCm.chatId) &&
                messages.equals(otherCm.messages);
    }
}
