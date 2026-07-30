package com.chat.demo.dao;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;

@Data
public class ChatMessage {
    private String type;
    private String fromUserId;
    private String toUserId;    // 私聊时使用
    private String roomId;
    private String content;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;
}