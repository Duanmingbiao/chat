package com.chat.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ChatApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChatApplication.class, args);
        System.out.println("🚀 聊天服务启动成功！");
        System.out.println("📡 WebSocket 地址：ws://localhost:8080/chat");
    }

}
