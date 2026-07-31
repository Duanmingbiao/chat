package com.chat.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

@SpringBootTest
class ChatApplicationTests {
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    @Test
    void contextLoads() {
        redisTemplate.opsForList().leftPush("list", "1");
        redisTemplate.opsForList().leftPush("list", "2");
        System.out.println(redisTemplate.opsForList().rightPop("list"));
    }

}
