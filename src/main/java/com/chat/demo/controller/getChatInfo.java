package com.chat.demo.controller;

import com.chat.demo.handler.ChatWebSocketHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.chat.demo.handler.ChatWebSocketHandler.USER_ID_SESSION;

@Slf4j
@RequestMapping("/chat")
@RestController
public class getChatInfo {
    @GetMapping("/Info")
    public void getUserInfo() {
        log.info(String.valueOf(ChatWebSocketHandler.USER_ID_SESSION));
        log.info(String.valueOf(ChatWebSocketHandler.ROOM_LIST));
    }
}
