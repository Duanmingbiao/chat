package com.chat.demo.handler;

import com.alibaba.fastjson.JSON;
import com.chat.demo.dao.ChatMessage;
import com.chat.demo.dao.Connect;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public class ChatWebSocketHandler extends TextWebSocketHandler {
    public static final ConcurrentHashMap<String, WebSocketSession> USER_ID_SESSION = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, Set<String>> ROOM_LIST = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        URI uri = session.getUri();
        UriComponents uriComponents = UriComponentsBuilder.fromUri(uri).build();
        MultiValueMap<String, String> queryParams = uriComponents.getQueryParams();
        Map<String, String> paramMap = new HashMap<>();
        queryParams.forEach((key, values) -> {
            if (values != null && !values.isEmpty()) {
                paramMap.put(key, values.get(0));
            }
        });
        ObjectMapper mapper = new ObjectMapper();
        Connect connect = mapper.convertValue(paramMap, Connect.class);
        String userId = connect.getUserId();
        if (userId != null && !userId.isEmpty()) {
            USER_ID_SESSION.put(userId, session);
        } else {
            USER_ID_SESSION.put(session.getId(), session);
        }
        log.info("用户 " + userId + "上线了", userId);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        ChatMessage chatMessage = JSON.parseObject(payload, ChatMessage.class);
        String type = chatMessage.getType();
        switch (type) {
            case "ping":
                session.sendMessage(new TextMessage("pong"));
                break;
            case "private":
                log.info("私聊");
                createPrivateRoom(chatMessage.getFromUserId(), chatMessage.getToUserId(), chatMessage);
                break;
            case "group":
                createGroupRoom(chatMessage.getFromUserId(),chatMessage.getRoomId(), chatMessage);
                log.info("群聊");
                break;
            case "leave":
                handleLeaveRoom(session, chatMessage.getFromUserId(), chatMessage);
                log.info("离开");
                break;
            case "join":
                handleJoinRoom(session, chatMessage.getFromUserId(), chatMessage);
                log.info("加入");
                break;
            default:
                log.info("未知消息");
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("传输异常：{}", exception.getMessage());
        if (session.isOpen()) {
            session.close(CloseStatus.SERVER_ERROR);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String userId = null;
        for (Map.Entry<String, WebSocketSession> entry : USER_ID_SESSION.entrySet()) {
            if (entry.getValue().equals(session)) {
                userId = entry.getKey();
                break;
            }
        }
        if (userId != null) {
            log.info("用户 {} 已下线", userId);
        }
    }

    /**
     * 私聊创建2人房间
     */
    private void createPrivateRoom(String fromUserId, String toUserId, ChatMessage chatMessage) throws IOException {
        String[] array = {fromUserId, toUserId};
        Arrays.sort(array);
        String roomId = "roomId_private_" + array[0] + "_" + array[1];
        HashSet<String> SetArray = new HashSet<>();
        SetArray.add(fromUserId);
        SetArray.add(toUserId);
        ROOM_LIST.put(roomId, SetArray);
        createGroupRoom(fromUserId, roomId, chatMessage);
    }

    /**
     * 群聊多人房间发送消息
     */
    private void createGroupRoom(String fromUserId,String roomId ,ChatMessage chatMessage) throws IOException {
        if (roomId == null || roomId.isEmpty()) {
            USER_ID_SESSION.get(fromUserId).sendMessage(new TextMessage("群聊不存在"));
            return;
        }
        for (String userId : ROOM_LIST.get(roomId)) {
            WebSocketSession session = USER_ID_SESSION.get(userId);
            if(userId.equals(fromUserId)){
                continue;
            }
            if (session != null && session.isOpen()) {
                try {
                    session.sendMessage(new TextMessage(JSON.toJSONString(chatMessage)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                log.info("用户 " + userId + " 不在线,请稍后重试");
            }
            ;
        }
    }

    /**
     * 加入群聊房间
     */
    private void handleJoinRoom(WebSocketSession session, String fromUserId, ChatMessage chatMessage) throws Exception {
        String roomId = chatMessage.getRoomId();
        if (roomId == null || roomId.isEmpty()) {
            session.sendMessage(new TextMessage("群聊id为空"));
            return;
        }
        //ConcurrentHashMap 的一个原子性方法，意思是：“如果指定的 key（房间号）不存在，就执行后面的函数创建一个新值，并存入 Map；如果已经存在，就直接返回旧值”
        Set<String> strings = ROOM_LIST.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>());
        boolean add = strings.add(fromUserId);
        if (add) {
            session.sendMessage(new TextMessage("加入群聊成功"));
        } else {
            session.sendMessage(new TextMessage("你已在群聊内"));
        }
    }

    /**
     * 退出群聊
     */
    private void handleLeaveRoom(WebSocketSession session, String fromUserId, ChatMessage chatMessage) throws Exception {
        String roomId = chatMessage.getRoomId();
        if (roomId == null || roomId.isEmpty()) {
            session.sendMessage(new TextMessage("群聊id为空"));
            return;
        }
        Set<String> strings = ROOM_LIST.get(roomId);
        boolean remove = strings.remove(fromUserId);
        if (remove) {
            session.sendMessage(new TextMessage("退出群聊成功"));
        } else {
            session.sendMessage(new TextMessage("你已退出群聊"));
        }
    }
}