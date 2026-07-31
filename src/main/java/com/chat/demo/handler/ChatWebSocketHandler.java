package com.chat.demo.handler;

import com.alibaba.fastjson.JSON;
import com.chat.demo.dao.ChatMessage;
import com.chat.demo.dao.Connect;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
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
import java.util.concurrent.TimeUnit;  // 【新增】导入 TimeUnit

@Slf4j
@Component
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private final RedisTemplate<String, Object> redisTemplate;
    public static final ConcurrentHashMap<String, WebSocketSession> USER_ID_SESSION = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, Set<String>> ROOM_LIST = new ConcurrentHashMap<>();

    public ChatWebSocketHandler(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

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
            // 【新增】用户上线后拉取离线消息
            pullOfflineMessages(session, userId);
        } else {
            USER_ID_SESSION.put(session.getId(), session);
        }
        log.info("用户 " + userId + "上线了", userId);
    }

    // 【新增】拉取离线消息方法
    private void pullOfflineMessages(WebSocketSession session, String userId) throws IOException {
        // 1. 拉取私聊离线消息
        String privateKey = "private:msg:" + userId;
        List<Object> privateMessages = redisTemplate.opsForList().range(privateKey, 0, -1);

        if (privateMessages != null && !privateMessages.isEmpty()) {
            redisTemplate.delete(privateKey);
            for (Object obj : privateMessages) {
                if (obj instanceof ChatMessage) {
                    ChatMessage msg = (ChatMessage) obj;
                    msg.setIsOffline(true);
                    session.sendMessage(new TextMessage(JSON.toJSONString(msg)));
                }
            }
            session.sendMessage(new TextMessage("您有 " + privateMessages.size() + " 条离线私聊消息"));
            log.info("用户 {} 拉取离线私聊消息 {} 条", userId, privateMessages.size());
        }

        // 2. 拉取群聊离线消息
        for (Map.Entry<String, Set<String>> entry : ROOM_LIST.entrySet()) {
            String roomId = entry.getKey();
            Set<String> members = entry.getValue();
            if (members != null && members.contains(userId)) {
                String groupKey = "group:msg:" + roomId;
                List<Object> groupMessages = redisTemplate.opsForList().range(groupKey, 0, -1);
                if (groupMessages != null && !groupMessages.isEmpty()) {
                    redisTemplate.delete(groupKey);
                    for (Object obj : groupMessages) {
                        if (obj instanceof ChatMessage) {
                            ChatMessage msg = (ChatMessage) obj;
                            msg.setIsOffline(true);
                            session.sendMessage(new TextMessage(JSON.toJSONString(msg)));
                        }
                    }
                    session.sendMessage(new TextMessage("您在群 " + roomId + " 有 " + groupMessages.size() + " 条离线消息"));
                    log.info("用户 {} 拉取群 {} 离线消息 {} 条", userId, roomId, groupMessages.size());
                }
            }
        }
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
                createGroupRoom(chatMessage.getFromUserId(), chatMessage.getRoomId(), chatMessage);
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
            // 【修改】只移除Session，不移除群成员关系
            USER_ID_SESSION.remove(userId);
            log.info("用户 {} 已下线", userId);
            // 【删除】不再从群聊中移除用户
            // for (Set<String> members : ROOM_LIST.values()) {
            //     members.remove(userId);
            // }
            // ROOM_LIST.entrySet().removeIf(entry -> entry.getValue().isEmpty());
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

        // 【新增】设置消息时间和房间ID
        chatMessage.setCreateTime(new Date());
        chatMessage.setRoomId(roomId);

        // 【修改】直接处理私聊逻辑，不再调用 createGroupRoom
        // 检查接收方是否在线
        WebSocketSession toSession = USER_ID_SESSION.get(toUserId);

        if (toSession != null && toSession.isOpen()) {
            // 在线：直接发送
            toSession.sendMessage(new TextMessage(JSON.toJSONString(chatMessage)));
            log.info("私聊消息已发送给在线用户: {}", toUserId);
        } else {
            // 离线：存储到Redis
            String key = "private:msg:" + toUserId;
            redisTemplate.opsForList().rightPush(key, chatMessage);
            redisTemplate.expire(key, 7, TimeUnit.DAYS);

            WebSocketSession fromSession = USER_ID_SESSION.get(fromUserId);
            if (fromSession != null && fromSession.isOpen()) {
                fromSession.sendMessage(new TextMessage("用户 " + toUserId + " 不在线，消息已存储"));
            }
            log.info("私聊消息已存储，接收方: {} 离线", toUserId);
        }
    }

    /**
     * 群聊多人房间发送消息
     */
    private void createGroupRoom(String fromUserId, String roomId, ChatMessage chatMessage) throws IOException {
        if (roomId == null || roomId.isEmpty()) {
            USER_ID_SESSION.get(fromUserId).sendMessage(new TextMessage("当前聊天不存在"));
            return;
        }

        // 【新增】设置消息时间
        chatMessage.setCreateTime(new Date());

        // 【新增】记录离线用户
        List<String> offlineUsers = new ArrayList<>();
        Set<String> members = ROOM_LIST.get(roomId);

        // 【修改】增加空值判断
        if (members == null || members.isEmpty()) {
            USER_ID_SESSION.get(fromUserId).sendMessage(new TextMessage("群聊为空"));
            return;
        }

        for (String userId : members) {
            WebSocketSession session = USER_ID_SESSION.get(userId);
            if (userId.equals(fromUserId)) {
                continue;
            }
            if (session != null && session.isOpen()) {
                try {
                    session.sendMessage(new TextMessage(JSON.toJSONString(chatMessage)));
                } catch (IOException e) {
                    log.error("发送消息给用户 {} 失败: {}", userId, e.getMessage());
                    // 【新增】发送失败也记录为离线
                    offlineUsers.add(userId);
                }
            } else {
                // 【修改】记录离线用户，而不是立即通知
                offlineUsers.add(userId);
                log.info("用户 {} 不在线", userId);
            }
        }

        // 【新增】批量存储离线群聊消息
        if (!offlineUsers.isEmpty()) {
            String key = "group:msg:" + roomId;
            redisTemplate.opsForList().rightPush(key, chatMessage);
            redisTemplate.expire(key, 7, TimeUnit.DAYS);

            WebSocketSession fromSession = USER_ID_SESSION.get(fromUserId);
            if (fromSession != null && fromSession.isOpen()) {
                fromSession.sendMessage(new TextMessage("有 " + offlineUsers.size() + " 个群成员不在线，消息已存储"));
            }
            log.info("群消息已存储，房间: {}, 离线用户: {}", roomId, offlineUsers);
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
        Set<String> strings = ROOM_LIST.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>());
        boolean add = strings.add(fromUserId);
        if (add) {
            session.sendMessage(new TextMessage("加入群聊成功"));
            // 【新增】加入群后拉取该群的离线消息
            String key = "group:msg:" + roomId;
            List<Object> messages = redisTemplate.opsForList().range(key, 0, -1);
            if (messages != null && !messages.isEmpty()) {
                redisTemplate.delete(key);
                for (Object obj : messages) {
                    if (obj instanceof ChatMessage) {
                        ChatMessage msg = (ChatMessage) obj;
                        msg.setIsOffline(true);
                        session.sendMessage(new TextMessage(JSON.toJSONString(msg)));
                    }
                }
                session.sendMessage(new TextMessage("您有 " + messages.size() + " 条群聊离线消息"));
                log.info("用户 {} 拉取群 {} 离线消息 {} 条", fromUserId, roomId, messages.size());
            }
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
        // 【修改】增加空值判断
        if (strings != null) {
            boolean remove = strings.remove(fromUserId);
            if (remove) {
                session.sendMessage(new TextMessage("退出群聊成功"));
                // 【新增】如果群为空，删除群
                if (strings.isEmpty()) {
                    ROOM_LIST.remove(roomId);
                    log.info("群聊 {} 已空，自动删除", roomId);
                }
            } else {
                session.sendMessage(new TextMessage("你已退出群聊"));
            }
        } else {
            session.sendMessage(new TextMessage("群聊不存在"));
        }
    }
}
