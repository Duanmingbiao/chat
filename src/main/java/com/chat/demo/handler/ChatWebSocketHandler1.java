package com.chat.demo.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class ChatWebSocketHandler1 extends TextWebSocketHandler {

    // ===================== 数据存储 =====================

    /**
     * 用户连接池：userId → WebSocketSession
     */
    private static final ConcurrentHashMap<String, WebSocketSession> USER_SESSION_MAP = new ConcurrentHashMap<>();

    /**
     * 房间管理：roomId → 该房间内所有用户的ID集合
     */
    private static final ConcurrentHashMap<String, Set<String>> ROOMS = new ConcurrentHashMap<>();

    /**
     * 反向映射：sessionId → userId（用于 O(1) 查找）
     */
    private static final ConcurrentHashMap<String, String> SESSION_USER_MAP = new ConcurrentHashMap<>();

    // ===================== WebSocket 生命周期 =====================

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        String userId = extractUserId(session);
        USER_SESSION_MAP.put(userId, session);
        SESSION_USER_MAP.put(session.getId(), userId);
        broadcastSystemMessage("【系统】" + userId + " 上线了");
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        // 处理心跳
        if ("ping".equals(payload)) {
            session.sendMessage(new TextMessage("pong"));
            return;
        }

        JSONObject json;
        try {
            json = JSON.parseObject(payload);
        } catch (Exception e) {
            broadcastSystemMessage("【广播】" + payload);
            return;
        }

        String type = json.getString("type");
        String fromUserId = json.getString("fromUserId");

        if (fromUserId == null || fromUserId.isEmpty()) {
            sendError(session, "缺少 fromUserId");
            return;
        }

        // ========== 根据类型分发 ==========
        if ("private".equals(type)) {
            handlePrivateMessage(session, json, fromUserId);
        } else if ("group".equals(type)) {
            handleGroupMessage(session, json, fromUserId);
        } else if ("join".equals(type)) {
            handleJoinRoom(session, json, fromUserId);
        } else if ("leave".equals(type)) {
            handleLeaveRoom(session, json, fromUserId);
        } else {
            String content = json.getString("content");
            if (content != null) {
                broadcastSystemMessage("【广播】" + fromUserId + "：" + content);
            } else {
                sendError(session, "未知的消息类型");
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String userId = SESSION_USER_MAP.remove(session.getId());
        if (userId != null) {
            USER_SESSION_MAP.remove(userId);
            System.out.println("❌ 用户 " + userId + " 下线，当前在线人数：" + USER_SESSION_MAP.size());

            for (Set<String> members : ROOMS.values()) {
                members.remove(userId);
            }
            ROOMS.entrySet().removeIf(entry -> entry.getValue().isEmpty());

            broadcastSystemMessage("【系统】" + userId + " 下线了");
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        System.err.println("⚠️ 传输异常：" + exception.getMessage());
        if (session.isOpen()) {
            session.close(CloseStatus.SERVER_ERROR);
        }
    }

    // ===================== 消息处理器 =====================

    /**
     * 私聊：自动创建两人房间
     */
    private void handlePrivateMessage(WebSocketSession session, JSONObject json, String fromUserId) throws Exception {
        String toUserId = json.getString("toUserId");
        String content = json.getString("content");

        if (toUserId == null || toUserId.isEmpty()) {
            sendError(session, "私聊缺少 toUserId");
            return;
        }
        if (content == null || content.isEmpty()) {
            sendError(session, "消息内容不能为空");
            return;
        }

        // 生成顺序无关的房间ID
        String roomId = getPrivateRoomId(fromUserId, toUserId);

        // 自动创建/加入房间
        Set<String> members = ROOMS.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>());
        members.add(fromUserId);
        members.add(toUserId);

        // 检查目标是否在线
        WebSocketSession targetSession = USER_SESSION_MAP.get(toUserId);
        if (targetSession != null && targetSession.isOpen()) {
            JSONObject response = new JSONObject();
            response.put("type", "private");
            response.put("roomId", roomId);
            response.put("fromUserId", fromUserId);
            response.put("content", content);
            response.put("timestamp", System.currentTimeMillis());

            TextMessage textMsg = new TextMessage(response.toJSONString());
            for (String memberId : members) {
                if (memberId.equals(fromUserId)) continue;
                WebSocketSession memberSession = USER_SESSION_MAP.get(memberId);
                if (memberSession != null && memberSession.isOpen()) {
                    memberSession.sendMessage(textMsg);
                }
            }
        } else {
            JSONObject error = new JSONObject();
            error.put("type", "error");
            error.put("msg", "用户 " + toUserId + " 不在线");
            session.sendMessage(new TextMessage(error.toJSONString()));
        }
    }

    /**
     * 群聊
     */
    private void handleGroupMessage(WebSocketSession session, JSONObject json, String fromUserId) throws Exception {
        String roomId = json.getString("roomId");
        String content = json.getString("content");

        if (roomId == null || roomId.isEmpty()) {
            sendError(session, "群聊缺少 roomId");
            return;
        }
        if (content == null || content.isEmpty()) {
            sendError(session, "消息内容不能为空");
            return;
        }

        Set<String> members = ROOMS.get(roomId);
        if (members == null || members.isEmpty()) {
            sendError(session, "房间 " + roomId + " 不存在或为空");
            return;
        }

        JSONObject response = new JSONObject();
        response.put("type", "group");
        response.put("roomId", roomId);
        response.put("fromUserId", fromUserId);
        response.put("content", content);
        response.put("timestamp", System.currentTimeMillis());

        TextMessage textMsg = new TextMessage(response.toJSONString());
        for (String memberId : members) {
            if (memberId.equals(fromUserId)) continue;
            WebSocketSession memberSession = USER_SESSION_MAP.get(memberId);
            if (memberSession != null && memberSession.isOpen()) {
                memberSession.sendMessage(textMsg);
            }
        }
        System.out.println("📢 群聊 [" + roomId + "]：" + fromUserId + "：" + content);
    }

    /**
     * 加入房间
     */
    private void handleJoinRoom(WebSocketSession session, JSONObject json, String fromUserId) throws Exception {
        String roomId = json.getString("roomId");
        if (roomId == null || roomId.isEmpty()) {
            sendError(session, "加入房间缺少 roomId");
            return;
        }

        Set<String> members = ROOMS.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>());
        boolean added = members.add(fromUserId);

        if (added) {
            System.out.println("🚪 " + fromUserId + " 加入房间：" + roomId + "，当前人数：" + members.size());
            JSONObject response = new JSONObject();
            response.put("type", "system");
            response.put("msg", "你已加入房间 " + roomId);
            session.sendMessage(new TextMessage(response.toJSONString()));
            roomBroadcast(roomId, "【系统】" + fromUserId + " 加入了房间", fromUserId);
        } else {
            sendError(session, "你已在房间 " + roomId + " 中");
        }
    }

    /**
     * 退出房间
     */
    private void handleLeaveRoom(WebSocketSession session, JSONObject json, String fromUserId) throws Exception {
        String roomId = json.getString("roomId");
        if (roomId == null || roomId.isEmpty()) {
            sendError(session, "退出房间缺少 roomId");
            return;
        }

        Set<String> members = ROOMS.get(roomId);
        if (members == null) {
            sendError(session, "房间 " + roomId + " 不存在");
            return;
        }

        boolean removed = members.remove(fromUserId);
        if (removed) {
            System.out.println("🚪 " + fromUserId + " 退出房间：" + roomId);
            JSONObject response = new JSONObject();
            response.put("type", "system");
            response.put("msg", "你已退出房间 " + roomId);
            session.sendMessage(new TextMessage(response.toJSONString()));
            roomBroadcast(roomId, "【系统】" + fromUserId + " 退出了房间", fromUserId);
            if (members.isEmpty()) {
                ROOMS.remove(roomId);
            }
        } else {
            sendError(session, "你不在房间 " + roomId + " 中");
        }
    }

    // ===================== 辅助方法 =====================

    /**
     * 生成私聊房间 ID（顺序无关）
     */
    private String getPrivateRoomId(String user1, String user2) {
        String[] users = {user1, user2};
        Arrays.sort(users);
        return "private_" + users[0] + "_" + users[1];
    }

    /**
     * 房间内广播
     */
    private void roomBroadcast(String roomId, String msg, String excludeUserId) {
        Set<String> members = ROOMS.get(roomId);
        if (members == null) return;
        TextMessage textMsg = new TextMessage(msg);
        for (String memberId : members) {
            if (memberId.equals(excludeUserId)) continue;
            WebSocketSession session = USER_SESSION_MAP.get(memberId);
            if (session != null && session.isOpen()) {
                try {
                    session.sendMessage(textMsg);
                } catch (Exception e) {
                    System.err.println("房间广播失败：" + e.getMessage());
                }
            }
        }
    }

    /**
     * 全局广播
     */
    private void broadcastSystemMessage(String msg) {
        TextMessage textMsg = new TextMessage(msg);
        for (WebSocketSession session : USER_SESSION_MAP.values()) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(textMsg);
                }
            } catch (Exception e) {
                System.err.println("系统广播失败：" + e.getMessage());
            }
        }
    }

    /**
     * 发送错误
     */
    private void sendError(WebSocketSession session, String msg) throws Exception {
        JSONObject error = new JSONObject();
        error.put("type", "error");
        error.put("msg", msg);
        session.sendMessage(new TextMessage(error.toJSONString()));
    }

    /**
     * 提取 userId
     */
    private String extractUserId(WebSocketSession session) {
        String query = session.getUri().getQuery();
        if (query != null && query.startsWith("userId=")) {
            return query.substring(7);
        }
        return session.getId();
    }

    // ===================== 对外接口 =====================

    public static ConcurrentHashMap<String, WebSocketSession> getUserSessionMap() {
        return new ConcurrentHashMap<>(USER_SESSION_MAP);
    }

    public static ConcurrentHashMap<String, Set<String>> getRooms() {
        return new ConcurrentHashMap<>(ROOMS);
    }

    public static int getOnlineCount() {
        return USER_SESSION_MAP.size();
    }

    public static int getRoomCount() {
        return ROOMS.size();
    }
}