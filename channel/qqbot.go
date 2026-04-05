package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"message-pusher/common"
	"message-pusher/model"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const qqBotTokenRefreshBuffer = 60
const qqBotOpenIDListenTimeout = 10 * time.Minute

var qqBotTokenStore sync.Map

var qqBotDetectSessions sync.Map

type qqBotTokenStoreItem struct {
	AppID        string
	ClientSecret string
	AccessToken  string
	ExpiresAt    int64
	Mutex        sync.Mutex
}

type qqBotTokenRequest struct {
	AppID        string `json:"appId"`
	ClientSecret string `json:"clientSecret"`
}

type qqBotTokenResponse struct {
	AccessToken string      `json:"access_token"`
	ExpiresIn   interface{} `json:"expires_in"`
	Code        int         `json:"code"`
	Message     string      `json:"message"`
}

type qqBotGatewayResponse struct {
	URL     string `json:"url"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type qqBotMessageRequest struct {
	MsgType int    `json:"msg_type"`
	Content string `json:"content"`
}

type qqBotMessageResponse struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Code      int    `json:"code"`
	Message   string `json:"message"`
}

type qqBotWSPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int            `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type qqBotHelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type qqBotReadyData struct {
	SessionID string `json:"session_id"`
}

type qqBotIdentifyPayload struct {
	Op int `json:"op"`
	D  struct {
		Token   string `json:"token"`
		Intents int    `json:"intents"`
		Shard   [2]int `json:"shard"`
	} `json:"d"`
}

type qqBotHeartbeatPayload struct {
	Op int  `json:"op"`
	D  *int `json:"d,omitempty"`
}

type qqBotDispatchEvent struct {
	ID     string `json:"id"`
	Content string `json:"content"`
	Author struct {
		ID           string `json:"id"`
		UserOpenID   string `json:"user_openid"`
		MemberOpenID string `json:"member_openid"`
	} `json:"author"`
	UserOpenID        string `json:"user_openid"`
	GroupMemberOpenID string `json:"group_member_openid"`
}

type qqBotDetectResult struct {
	OpenID    string `json:"openid"`
	EventType string `json:"event_type"`
	MessageID string `json:"message_id"`
	Content   string `json:"content"`
}

type qqBotDetectSession struct {
	key      string
	resultCh chan qqBotDetectResult
	stopCh   chan struct{}
	closeOnce sync.Once
}

func newQQBotDetectSession(key string) *qqBotDetectSession {
	return &qqBotDetectSession{
		key:      key,
		resultCh: make(chan qqBotDetectResult, 1),
		stopCh:   make(chan struct{}),
	}
}

func (s *qqBotDetectSession) stop() {
	s.closeOnce.Do(func() {
		close(s.stopCh)
		qqBotDetectSessions.Delete(s.key)
	})
}

func (s *qqBotDetectSession) deliver(result qqBotDetectResult) bool {
	select {
	case s.resultCh <- result:
		return true
	default:
		return false
	}
}

func getQQBotMessageText(message *model.Message) string {
	if message.Content != "" {
		return message.Content
	}
	if message.Description != "" {
		return message.Description
	}
	return message.Title
}

func getQQBotTarget(message *model.Message, channel_ *model.Channel) string {
	if message.To != "" {
		return message.To
	}
	if message.OpenId != "" {
		return message.OpenId
	}
	return channel_.AccountId
}

func parseQQBotExpiresIn(raw interface{}) (int64, error) {
	switch v := raw.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		seconds, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return seconds, nil
	default:
		return 0, errors.New("无法解析 QQBot expires_in 字段")
	}
}

func (i *qqBotTokenStoreItem) getAccessToken() (string, error) {
	i.Mutex.Lock()
	defer i.Mutex.Unlock()
	if i.AccessToken != "" && time.Now().Unix() < i.ExpiresAt-qqBotTokenRefreshBuffer {
		return i.AccessToken, nil
	}
	requestBody, err := json.Marshal(qqBotTokenRequest{
		AppID:        i.AppID,
		ClientSecret: i.ClientSecret,
	})
	if err != nil {
		return "", err
	}
	resp, err := http.Post("https://bots.qq.com/app/getAppAccessToken", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var res qqBotTokenResponse
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		if res.Message != "" {
			return "", errors.New(res.Message)
		}
		return "", errors.New(resp.Status)
	}
	if res.AccessToken == "" {
		if res.Message != "" {
			return "", errors.New(res.Message)
		}
		return "", errors.New("QQ 机器人 access token 为空")
	}
	expiresIn, err := parseQQBotExpiresIn(res.ExpiresIn)
	if err != nil {
		return "", err
	}
	i.AccessToken = res.AccessToken
	i.ExpiresAt = time.Now().Unix() + expiresIn
	return i.AccessToken, nil
}

func getQQBotTokenStoreItem(channel_ *model.Channel) *qqBotTokenStoreItem {
	key := channel_.AppId + "|" + channel_.Secret
	if item, ok := qqBotTokenStore.Load(key); ok {
		return item.(*qqBotTokenStoreItem)
	}
	item := &qqBotTokenStoreItem{AppID: channel_.AppId, ClientSecret: channel_.Secret}
	actual, _ := qqBotTokenStore.LoadOrStore(key, item)
	return actual.(*qqBotTokenStoreItem)
}

func getQQBotGatewayURL(accessToken string) (string, error) {
	req, err := http.NewRequest("GET", "https://api.sgroup.qq.com/gateway", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "QQBot "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var res qqBotGatewayResponse
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		if res.Message != "" {
			return "", errors.New(res.Message)
		}
		return "", errors.New(resp.Status)
	}
	if strings.TrimSpace(res.URL) == "" {
		if res.Message != "" {
			return "", errors.New(res.Message)
		}
		return "", errors.New("QQ 机器人 gateway 地址为空")
	}
	return res.URL, nil
}

func getQQBotIntents() int {
	const (
		publicGuildMessages = 1 << 30
		directMessage       = 1 << 12
		groupAndC2C         = 1 << 25
		interaction         = 1 << 26
	)
	return publicGuildMessages | directMessage | groupAndC2C | interaction
}

func extractQQBotOpenID(eventType string, event qqBotDispatchEvent) string {
	switch eventType {
	case "C2C_MESSAGE_CREATE":
		return strings.TrimSpace(event.Author.UserOpenID)
	case "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE":
		return strings.TrimSpace(event.Author.MemberOpenID)
	case "DIRECT_MESSAGE_CREATE", "AT_MESSAGE_CREATE":
		return strings.TrimSpace(event.Author.ID)
	default:
		for _, value := range []string{
			event.Author.UserOpenID,
			event.Author.MemberOpenID,
			event.Author.ID,
			event.UserOpenID,
			event.GroupMemberOpenID,
		} {
			value = strings.TrimSpace(value)
			if value != "" {
				return value
			}
		}
		return ""
	}
}

func WaitForQQBotOpenID(appID string, clientSecret string, timeout time.Duration) (*qqBotDetectResult, error) {
	appID = strings.TrimSpace(appID)
	clientSecret = strings.TrimSpace(clientSecret)
	if appID == "" || clientSecret == "" {
		return nil, errors.New("APP_ID 或 CLIENT_SECRET 为空")
	}
	if timeout <= 0 {
		timeout = qqBotOpenIDListenTimeout
	}

	key := appID + "|" + clientSecret
	if _, exists := qqBotDetectSessions.Load(key); exists {
		return nil, errors.New("该 APP_ID 当前已有 OPENID 识别任务在运行，请稍后再试")
	}

	session := newQQBotDetectSession(key)
	actual, loaded := qqBotDetectSessions.LoadOrStore(key, session)
	if loaded {
		return nil, errors.New("该 APP_ID 当前已有 OPENID 识别任务在运行，请稍后再试")
	}
	session = actual.(*qqBotDetectSession)
	defer session.stop()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runQQBotOpenIDDetector(ctx, session, appID, clientSecret)
	}()

	select {
	case result := <-session.resultCh:
		return &result, nil
	case err := <-errCh:
		if err == nil {
			return nil, errors.New("QQBot 监听已结束，但未捕获到 OPENID")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("等待超时：%s 内未收到任何用户消息", timeout.String())
		}
		if errors.Is(err, context.Canceled) {
			return nil, errors.New("QQBot OPENID 监听已取消")
		}
		return nil, err
	}
}

func runQQBotOpenIDDetector(ctx context.Context, session *qqBotDetectSession, appID string, clientSecret string) error {
	item := &qqBotTokenStoreItem{AppID: appID, ClientSecret: clientSecret}
	accessToken, err := item.getAccessToken()
	if err != nil {
		return err
	}
	gatewayURL, err := getQQBotGatewayURL(accessToken)
	if err != nil {
		return err
	}

	common.SysLog(fmt.Sprintf("QQBot OPENID detector started for app_id=%s", appID))
	dialer := websocket.Dialer{HandshakeTimeout: 20 * time.Second}
	conn, _, err := dialer.DialContext(ctx, gatewayURL, http.Header{})
	if err != nil {
		return err
	}
	defer func() {
		deadline := time.Now().Add(3 * time.Second)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"), deadline)
		_ = conn.Close()
		common.SysLog(fmt.Sprintf("QQBot OPENID detector closed for app_id=%s", appID))
	}()

	stopHeartbeat := make(chan struct{})
	var heartbeatOnce sync.Once
	stopHeartbeatLoop := func() {
		heartbeatOnce.Do(func() {
			close(stopHeartbeat)
		})
	}
	defer stopHeartbeatLoop()

	go func() {
		<-ctx.Done()
		stopHeartbeatLoop()
		_ = conn.SetReadDeadline(time.Now())
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "timeout"), time.Now().Add(2*time.Second))
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-session.stopCh:
				return context.Canceled
			default:
				return err
			}
		}

		var payload qqBotWSPayload
		if err = json.Unmarshal(data, &payload); err != nil {
			continue
		}

		switch payload.Op {
		case 10:
			var hello qqBotHelloData
			if err = json.Unmarshal(payload.D, &hello); err != nil {
				return err
			}
			identify := qqBotIdentifyPayload{Op: 2}
			identify.D.Token = "QQBot " + accessToken
			identify.D.Intents = getQQBotIntents()
			identify.D.Shard = [2]int{0, 1}
			if err = conn.WriteJSON(identify); err != nil {
				return err
			}
			if hello.HeartbeatInterval > 0 {
				go qqBotHeartbeatLoop(conn, time.Duration(hello.HeartbeatInterval)*time.Millisecond, payload.S, stopHeartbeat)
			}
		case 0:
			if payload.T == "READY" {
				var ready qqBotReadyData
				_ = json.Unmarshal(payload.D, &ready)
				common.SysLog(fmt.Sprintf("QQBot OPENID detector ready for app_id=%s session_id=%s", appID, ready.SessionID))
				continue
			}
			if payload.T == "RESUMED" {
				continue
			}
			if !isQQBotMessageEvent(payload.T) {
				continue
			}
			var event qqBotDispatchEvent
			if err = json.Unmarshal(payload.D, &event); err != nil {
				continue
			}
			openid := extractQQBotOpenID(payload.T, event)
			if openid == "" {
				continue
			}
			session.deliver(qqBotDetectResult{
				OpenID:    openid,
				EventType: payload.T,
				MessageID: event.ID,
				Content:   event.Content,
			})
			return nil
		case 7:
			return errors.New("QQBot websocket 请求重新鉴权")
		case 9:
			return errors.New("QQBot websocket session 无效")
		}

		select {
		case <-session.stopCh:
			return context.Canceled
		default:
		}
	}
}

func qqBotHeartbeatLoop(conn *websocket.Conn, interval time.Duration, lastSeq *int, stopCh <-chan struct{}) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			payload := qqBotHeartbeatPayload{Op: 1, D: lastSeq}
			_ = conn.WriteJSON(payload)
		case <-stopCh:
			return
		}
	}
}

func isQQBotMessageEvent(eventType string) bool {
	switch eventType {
	case "C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE", "DIRECT_MESSAGE_CREATE", "AT_MESSAGE_CREATE":
		return true
	default:
		return false
	}
}

func SendQQBotMessage(message *model.Message, user *model.User, channel_ *model.Channel) error {
	target := strings.TrimSpace(getQQBotTarget(message, channel_))
	if target == "" {
		return errors.New("QQBot OpenID 为空")
	}
	content := strings.TrimSpace(getQQBotMessageText(message))
	if content == "" {
		return errors.New("QQBot 消息内容为空")
	}
	item := getQQBotTokenStoreItem(channel_)
	accessToken, err := item.getAccessToken()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(qqBotMessageRequest{MsgType: 0, Content: content})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.sgroup.qq.com/v2/users/%s/messages", target)
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "QQBot "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var res qqBotMessageResponse
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if res.Message != "" {
			return errors.New(res.Message)
		}
		return errors.New(resp.Status)
	}
	if res.ID == "" {
		if res.Message != "" {
			return errors.New(res.Message)
		}
		common.SysLog("QQBot message sent without message id")
	}
	return nil
}
