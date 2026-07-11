package grok

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/websocket"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// imagineSlot tracks one in-flight image generation slot.
type imagineSlot struct {
	imageID  string
	order    int
	width    int
	height   int
	lastBlob string
	lastURL  string
	done     bool
	progress int
}

// ImagineStream handles the WS-based image generation protocol.
type ImagineStream struct {
	token    string
	proxyURL string
}

// NewImagineStream creates an ImagineStream for the given SSO token.
func NewImagineStream(token string) *ImagineStream {
	cfg := config.Global()
	proxyURL := cfg.GetStr("proxy.egress.proxy_url", "")
	return &ImagineStream{
		token:    token,
		proxyURL: proxyURL,
	}
}

// StreamImages connects to the WS imagine endpoint and sends events to the
// returned channel until *n* final images are collected or an error occurs.
func (s *ImagineStream) StreamImages(prompt, aspectRatio string, n int, enableNSFW, enablePro bool) <-chan ImagineEvent {
	ch := make(chan ImagineEvent, 64)
	go func() {
		defer close(ch)
		collected := 0
		for collected < n {
			needed := n - collected
			wsClosed, roundCollected := s.runRounds(ch, prompt, aspectRatio, needed, enableNSFW, enablePro)
			collected += roundCollected
			if collected >= n {
				return
			}
			if !wsClosed {
				return
			}
		}
	}()
	return ch
}

// runRounds connects to the WS and runs image rounds until done or WS closes.
func (s *ImagineStream) runRounds(ch chan<- ImagineEvent, prompt, aspectRatio string, needed int, enableNSFW, enablePro bool) (wsClosed bool, collected int) {
	headers := s.buildWSHeaders()
	dialer := &websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
	}
	if s.proxyURL != "" {
		if proxyURL, err := url.Parse(s.proxyURL); err == nil {
			dialer.Proxy = fhttp.ProxyURL(proxyURL)
		}
	}

	conn, _, err := dialer.Dial(WSImagineURL, headers)
	if err != nil {
		ch <- ImagineEvent{Type: ImagineEventError, Error: fmt.Sprintf("WS imagine connect failed: %v", err)}
		return true, 0
	}
	defer conn.Close()

	for collected < needed {
		wc, rc := s.runOneRound(conn, ch, prompt, aspectRatio, needed-collected, enableNSFW, enablePro)
		collected += rc
		if collected >= needed || wc {
			return wc, collected
		}
	}
	return false, collected
}

// runOneRound sends reset+prompt and processes frames until all slots
// complete or the WS closes.
func (s *ImagineStream) runOneRound(conn *websocket.Conn, ch chan<- ImagineEvent, prompt, aspectRatio string, needed int, enableNSFW, enablePro bool) (wsClosed bool, collected int) {
	requestID := genUUID()

	if err := conn.WriteJSON(BuildImagineResetMessage()); err != nil {
		return true, 0
	}
	if err := conn.WriteJSON(BuildImagineRequestMessage(requestID, prompt, aspectRatio, enableNSFW, enablePro)); err != nil {
		return true, 0
	}

	slots := map[string]*imagineSlot{}
	roundStart := time.Now()
	timeout := 120 * time.Second

	for {
		if time.Since(roundStart) >= timeout {
			for _, slot := range slots {
				if !slot.done && slot.lastBlob != "" {
					collected++
					ch <- ImagineEvent{
						Type: ImagineEventImage, ImageID: slot.imageID,
						Order: slot.order, URL: slot.lastURL, Blob: slot.lastBlob,
						Width: slot.width, Height: slot.height, IsFinal: true,
					}
				}
			}
			return false, collected
		}

		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		msgType, data, rerr := conn.ReadMessage()
		if rerr != nil {
			for _, slot := range slots {
				if !slot.done && slot.lastBlob != "" {
					collected++
					ch <- ImagineEvent{
						Type: ImagineEventImage, ImageID: slot.imageID,
						Order: slot.order, URL: slot.lastURL, Blob: slot.lastBlob,
						Width: slot.width, Height: slot.height, IsFinal: true,
					}
				}
			}
			return true, collected
		}
		if msgType != websocket.TextMessage {
			continue
		}

		var msg map[string]any
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		msgTypeStr, _ := msg["type"].(string)
		switch msgTypeStr {
		case "json":
			parsed := ParseImagineJSONFrame(msg)
			if parsed == nil {
				continue
			}
			switch parsed.Status {
			case "start_stage":
				slots[parsed.ImageID] = &imagineSlot{
					imageID: parsed.ImageID, order: parsed.Order,
					width: parsed.Width, height: parsed.Height, progress: 10,
				}
				ch <- ImagineEvent{
					Type: ImagineEventProgress, ImageID: parsed.ImageID,
					Order: parsed.Order, Progress: 10,
				}

			case "completed":
				slot, ok := slots[parsed.ImageID]
				if !ok || slot.done {
					continue
				}
				slot.done = true
				if parsed.Moderated {
					ch <- ImagineEvent{
						Type: ImagineEventModerated, ImageID: parsed.ImageID,
						Order: slot.order, Moderated: true,
					}
				} else {
					collected++
					ch <- ImagineEvent{
						Type: ImagineEventImage, ImageID: slot.imageID,
						Order: slot.order, URL: slot.lastURL, Blob: slot.lastBlob,
						Width: slot.width, Height: slot.height, IsFinal: true,
					}
				}
				if collected >= needed || allSlotsDone(slots) {
					return false, collected
				}
			}

		case "image":
			urlStr, _ := msg["url"].(string)
			blob, _ := msg["blob"].(string)
			imageID, _ := ParseImageURL(urlStr)
			slot, ok := slots[imageID]
			if !ok || slot.done {
				continue
			}
			slot.lastBlob = blob
			slot.lastURL = urlStr
			progress := 50
			if pct, ok := msg["percentage_complete"]; ok {
				if pf, ok := pct.(float64); ok {
					progress = int(pf)
				}
			}
			if progress < 10 {
				progress = 10
			}
			if progress > 99 {
				progress = 99
			}
			if progress > slot.progress {
				slot.progress = progress
				ch <- ImagineEvent{
					Type: ImagineEventProgress, ImageID: imageID,
					Order: slot.order, Progress: progress,
				}
			}

		case "error":
			errMsg, _ := msg["err_msg"].(string)
			if errMsg == "" {
				errMsg = fmt.Sprintf("upstream imagine error: %v", msg)
			}
			ch <- ImagineEvent{Type: ImagineEventError, Error: errMsg}
			return true, collected
		}
	}
}

func allSlotsDone(slots map[string]*imagineSlot) bool {
	for _, s := range slots {
		if !s.done {
			return false
		}
	}
	return len(slots) > 0
}

// buildWSHeaders returns the fhttp headers for the WS imagine handshake.
func (s *ImagineStream) buildWSHeaders() fhttp.Header {
	profile := resolveProxyProfile()
	ua := profile.UserAgent
	if ua == "" {
		ua = DefaultUserAgent
	}
	tok := platform.SanitizeToken(s.token)

	h := fhttp.Header{}
	h.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Origin", "https://grok.com")
	h.Set("Pragma", "no-cache")
	h.Set("User-Agent", ua)
	h.Set("Cookie", BuildSSOCookie(tok, profile))
	for k, v := range clientHints("", ua) {
		h.Set(k, v)
	}
	return h
}

// genUUID generates a new UUID v4 string.
func genUUID() string {
	b := make([]byte, 16)
	_, _ = io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
