package grok

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/aurora-develop/grok2api/internal/config"
	"github.com/aurora-develop/grok2api/internal/platform"
)

// r is the math/rand source for random birth-date generation. Not security-
// sensitive; seeded once at startup. math/rand is auto-seeded since Go 1.20.
var r = rand.New(rand.NewSource(time.Now().UnixNano()))

// BuildAcceptTOSPayload builds the gRPC-Web payload for
// SetTosAcceptedVersion (proto field 2 = true).
//
// Wire format: 0x10 0x01 (proto field 2, varint, value 1).
func BuildAcceptTOSPayload() []byte {
	return EncodeGRPCWebPayload([]byte{0x10, 0x01})
}

// BuildNSFWMgmtPayload builds the gRPC-Web payload that toggles
// always_show_nsfw_content.
//
// Wire layout (mirrors xai_auth.py build_nsfw_mgmt_payload):
//
//	inner = 0x0a + len(name) + name                       # field 1: string
//	protobuf = 0x0a 0x02 0x10 <bool> 0x12 + len(inner) + inner
//	└ field 1 (length-delimited): nested { field 2 (varint bool),
//	  field 1 (string: "always_show_nsfw_content") }
//	frame = 0x00 + uint32 BE len(protobuf) + protobuf    # gRPC-Web data frame
func BuildNSFWMgmtPayload(enabled bool) []byte {
	name := []byte("always_show_nsfw_content")
	inner := append([]byte{0x0a, byte(len(name))}, name...)
	var boolByte byte
	if enabled {
		boolByte = 0x01
	}
	protobuf := []byte{0x0a, 0x02, 0x10, boolByte, 0x12, byte(len(inner))}
	protobuf = append(protobuf, inner...)
	return EncodeGRPCWebPayload(protobuf)
}

// BuildSetBirthPayload returns the JSON payload for /rest/auth/set-birth-date
// with a random adult birth date.
func BuildSetBirthPayload() map[string]any {
	now := time.Now().UTC()
	year := now.Year() - (20 + r.Intn(29)) // 20–48 years old
	month := 1 + r.Intn(12)
	day := 1 + r.Intn(28)
	hour := r.Intn(24)
	minute := r.Intn(60)
	second := r.Intn(60)
	ms := r.Intn(1000)
	stamp := fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
		year, month, day, hour, minute, second, ms)
	return map[string]any{"birthDate": stamp}
}

// AcceptTOS calls SetTosAcceptedVersion on accounts.x.ai.
func AcceptTOS(ctx context.Context, t *Transport, token string) (GRPCStatus, error) {
	cfg := config.Global()
	timeoutS := cfg.GetInt("nsfw.timeout", 30)
	_, trailers, err := t.PostGRPCWeb(ctx, AcceptTOSURL, token,
		BuildAcceptTOSPayload(),
		WithOrigin(AccountsBase),
		WithReferer(AccountsBase+"/accept-tos"),
		WithTimeout(time.Duration(timeoutS)*time.Second),
	)
	if err != nil {
		return GRPCStatus{}, err
	}
	status := GetGRPCStatus(trailers)
	if status.OK() || status.Code == -1 {
		return status, nil
	}
	return status, platform.UpstreamError(
		fmt.Sprintf("accept_tos: gRPC error code=%d message=%s", status.Code, status.Message),
		status.HTTPEquiv(), "",
	)
}

// SetNSFW toggles always_show_nsfw_content via gRPC-Web.
func SetNSFW(ctx context.Context, t *Transport, token string, enabled bool) (GRPCStatus, error) {
	cfg := config.Global()
	timeoutS := cfg.GetInt("nsfw.timeout", 30)
	label := "enable_nsfw"
	if !enabled {
		label = "disable_nsfw"
	}
	_, trailers, err := t.PostGRPCWeb(ctx, NSFWMgmtURL, token,
		BuildNSFWMgmtPayload(enabled),
		WithOrigin(Base),
		WithReferer(Base+"/?_s=data"),
		WithTimeout(time.Duration(timeoutS)*time.Second),
	)
	if err != nil {
		return GRPCStatus{}, err
	}
	status := GetGRPCStatus(trailers)
	if status.OK() || status.Code == -1 {
		return status, nil
	}
	return status, platform.UpstreamError(
		fmt.Sprintf("%s: gRPC error code=%d message=%s", label, status.Code, status.Message),
		status.HTTPEquiv(), "",
	)
}

// EnableNSFW is a convenience wrapper for SetNSFW(true).
func EnableNSFW(ctx context.Context, t *Transport, token string) (GRPCStatus, error) {
	return SetNSFW(ctx, t, token, true)
}

// DisableNSFW is a convenience wrapper for SetNSFW(false).
func DisableNSFW(ctx context.Context, t *Transport, token string) (GRPCStatus, error) {
	return SetNSFW(ctx, t, token, false)
}

// SetBirthDate posts a random adult birth date for *token* via REST.
func SetBirthDate(ctx context.Context, t *Transport, token string) (map[string]any, error) {
	cfg := config.Global()
	timeoutS := cfg.GetInt("nsfw.timeout", 30)
	payload := BuildSetBirthPayload()
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, platform.UpstreamError("set_birth_date: encode failed: "+err.Error(), 500, "")
	}
	return t.PostJSON(ctx, SetBirthURL, token, body,
		WithOrigin(Base),
		WithReferer(Base+"/?_s=data"),
		WithTimeout(time.Duration(timeoutS)*time.Second),
	)
}

// NSFWSequence runs accept_tos → set_birth_date → enable_nsfw.
//
// The set_birth_date 429-with-birth-date-change-limit-reached response means
// the birth date is already locked; this is treated as a no-op success.
func NSFWSequence(ctx context.Context, t *Transport, token string) error {
	if _, err := AcceptTOS(ctx, t, token); err != nil {
		return err
	}
	if _, err := SetBirthDate(ctx, t, token); err != nil {
		if appErr, ok := err.(*platform.AppError); ok {
			if appErr.Status == 429 && containsSubstr(appErr.Body, "birth-date-change-limit-reached") {
				// Birth date already set and locked — safe to skip.
			} else {
				return err
			}
		} else {
			return err
		}
	}
	if _, err := EnableNSFW(ctx, t, token); err != nil {
		return err
	}
	return nil
}

func containsSubstr(s, sub string) bool {
	if sub == "" {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
