package mcpproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// sessionPool keeps an upstream MCP session open between the calls of one
// caller trace. Opening a session costs two round trips (initialize, then the
// initialized notification) and closing it a third (DELETE); against an
// upstream behind Cloudflare that was about 170 ms of every proxied skills
// call, whose own work takes a few milliseconds.
//
// A session never spans two traces, so an upstream that groups work by
// session sees one caller per session, as it did when every call opened its
// own. Calls without a trace are not pooled. A session is also keyed by what
// authorizes it: a rotated token, a new URL, or a reconnected generation
// opens a new one.
type sessionPool struct {
	mu       sync.Mutex
	sessions map[poolSlot]*pooledSession
}

type poolSlot struct{ connection, trace string }

type pooledSession struct {
	auth     string
	session  *mcp.ClientSession
	lastUsed time.Time
}

// Agent sessions call in bursts: in production 63% of proxied calls came
// within 15 minutes of the previous call in the same trace.
const (
	poolIdleTimeout = 15 * time.Minute
	poolMaxSessions = 512
)

var poolNow = time.Now

func authKey(c record) string {
	sum := sha256.Sum256([]byte(c.URL + "\x00" + c.Generation + "\x00" + c.Token.AccessToken))
	return hex.EncodeToString(sum[:])
}

// get returns the session for c and the call's trace, opening one when none is
// pooled. reused reports that an earlier call opened it. release must be
// called once the call is done; failed reports the call failed, which
// discards the session.
func (p *sessionPool) get(ctx context.Context, c record, client *http.Client) (session *mcp.ClientSession, reused bool, release func(failed bool), err error) {
	trace := TraceIDFromContext(ctx)
	if trace == "" {
		session, err := openSession(ctx, c, client)
		if err != nil {
			return nil, false, nil, err
		}
		return session, false, func(bool) { session.Close() }, nil
	}
	slot, auth := poolSlot{c.Name, trace}, authKey(c)
	p.mu.Lock()
	p.evictLocked()
	if pooled, ok := p.sessions[slot]; ok && pooled.auth == auth {
		pooled.lastUsed = poolNow()
		p.mu.Unlock()
		return pooled.session, true, p.releaser(slot, pooled.session), nil
	}
	p.mu.Unlock()

	session, err = openSession(ctx, c, client)
	if err != nil {
		return nil, false, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.sessions[slot]; ok {
		if current.auth == auth {
			// A concurrent call in the same trace opened one first.
			go session.Close()
			current.lastUsed = poolNow()
			return current.session, true, p.releaser(slot, current.session), nil
		}
		go current.session.Close()
	}
	p.sessions[slot] = &pooledSession{auth: auth, session: session, lastUsed: poolNow()}
	return session, false, p.releaser(slot, session), nil
}

func (p *sessionPool) releaser(slot poolSlot, session *mcp.ClientSession) func(bool) {
	return func(failed bool) {
		if !failed {
			return
		}
		p.mu.Lock()
		if current, ok := p.sessions[slot]; ok && current.session == session {
			delete(p.sessions, slot)
		}
		p.mu.Unlock()
		go session.Close()
	}
}

// evictLocked closes idle sessions, and the least recently used one when the
// pool is full.
func (p *sessionPool) evictLocked() {
	now := poolNow()
	var oldest poolSlot
	var oldestAt time.Time
	for slot, pooled := range p.sessions {
		if now.Sub(pooled.lastUsed) > poolIdleTimeout {
			delete(p.sessions, slot)
			go pooled.session.Close()
			continue
		}
		if oldestAt.IsZero() || pooled.lastUsed.Before(oldestAt) {
			oldest, oldestAt = slot, pooled.lastUsed
		}
	}
	if len(p.sessions) >= poolMaxSessions {
		go p.sessions[oldest].session.Close()
		delete(p.sessions, oldest)
	}
}
