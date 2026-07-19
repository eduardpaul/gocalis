// Package telegram implements the "telegram" audio node type: a gocalis node
// whose speaker/microphone is a Telegram call placed from a logged-in USER
// account (not a bot). A telegram node behaves like any other audionode.AudioNode
// — the brain can say/play/ask on it and the intercom engine can bridge it to
// physical or WebRTC nodes — with two differences captured by the CallEndpoint
// contract: its media only flows once a remote peer is present, and (for
// on-demand nodes) the call is placed lazily and torn down when idle.
//
// Responsibilities are split across a stable, transport-neutral seam so the bulk
// of the logic is pure Go and unit-testable:
//
//   - TelegramNode (node.go): lifecycle state machine (autowake vs on-demand),
//     readiness gating, idle teardown, 16kHz<->48kHz resampling and real-time
//     pacing. Depends only on the Call/Manager interfaces below.
//   - Manager / Call (this file): the Telegram-specific backend. The concrete
//     implementation (gogram for MTProto signaling + the ntgcalls Go binding for
//     Opus media) is selected by build tag: the default build uses a stub that
//     reports telegram support is not compiled in; `-tags telegram_native` pulls
//     in the real backend.
//
// Media is exchanged as mono PCM16 at 48kHz (Telegram's native call rate) in
// both directions; the node handles conversion to/from gocalis's internal 16kHz.
package telegram

import "context"

// TelegramRate is the sample rate of the PCM16 audio exchanged with a Call. It
// is Telegram's native call rate; the node resamples between this and gocalis's
// internal 16kHz model/bridge rate.
const TelegramRate = 48000

// Target identifies who a telegram node talks to.
type Target struct {
	// Type is "group" (join a group/channel voice chat — many listeners, several
	// such calls may run concurrently on one account) or "contact" (a 1:1 private
	// call — one peer, and Telegram permits only one active at a time per account).
	Type string
	// Peer is the @username, phone number, or numeric chat/user id to resolve.
	Peer string
}

// Call is a single Telegram call — one group voice chat or one 1:1 private call.
// Audio is carried as mono PCM16 at TelegramRate in both directions. All methods
// on a given Call are invoked from the owning TelegramNode; OnFrame may deliver
// inbound audio from a backend goroutine.
type Call interface {
	// Join places/joins the call. For a group it joins the group voice chat; for a
	// contact it dials the 1:1 call. It does NOT wait for a remote peer — that is
	// WaitPeer. It returns once signaling/media negotiation is under way or ctx is
	// cancelled.
	Join(ctx context.Context) error

	// WaitPeer blocks until at least one remote participant is present (group) or
	// the callee has accepted (contact), or ctx is cancelled / its deadline
	// elapses. It is the "ready when a user is on the other side" gate.
	WaitPeer(ctx context.Context) error

	// Write sends one outbound frame of mono PCM16 at TelegramRate. The node paces
	// calls at real-time frame cadence, so Write should not itself block for
	// pacing; it hands the frame to the media layer (Opus-encoded and sent).
	Write(pcm48 []int16) error

	// OnFrame registers the sink for inbound mixed audio (mono PCM16 at
	// TelegramRate) from remote participants. Passing nil detaches it.
	OnFrame(fn func(pcm48 []int16))

	// Leave tears down this call (hangs up the 1:1 or leaves the group VC) and
	// releases its media resources. It is idempotent.
	Leave() error
}

// Manager owns the process-wide logged-in Telegram user session and mints a Call
// per telegram node. One Manager backs every telegram node so they share a
// single login.
type Manager interface {
	// NewCall creates a Call bound to target without placing it yet.
	NewCall(target Target) (Call, error)
	// Close logs out / tears down the shared session and any open calls.
	Close() error
}

// Limiter serializes the resources that Telegram allows only one of per account.
// Group voice chats can run concurrently, so they do not take the gate; 1:1
// private calls do, so at most one is ever active at a time.
type Limiter struct {
	p2p chan struct{}
}

// NewLimiter creates a limiter permitting a single concurrent 1:1 private call.
func NewLimiter() *Limiter {
	return &Limiter{p2p: make(chan struct{}, 1)}
}

// AcquireP2P reserves the single 1:1-private-call slot, blocking until it is free
// or ctx is cancelled. It must be paired with ReleaseP2P.
func (l *Limiter) AcquireP2P(ctx context.Context) error {
	select {
	case l.p2p <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseP2P frees the 1:1-private-call slot. It is safe to call when not held.
func (l *Limiter) ReleaseP2P() {
	select {
	case <-l.p2p:
	default:
	}
}
