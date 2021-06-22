package eventbus

import "errors"

// ErrAlreadyClosed is returned by Close when the bus has already been
// closed.
var ErrAlreadyClosed = errors.New("eventbus: bus already closed")

// Close marks the bus as closed. After Close returns, Subscribe,
// SubscribeWithPriority, SubscribeWithReplay, Publish, and PublishAsync
// become no-ops: subscribe calls return an id of 0 and publish calls
// deliver to nobody. Close does not wait for handlers already dispatched
// by a prior PublishAsync to finish; call Wait on their Handle first if
// that matters. Close is safe to call more than once; subsequent calls
// return ErrAlreadyClosed.
func (b *Bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrAlreadyClosed
	}
	b.closed = true
	return nil
}

// Closed reports whether Close has been called.
func (b *Bus) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}
