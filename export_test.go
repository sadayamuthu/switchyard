package switchyard

// PendingLen exposes the pending-request count to external tests.
func PendingLen(p *Proxy) int { return p.pendingLen() }
