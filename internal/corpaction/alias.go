package corpaction

import (
	"strings"
	"sync"
)

// Alias links a renamed instrument to its predecessor. OKX renames a contract
// on some rebases (e.g. SPACEX-USDT-SWAP -> SPCX-USDT-SWAP), starting a fresh
// instId whose history must be stitched onto the predecessor's.
type Alias struct {
	Successor    string
	Predecessor  string
	SourceMarket string
	BoundaryMS   int64
}

// AliasRegistry records instrument-rename links so the serving layer can stitch
// a successor's history onto its predecessor. Safe for concurrent use.
type AliasRegistry struct {
	mu       sync.RWMutex
	byNewKey map[string]Alias
}

// NewAliasRegistry builds an empty AliasRegistry.
func NewAliasRegistry() *AliasRegistry {
	return &AliasRegistry{byNewKey: make(map[string]Alias)}
}

// Link records that successor is the renamed continuation of predecessor,
// effective at boundaryMS. The most recent link for a successor wins.
func (r *AliasRegistry) Link(exchange string, sourceMarket string, successor string, predecessor string, boundaryMS int64) {
	successor = strings.ToUpper(strings.TrimSpace(successor))
	predecessor = strings.ToUpper(strings.TrimSpace(predecessor))
	if successor == "" || predecessor == "" || successor == predecessor {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byNewKey[key(exchange, successor)] = Alias{
		Successor:    successor,
		Predecessor:  predecessor,
		SourceMarket: sourceMarket,
		BoundaryMS:   boundaryMS,
	}
}

// Predecessor returns the alias linking successor to its prior instId, if any.
func (r *AliasRegistry) Predecessor(exchange string, successor string) (Alias, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	alias, ok := r.byNewKey[key(exchange, strings.ToUpper(strings.TrimSpace(successor)))]
	return alias, ok
}

// Lookup exposes the predecessor as primitive values so consumers need not
// import this package's types.
func (r *AliasRegistry) Lookup(exchange string, successor string) (predecessor string, sourceMarket string, boundaryMS int64, ok bool) {
	alias, found := r.Predecessor(exchange, successor)
	if !found {
		return "", "", 0, false
	}
	return alias.Predecessor, alias.SourceMarket, alias.BoundaryMS, true
}
