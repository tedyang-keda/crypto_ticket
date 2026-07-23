package corpaction

import "testing"

func TestAliasRegistryLinkAndLookup(t *testing.T) {
	r := NewAliasRegistry()
	if _, ok := r.Predecessor("okx", "SPCX-USDT-SWAP"); ok {
		t.Fatal("no alias should exist yet")
	}

	r.Link("okx", "okx:SWAP", "SPCX-USDT-SWAP", "SPACEX-USDT-SWAP", 1234)

	alias, ok := r.Predecessor("okx", "spcx-usdt-swap")
	if !ok {
		t.Fatal("alias should be found case-insensitively")
	}
	if alias.Predecessor != "SPACEX-USDT-SWAP" || alias.SourceMarket != "okx:SWAP" || alias.BoundaryMS != 1234 {
		t.Fatalf("unexpected alias: %+v", alias)
	}

	pred, src, boundary, ok := r.Lookup("okx", "SPCX-USDT-SWAP")
	if !ok || pred != "SPACEX-USDT-SWAP" || src != "okx:SWAP" || boundary != 1234 {
		t.Fatalf("lookup mismatch: %s/%s/%d/%v", pred, src, boundary, ok)
	}
}

func TestAliasRegistryIgnoresSelfLink(t *testing.T) {
	r := NewAliasRegistry()
	r.Link("okx", "okx:SWAP", "SAME", "SAME", 1)
	if _, ok := r.Predecessor("okx", "SAME"); ok {
		t.Fatal("self-link should be ignored")
	}
}
