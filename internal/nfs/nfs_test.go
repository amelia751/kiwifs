package nfs

import (
	"net"
	"testing"
)

func TestParseAllowAcceptsCIDRsAndBareIPs(t *testing.T) {
	got, err := ParseAllow("127.0.0.1, 10.0.0.0/8,  ::1")
	if err != nil {
		t.Fatalf("ParseAllow: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 nets, got %d", len(got))
	}
}

func TestParseAllowEmpty(t *testing.T) {
	got, err := ParseAllow("")
	if err != nil || got != nil {
		t.Fatalf("empty spec should return nil, got %v/%v", got, err)
	}
}

func TestParseAllowInvalid(t *testing.T) {
	if _, err := ParseAllow("not-an-ip"); err == nil {
		t.Fatal("expected error for invalid entry")
	}
}

func TestIPAllowed(t *testing.T) {
	_, lan, _ := net.ParseCIDR("10.0.0.0/8")
	allow := []*net.IPNet{lan}
	cases := map[string]struct {
		addr net.Addr
		ok   bool
	}{
		"in-range":  {&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 2049}, true},
		"outside":   {&net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 2049}, false},
		"nil-addr":  {&net.TCPAddr{IP: nil, Port: 2049}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ipAllowed(tc.addr, allow); got != tc.ok {
				t.Fatalf("ipAllowed(%v) = %v, want %v", tc.addr, got, tc.ok)
			}
		})
	}
}

func TestDefaultAllowCoversLoopback(t *testing.T) {
	allow := DefaultAllow()
	if !ipAllowed(&net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, allow) {
		t.Fatal("127.0.0.1 should be in DefaultAllow")
	}
	if !ipAllowed(&net.TCPAddr{IP: net.ParseIP("::1")}, allow) {
		t.Fatal("::1 should be in DefaultAllow")
	}
	if ipAllowed(&net.TCPAddr{IP: net.ParseIP("8.8.8.8")}, allow) {
		t.Fatal("8.8.8.8 should not be in DefaultAllow")
	}
}
