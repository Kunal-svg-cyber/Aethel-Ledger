package main

import "testing"

func fakeEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveAddrs(t *testing.T) {
	cases := []struct {
		name         string
		env          map[string]string
		wantL, wantS string
	}{
		{"local defaults", nil, ":50051", ":8080"},
		{"local stats override", map[string]string{"STATS_ADDR": ":9090"}, ":50051", ":9090"},
		{"cloud run port, stats off", map[string]string{"PORT": "8080"}, ":8080", ""},
		{"cloud run port, stats explicit", map[string]string{"PORT": "8080", "STATS_ADDR": "127.0.0.1:9000"}, ":8080", "127.0.0.1:9000"},
	}
	for _, c := range cases {
		l, s := resolveAddrs(fakeEnv(c.env))
		if l != c.wantL || s != c.wantS {
			t.Errorf("%s: got (%q,%q) want (%q,%q)", c.name, l, s, c.wantL, c.wantS)
		}
	}
}
