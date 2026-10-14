package main

const (
	defaultListenAddr = ":50051"
	defaultStatsAddr  = ":8080"
)

// resolveAddrs returns the gRPC listen address and the stats address.
//
// With PORT set (as on Cloud Run) gRPC listens on that port and the stats
// endpoint is disabled unless STATS_ADDR is set explicitly, because the
// platform exposes a single port and /stats is unauthenticated. Without
// PORT, local defaults apply. An empty stats address means disabled.
func resolveAddrs(getenv func(string) string) (listen, stats string) {
	if port := getenv("PORT"); port != "" {
		return ":" + port, getenv("STATS_ADDR")
	}
	stats = defaultStatsAddr
	if v := getenv("STATS_ADDR"); v != "" {
		stats = v
	}
	return defaultListenAddr, stats
}
