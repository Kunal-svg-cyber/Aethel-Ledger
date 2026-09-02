package metrics

import (
	"encoding/json"
	"net/http"
)

// Handler serves the current Snapshot as JSON. Deliberately plain JSON
// rather than Prometheus exposition format — this project doesn't run a
// Prometheus scraper, so a format a human (or curl) can read directly is
// more useful than one that only a metrics pipeline can parse.
func (r *Recorder) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(r.Snapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
