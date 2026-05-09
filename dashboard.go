package main

import (
	_ "embed"
	"net/http"
)

//go:embed web/cluster.html
var clusterDashboardHTML []byte

func clusterDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(clusterDashboardHTML)
}
