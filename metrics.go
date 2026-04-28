package main

import (
	"fmt"
	"log"
	"time"
)

func (fs *FileStore) GatherMetrics() {
	leaderCh := fs.raft.LeaderCh()
lead:
	for {
		select {
		case isLeader := <-leaderCh:
			if isLeader {
				break lead
			}
		}
	}
	ticker := time.NewTicker(MetricsInterval * time.Second)
	for {
		select {
		case <-ticker.C:
			f := fs.raft.GetConfiguration()
			if err := f.Error(); err != nil {
				log.Printf("failed to get raft configuration: %v", err)
				return
			}
			for _, srv := range f.Configuration().Servers {
				log.Printf("Node ID: %s, Address: %s, Suffrage: %s",
					srv.ID, srv.Address, srv.Suffrage)
				mdata, err := getMetrics(string(srv.ID))
				if err == nil {
					fmt.Println(mdata)
					mdata.Addr = string(srv.ID)
					fs.metrics[string(srv.ID)] = mdata
				} else {
					fs.metrics[string(srv.ID)] = Metrics{
						Addr:        string(srv.ID),
						CpuPercent:  0,
						MemTotal:    0,
						MemUsed:     0,
						MemPercent:  0,
						DiskTotal:   0,
						DiskUsed:    0,
						DiskPercent: 0,
					}
				}
				// fmt.Println("Metrics:", fs.metrics, len(fs.metrics))
			}
		case isLeader := <-leaderCh:
			if !isLeader {
				goto lead
			}
		}
	}
}
