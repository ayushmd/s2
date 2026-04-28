package main

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Block struct {
	Id   string `json:"id"`
	Addr string `json:"addr"`
	Size int    `json:"size"`
}

type Blueprint struct {
	NumBlocks   int                        `json:"num_blocks"`
	TotalBlocks int                        `json:"total_blocks"`
	Store       [][ReplicationFactor]Block `json:"store"`
}

type WeightedBlock struct {
	id     string
	addr   string
	weight int
}

func ceil(a, b int) int {
	return (a + b - 1) / b
}

func (fs *FileStore) FilePlan(n int) Blueprint {
	var plan Blueprint
	var blockstore [][ReplicationFactor]Block
	var nodes []WeightedBlock
	snapshot := fs.metrics
	for k, v := range snapshot {
		if v.DiskPercent != 0 && v.MemTotal != 0 {
			nodes = append(nodes, WeightedBlock{
				id:     k,
				addr:   k,
				weight: 0,
			})
		}
	}
	fmt.Println("Metric snap: ", snapshot)
	numNodes := len(nodes)
	if numNodes == 0 {
		numNodes = 1
	}
	factor := ReplicationFactor
	if numNodes < ReplicationFactor {
		factor = numNodes
	}
	plan.NumBlocks = ceil(n, BlockSize)
	plan.TotalBlocks = plan.NumBlocks * factor
	point := 0
	for i := 0; i < plan.NumBlocks; i++ {
		var singleblock [ReplicationFactor]Block
		for k := 0; k < factor; k++ {
			currNode := nodes[(point+k)%numNodes]
			singleblock[k] = Block{
				Id:   currNode.id,
				Addr: currNode.addr,
			}
		}
		blockstore = append(blockstore, singleblock)
		point = (point + 1) % numNodes
	}
	plan.Store = blockstore
	return plan
}

func (fs *FileStore) CreateFile(bucket, key, compression string, size int, r io.Reader) error {
	var plan Blueprint
	var err error
	if fs.IsLeader() {
		plan = fs.FilePlan(size)
	} else {
		// api call and get plan from leader
		_, haddr := fs.GetLeader()
		plan, err = getPlan(haddr, size)
		if err != nil {
			fmt.Println("Error getting plan from leader:", err)
			return err
		}
	}

	fmt.Println("Plan:", plan)

	workers := make(chan struct{}, MaxWorker)
	buf := make([]byte, WindowSize)

	id := uuid.New().String()
	totalRead := 0
	path := filepath.Join(bucket, key, id)

	for {
		n, err := io.ReadFull(r, buf)
		// n, err := r.Read(buf)
		if n > 0 {
			var currBlock int = totalRead / BlockSize
			var currWindow int = (totalRead % BlockSize) / WindowSize
			block := plan.Store[currBlock]
			dataCopy := make([]byte, n)
			copy(dataCopy, buf[:n])
			for k := 0; k < ReplicationFactor; k++ {
				peerAddr := block[k].Addr
				workers <- struct{}{}
				go func(blockNum, windowNum int, data []byte, addr string) {
					defer func() {
						<-workers
					}()
					fmt.Println("Sending block", blockNum, "window", windowNum, "size", len(data))
					sendFilePeer(path, addr, blockNum, windowNum, data)
				}(currBlock, currWindow, dataCopy, peerAddr)
			}
			totalRead += n
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}
	}

	var op FileOperation = FileOperation{
		Type: "create",
		FileMetadata: FileMetadata{
			Bucket:      bucket,
			Key:         key,
			Compression: compression,
			Size:        size,
			Timestamp:   time.Now().Unix(),
			Blueprint:   plan,
			Identifier:  id,
		},
	}
	fmt.Println("Sent blocks: ", op)
	if fs.IsLeader() {
		fs.SubmitOperation(op)
	} else {
		_, lid := fs.GetLeader()
		err := setOperation(lid, op)
		fmt.Println("Operation sending to leader: ", lid, err)
	}
	fmt.Println("Operation submitted")
	return nil
}

func (fs *FileStore) GetEntireFile(meta FileMetadata) (readerWithClosers, error) {
	// metadata := fs.meta[path] // earlier in mem memory metadata mapper
	path := filepath.Join(meta.Bucket, meta.Key, meta.Identifier)

	NumBlocks := meta.NumBlocks
	readers := make([]io.Reader, NumBlocks)
	closers := make([]func() error, NumBlocks)
	errCh := make(chan error, NumBlocks)
	var wg sync.WaitGroup

	for i := 0; i < NumBlocks; i++ {
		wg.Add(1)
		go func(block int) {
			defer wg.Done()
			client := &http.Client{}

			var r io.Reader
			var closeFunc func() error
			var err error

			// Try all replicas
			for j := 0; j < ReplicationFactor; j++ {
				addr := meta.Store[block][j].Addr
				r, closeFunc, err = getFilePeer(path, addr, block, client)
				if err == nil {
					readers[block] = r
					closers[block] = closeFunc
					return
				} else {
					fmt.Println("Error getFilePeer", block, "from", addr, ":", err)
				}
			}

			// All replicas failed
			errCh <- fmt.Errorf("failed to fetch block %d: %w", block, err)
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	if len(errCh) > 0 {
		// Close any open readers before returning
		for _, c := range closers {
			if c != nil {
				_ = c()
			}
		}
		return readerWithClosers{}, <-errCh
	}

	// Combine all block readers
	multiReader := io.MultiReader(readers...)

	// Wrap to ensure closers are called after reading
	return readerWithClosers{
		Reader:  multiReader,
		closers: closers,
	}, nil
}

type readerWithClosers struct {
	io.Reader
	closers []func() error
}

func (rwc *readerWithClosers) Close() error {
	var firstErr error
	for _, c := range rwc.closers {
		if c != nil {
			if err := c(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (fs *FileStore) DeleteEntireFile(bucket, key string) error {
	path := filepath.Join(bucket, key)
	// plan := fs.meta[path]
	plan, err := fs.store.GetKey(bucket, key)
	if err != nil {
		return err
	}
	fmt.Println("Plan for deleting: ", plan)
	uniqueEndpoints := make(map[string]struct{})
	for _, v := range plan.Store {
		for _, block := range v {
			uniqueEndpoints[block.Addr] = struct{}{}
		}
	}
	for url := range uniqueEndpoints {
		u := url
		go fs.deleteFileLocal(u, path)
	}
	var op FileOperation = FileOperation{
		Type: "delete",
		FileMetadata: FileMetadata{
			Bucket:    bucket,
			Key:       key,
			Timestamp: time.Now().Unix(),
		},
	}
	fmt.Println("Delete Operation: ", op)
	if fs.IsLeader() {
		fs.SubmitOperation(op)
	} else {
		_, lid := fs.GetLeader()
		err := setOperation(lid, op)
		fmt.Println("Delete Operation sending to leader: ", lid, err)
	}
	fmt.Println("Delete Operation submitted")
	return nil
}
