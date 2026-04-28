package main

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

// FileRequest represents the request body for file operations
type FileRequest struct {
	Path        string `json:"path"`
	Content     string `json:"content,omitempty"`
	Compression bool   `json:"compression,omitempty"`
}

// FileResponse represents the response for file operations
type FileResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	Data        string `json:"data,omitempty"`
	Compression bool   `json:"compression,omitempty"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

// SuccessResponse represents an error response
type SuccessResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type AddNodeRequest struct {
	Id   string `json:"id"`
	Addr string `json:"addr"`
}

type FilePlanRequest struct {
	Size int `json:"size"`
}

type Metrics struct {
	Addr        string  `json:"addr"`
	CpuPercent  float64 `json:"cpu_percent"`
	MemTotal    uint64  `json:"mem_total"`
	MemUsed     uint64  `json:"mem_used"`
	MemPercent  float64 `json:"mem_pct"`
	DiskTotal   uint64  `json:"disk_total"`
	DiskUsed    uint64  `json:"disk_used"`
	DiskPercent float64 `json:"disk_pct"`
}

type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix,omitempty"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	KeyCount       int            `xml:"KeyCount"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []Content      `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type Content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"` // ISO8601, e.g. 2025-02-07T12:00:00.000Z
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass,omitempty"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// S3 ListBuckets response
type ListAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	Xmlns   string        `xml:"xmlns,attr"`
	Buckets []BucketEntry `xml:"Buckets>Bucket"`
	Owner   Owner         `xml:"Owner"`
}

type BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

// S3 Error response (for 404, etc.)
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(FileResponse{
		Success: true,
		Message: "Server is healthy",
	})
}

func MetricsHandler(w http.ResponseWriter, r *http.Request) {
	v, _ := mem.VirtualMemory()
	c, _ := cpu.Percent(0, false)
	d, _ := disk.Usage("/")

	stats := map[string]interface{}{
		"cpu_percent": c[0],
		"mem_total":   v.Total / 1024 / 1024,
		"mem_used":    v.Used / 1024 / 1024,
		"mem_pct":     v.UsedPercent,
		"disk_total":  d.Total / 1024 / 1024 / 1024,
		"disk_used":   d.Used / 1024 / 1024 / 1024,
		"disk_pct":    d.UsedPercent,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (fs *FileStore) Plan(w http.ResponseWriter, r *http.Request) {
	var req FilePlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}
	if req.Size <= 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Size must be greater than 0",
		})
		return
	}
	plan := fs.FilePlan(req.Size)
	fmt.Println("Generated plan:", plan)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(plan)
}

func (fs *FileStore) Submit(w http.ResponseWriter, r *http.Request) {
	var op FileOperation
	if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}
	if op.Type != "create" && op.Type != "delete" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Invalid operation type",
		})
		return
	}
	fs.SubmitOperation(op)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SuccessResponse{
		Success: true,
		Message: "Operation submitted",
	})
}

func (fs *FileStore) AddNewVoter(w http.ResponseWriter, r *http.Request) {
	var req AddNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Invalid request body",
		})
		return
	}
	fmt.Println("Add Voter Request:", req)
	if req.Id == "" || req.Addr == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "ID and Address are required",
		})
		return
	}
	if err := fs.AddVoter(req.Id, req.Addr); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SuccessResponse{
		Success: true,
		Message: "Node added as voter",
	})
}

func ensureDir(path string) error {
	err := os.MkdirAll(path, 0755) // Creates all missing parent directories
	if err != nil {
		return fmt.Errorf("creating directory %s: %w", path, err)
	}
	return nil
}

func ensureLeadingSlash(s string) string {
	return s
	// if len(s) == 0 || s[0] != '/' {
	// 	return "/" + s
	// }
	// return s
}

func (fs *FileStore) CreateFileWindow(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Query().Get("filename")
	block := r.URL.Query().Get("block")
	window := r.URL.Query().Get("window")
	if filename == "" || window == "" {
		http.Error(w, "filename and window are required", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, filename, block)
	if err := ensureDir(filePath); err != nil {
		http.Error(w, "failed to create directory", http.StatusInternalServerError)
		return
	}

	dstPath := filepath.Join(filePath, window)
	dst, err := os.Create(dstPath)
	if err != nil {
		http.Error(w, "failed to create file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	written, err := io.Copy(dst, r.Body)
	if err != nil {
		http.Error(w, "failed to write body to file", http.StatusInternalServerError)
		return
	}

	// if written != int64(WindowSize) {
	// 	fmt.Println("warning: expected %d bytes, wrote %d bytes", WindowSize, written)
	// }

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SuccessResponse{
		Success: true,
		Message: fmt.Sprintf("wrote %d bytes", written),
	})
}

func (fs *FileStore) GetFileWindow(w http.ResponseWriter, r *http.Request) {
	// get filename and window from query params
	filename := r.URL.Query().Get("filename")
	block := r.URL.Query().Get("block")
	window := r.URL.Query().Get("window")
	if filename == "" || window == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Filename and window are required",
		})
		return
	}

	filePath := filepath.Join(storageDir, filename, block, window)
	data, err := os.ReadFile(filePath)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Failed to read file",
		})
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

func sortedFiles(path string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var files []os.FileInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			files = append(files, info)
		}
	}

	// Sort files by name (which should correspond to window number)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	return files, nil
}

func (fs *FileStore) GetFileBlock(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Query().Get("filename")
	block := r.URL.Query().Get("block")

	if filename == "" || block == "" {
		http.Error(w, "Filename and block are required", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(storageDir, filename, block)

	files, err := sortedFiles(filePath)
	if err != nil {
		http.Error(w, "Failed to read directory", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	for _, file := range files {
		select {
		case <-r.Context().Done():
			return
		default:
		}

		partPath := filepath.Join(filePath, file.Name())

		f, err := os.Open(partPath)
		if err != nil {
			http.Error(w, "Failed to read file part", http.StatusInternalServerError)
			return
		}

		if _, err := io.Copy(w, f); err != nil {
			f.Close()
			return
		}

		f.Close()
	}
}

func (fs *FileStore) DeleteFile(w http.ResponseWriter, r *http.Request) {
	// get filename from query params
	filename := r.URL.Query().Get("filename")
	if filename == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Filename is required",
		})
		return
	}

	filePath := filepath.Join(storageDir, filename)
	fmt.Println("Delete path: ", filePath)
	err := os.RemoveAll(filePath)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{
			Success: false,
			Error:   "Failed to delete file",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(SuccessResponse{
		Success: true,
		Message: "File deleted successfully",
	})
}

func writeS3Error(w http.ResponseWriter, code, message, resource string, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(S3Error{
		Code:     code,
		Message:  message,
		Resource: resource,
	})
}

func (fs *FileStore) ListBucketsS3(w http.ResponseWriter, r *http.Request) {
	buckets, err := fs.store.ListBuckets()
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	entries := make([]BucketEntry, 0, len(buckets))
	for _, name := range buckets {
		entries = append(entries, BucketEntry{Name: name, CreationDate: now})
	}
	result := ListAllMyBucketsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: entries,
		Owner:   Owner{ID: "minio", DisplayName: "minio"},
	}
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(result); err != nil {
		return
	}
}

func (fs *FileStore) CreateBucketS3(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		writeS3Error(w, "InvalidBucketName", "The specified bucket is invalid.", "", http.StatusBadRequest)
		return
	}
	if err := fs.store.CreateBucket(bucket); err != nil {
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (fs *FileStore) HeadBucketS3(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	buckets, err := fs.store.ListBuckets()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	for _, b := range buckets {
		if b == bucket {
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (fs *FileStore) ListObjectsV2S3(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	maxKeys := 1000
	if s := r.URL.Query().Get("max-keys"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxKeys = n
		}
	}

	limit := maxKeys * 10
	if limit > 5000 {
		limit = 5000
	}
	allMeta, err := fs.store.ListKeys(bucket, prefix, limit)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}

	var list []Content
	commonPrefixSet := make(map[string]struct{})

	if delimiter != "" {
		dlen := len(delimiter)
		for _, m := range allMeta {
			key := m.Key
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}
			rest := key
			if prefix != "" {
				rest = key[len(prefix):]
			}
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				commonPrefix := prefix + rest[:idx+dlen]
				commonPrefixSet[commonPrefix] = struct{}{}
			} else {
				list = append(list, Content{
					Key:          key,
					LastModified: time.Unix(m.Timestamp, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
					ETag:         `"` + m.Identifier + `"`,
					Size:         int64(m.Size),
					StorageClass: "STANDARD",
				})
			}
		}
		commonPrefixes := make([]CommonPrefix, 0, len(commonPrefixSet))
		for p := range commonPrefixSet {
			commonPrefixes = append(commonPrefixes, CommonPrefix{Prefix: p})
		}
		sort.Slice(commonPrefixes, func(i, j int) bool { return commonPrefixes[i].Prefix < commonPrefixes[j].Prefix })
		total := len(list) + len(commonPrefixes)
		isTruncated := len(allMeta) >= limit || total > maxKeys
		if total > maxKeys {
			if len(list) > maxKeys {
				list = list[:maxKeys]
				commonPrefixes = nil
			} else {
				commonPrefixes = commonPrefixes[:maxKeys-len(list)]
			}
		}
		result := ListBucketResult{
			Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:           bucket,
			Prefix:         prefix,
			Delimiter:      delimiter,
			KeyCount:       len(list) + len(commonPrefixes),
			MaxKeys:        maxKeys,
			IsTruncated:    isTruncated,
			Contents:       list,
			CommonPrefixes: commonPrefixes,
		}
		w.Header().Set("Content-Type", "application/xml")
		if err := xml.NewEncoder(w).Encode(result); err != nil {
			return
		}
		return
	}

	isTruncated := len(allMeta) > maxKeys
	if isTruncated {
		allMeta = allMeta[:maxKeys]
	}
	for _, m := range allMeta {
		list = append(list, Content{
			Key:          m.Key,
			LastModified: time.Unix(m.Timestamp, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + m.Identifier + `"`,
			Size:         int64(m.Size),
			StorageClass: "STANDARD",
		})
	}
	result := ListBucketResult{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucket,
		Prefix:         prefix,
		KeyCount:       len(list),
		MaxKeys:        maxKeys,
		IsTruncated:    isTruncated,
		Contents:       list,
		CommonPrefixes: nil,
	}
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(result); err != nil {
		return
	}
}

func (fs *FileStore) PutS3File(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	path := ensureLeadingSlash(vars["filepath"])
	bucket := vars["bucket"]
	size := r.ContentLength
	if size < 0 {
		if length := r.Header.Get("Content-Length"); length != "" {
			if n, err := strconv.ParseInt(length, 10, 64); err == nil {
				size = n
			}
		}
	}
	compression := r.Header.Get("Content-Encoding")
	if size == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := fs.CreateFile(bucket, path, compression, int(size), r.Body); err != nil {
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (fs *FileStore) DeleteS3File(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	path := vars["filepath"]
	path = ensureLeadingSlash(path)
	bucket := vars["bucket"]
	if err := fs.DeleteEntireFile(bucket, path); err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			writeS3Error(w, "NoSuchKey", "The specified key does not exist.", r.URL.Path, http.StatusNotFound)
			return
		}
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (fs *FileStore) GetS3File(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	path := vars["filepath"]
	path = ensureLeadingSlash(path)
	bucket := vars["bucket"]
	metadata, err := fs.store.GetKey(bucket, path)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			writeS3Error(w, "NoSuchKey", "The specified key does not exist.", r.URL.Path, http.StatusNotFound)
			return
		}
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}
	reader, err := fs.GetEntireFile(metadata)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+metadata.Identifier+`"`)
	w.Header().Set("Last-Modified", time.Unix(metadata.Timestamp, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(metadata.Size), 10))
	if _, err := io.Copy(w, reader); err != nil {
		fmt.Println("Error streaming file:", err)
		return
	}
}

func (fs *FileStore) HeadS3File(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	path := vars["filepath"]
	path = ensureLeadingSlash(path)
	bucket := vars["bucket"]
	metadata, err := fs.store.GetKey(bucket, path)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+metadata.Identifier+`"`)
	w.Header().Set("Last-Modified", time.Unix(metadata.Timestamp, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(metadata.Size), 10))
	w.WriteHeader(http.StatusOK)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		rec := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK, // default
		}

		next.ServeHTTP(rec, r)
		if r.URL.Path != "/metrics" {
			fmt.Printf("[%s] %s %d %s\n",
				r.Method,
				r.URL.Path,
				rec.status,
				r.RemoteAddr,
			)
		}
	})
}

func (fs *FileStore) listDb() {
	ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("\nPrinting DB:")
		it, _ := fs.store.db.NewIter(nil)
		for it.First(); it.Valid(); it.Next() {
			val := it.Value()
			if len(val) > 20 {
				fmt.Printf("Key: %s, Value: %s...\n", it.Key(), val[:20])
			} else {
				fmt.Printf("Key: %s, Value: %s\n", it.Key(), val)
			}
		}
	}
}

func (fs *FileStore) ListNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(fs.metrics)
}

func (fs *FileStore) StartApi(addr string) error {
	fmt.Println("Starting API server on", addr)
	r := mux.NewRouter()
	r.HandleFunc("/health", healthCheck).Methods("GET")
	r.HandleFunc("/metrics", MetricsHandler).Methods("GET")

	// My S2 API's Below
	r.HandleFunc("/s2/nodes", fs.ListNodes).Methods("GET")
	r.HandleFunc("/s2/add-voter", fs.AddNewVoter).Methods("POST")
	r.HandleFunc("/s2/plan", fs.Plan).Methods("POST")
	r.HandleFunc("/s2/submit", fs.Submit).Methods("POST")

	r.HandleFunc("/s2/file", fs.CreateFileWindow).Methods("POST")
	r.HandleFunc("/s2/file-window", fs.GetFileWindow).Methods("GET")
	r.HandleFunc("/s2/file-block", fs.GetFileBlock).Methods("GET")
	r.HandleFunc("/s2/file", fs.DeleteFile).Methods("DELETE")

	// S3-compatible API (path-style: /bucket/key). Order matters: more specific first.
	r.HandleFunc("/", fs.ListBucketsS3).Methods("GET")
	r.HandleFunc("/{bucket}", fs.CreateBucketS3).Methods("PUT")
	r.HandleFunc("/{bucket}", fs.HeadBucketS3).Methods("HEAD")
	r.HandleFunc("/{bucket}", fs.ListObjectsV2S3).Methods("GET")
	r.HandleFunc("/{bucket}/", fs.ListObjectsV2S3).Methods("GET")
	r.HandleFunc("/{bucket}/{filepath:.+}", fs.GetS3File).Methods("GET")
	r.HandleFunc("/{bucket}/{filepath:.+}", fs.HeadS3File).Methods("HEAD")
	r.HandleFunc("/{bucket}/{filepath:.+}", fs.PutS3File).Methods("PUT")
	r.HandleFunc("/{bucket}/", fs.PutS3File).Methods("PUT")
	r.HandleFunc("/{bucket}/{filepath:.+}", fs.DeleteS3File).Methods("DELETE")

	loggedRouter := loggingMiddleware(r)
	return http.ListenAndServe(addr, loggedRouter)
}
