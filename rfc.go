package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func sendFilePeer(filename, addr string, block, window int, data []byte) error {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}

	// Append path (customize as needed)
	baseURL := strings.TrimRight(addr, "/") + "/s2/file"

	// Build query parameters safely
	params := url.Values{}
	params.Set("filename", filename)
	params.Set("block", fmt.Sprintf("%d", block))
	params.Set("window", fmt.Sprintf("%d", window))

	url := baseURL + "?" + params.Encode()
	fmt.Println("Sending block to URL:", url)

	reader := bytes.NewReader(data)

	req, err := http.NewRequest("POST", url, reader)
	if err != nil {
		return err
	}

	// Optional: chunked transfer for large data (Go handles small []byte automatically)
	// req.TransferEncoding = []string{"chunked"}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Println("Server response:", string(body))
	return nil
}

func getPlan(addr string, n int) (Blueprint, error) {
	var plan Blueprint
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	url := strings.TrimRight(addr, "/") + "/s2/plan"

	var body FilePlanRequest = FilePlanRequest{
		Size: n,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return plan, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return plan, err
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return plan, err
	}
	defer resp.Body.Close()

	data, err = io.ReadAll(resp.Body)
	if err != nil {
		return plan, err
	}

	err = json.Unmarshal(data, &plan)
	if err != nil {
		return plan, err
	}

	fmt.Println("Received plan from leader:", plan)
	return plan, nil
}

func setOperation(addr string, op FileOperation) error {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	url := strings.TrimRight(addr, "/") + "/s2/submit"
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	fmt.Println("Status:", resp.Status)
	return nil
}

func getMetrics(addr string) (Metrics, error) {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}

	// Append the metrics path
	url := strings.TrimRight(addr, "/") + "/metrics"
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error:", err)
		return Metrics{}, err
	}
	defer resp.Body.Close()

	var metrics Metrics
	err = json.NewDecoder(resp.Body).Decode(&metrics)
	if err != nil {
		fmt.Println("Error reading body:", err)
		return Metrics{}, err
	}
	return metrics, nil
}

func getFilePeer(filename, addr string, block int, client *http.Client) (io.Reader, func() error, error) {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	baseURL := strings.TrimRight(addr, "/") + "/s2/file-block"
	params := url.Values{}
	params.Set("filename", filename)
	params.Set("block", fmt.Sprintf("%d", block))
	url := baseURL + "?" + params.Encode()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("failed to fetch block %d, status: %s", block, resp.Status)
	}

	return resp.Body, resp.Body.Close, nil
}

func addVoter(addr string, id, nodeaddr string) error {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	url := strings.TrimRight(addr, "/") + "/s2/add-voter"
	fmt.Println("Joining node: ", url)
	body := AddNodeRequest{
		Id:   id,
		Addr: nodeaddr,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println("Add Voter Response:", string(res))
	return err
}

func deleteFile(addr, path string) error {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	urlAddr := strings.TrimRight(addr, "/") + "/s2/file"
	params := url.Values{}
	params.Set("filename", path)

	finalUrl := urlAddr + "?" + params.Encode()
	fmt.Println("Sending block to URL:", finalUrl)
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("DELETE", finalUrl, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Println("Server response:", string(body))
	return nil
}
