package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client owns a bounded transport shared by one poller and one report sender.
// It performs exactly one send per call; retries belong to the daemon so every
// report attempt can reserve a new durable sequence number.
type Client struct {
	baseURL   string
	token     string
	http      *http.Client
	transport *http.Transport
}

type HTTPError struct {
	StatusCode int
	Body       ErrorBody
}

func (e *HTTPError) Error() string {
	// Do not retain or log arbitrary server bodies (or credentials).
	return fmt.Sprintf("agent HTTP status %d (%s)", e.StatusCode, e.Body.Error)
}

func NewClient(baseURL, machineToken string) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("controller URL must be an http(s) origin without credentials, query, or path")
	}
	if machineToken == "" || strings.ContainsAny(machineToken, "\r\n") {
		return nil, errors.New("machine token is required and must not contain newlines")
	}
	transport := &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		DialContext:     (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxConnsPerHost: 2, MaxIdleConns: 2, MaxIdleConnsPerHost: 2,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: 35 * time.Second, MaxResponseHeaderBytes: 16 << 10,
	}
	return &Client{
		baseURL: u.Scheme + "://" + u.Host, token: machineToken, transport: transport,
		http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
	}, nil
}

func (c *Client) Close() { c.transport.CloseIdleConnections() }

func (c *Client) Poll(ctx context.Context, incarnation int64, timeoutSeconds int) (PollResponse, error) {
	if incarnation < 0 || timeoutSeconds < 0 || timeoutSeconds > 30 {
		return PollResponse{}, errors.New("invalid poll incarnation or timeout (must be 0..30 seconds)")
	}
	body, err := json.Marshal(map[string]any{"agent_incarnation": incarnation, "timeout_s": timeoutSeconds})
	if err != nil {
		return PollResponse{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second+5*time.Second)
	defer cancel()
	data, err := c.post(ctx, "/poll", body)
	if err != nil {
		return PollResponse{}, err
	}
	return ParsePollResponse(data)
}

func (c *Client) Report(ctx context.Context, report ReportRequest) (ReportResponse, error) {
	body, err := EncodeReportRequest(report)
	if err != nil {
		return ReportResponse{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	data, err := c.post(ctx, "/report", body)
	if err != nil {
		return ReportResponse{}, err
	}
	return ParseReportResponse(data)
}

// The largest legal argv can occupy over 3 MiB when JSON-escaped. Bound the
// complete body before parsing so unknown fields cannot cause unlimited retention.
const maxResponseBytes = 4 << 20

func (c *Client) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/v1/agents"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResponseBytes {
		return nil, errors.New("agent response exceeds body limit")
	}
	if response.StatusCode != http.StatusOK {
		body, parseErr := ParseErrorBody(data)
		if parseErr != nil {
			body = ErrorBody{Error: "invalid_error_body"}
		}
		return nil, &HTTPError{StatusCode: response.StatusCode, Body: body}
	}
	return data, nil
}

func retryable(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == 408 || httpErr.StatusCode == 429 || httpErr.StatusCode >= 500
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
