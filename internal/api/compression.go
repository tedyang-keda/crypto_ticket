package api

import (
	"bufio"
	"compress/gzip"
	"net"
	"net/http"
	"strings"
)

func gzipResponses(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appendVary(w.Header(), "Accept-Encoding")
		if !acceptsGzip(r.Header.Get("Accept-Encoding")) || shouldSkipGzip(r) {
			next.ServeHTTP(w, r)
			return
		}

		gz := &gzipResponseWriter{ResponseWriter: w}
		defer gz.Close()
		next.ServeHTTP(gz, r)
	})
}

func acceptsGzip(value string) bool {
	for _, part := range strings.Split(value, ",") {
		token := strings.TrimSpace(strings.ToLower(part))
		if token == "gzip" || strings.HasPrefix(token, "gzip;") {
			return true
		}
	}
	return false
}

func shouldSkipGzip(r *http.Request) bool {
	if r.Method == http.MethodOptions {
		return true
	}
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return true
	}
	return false
}

func appendVary(header http.Header, value string) {
	existing := header.Values("Vary")
	for _, line := range existing {
		for _, part := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(part), value) {
				return
			}
		}
	}
	header.Add("Vary", value)
}

type gzipResponseWriter struct {
	http.ResponseWriter
	writer        *gzip.Writer
	headerWritten bool
	compress      bool
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	if w.headerWritten {
		return
	}
	w.headerWritten = true
	w.compress = gzipStatusAllowsBody(status) && w.Header().Get("Content-Encoding") == ""
	if w.compress {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *gzipResponseWriter) Write(data []byte) (int, error) {
	if !w.headerWritten {
		w.WriteHeader(http.StatusOK)
	}
	if !w.compress {
		return w.ResponseWriter.Write(data)
	}
	if w.writer == nil {
		writer, err := gzip.NewWriterLevel(w.ResponseWriter, gzip.BestSpeed)
		if err != nil {
			return 0, err
		}
		w.writer = writer
	}
	return w.writer.Write(data)
}

func (w *gzipResponseWriter) Flush() {
	if w.writer != nil {
		_ = w.writer.Flush()
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (w *gzipResponseWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	return w.writer.Close()
}

func gzipStatusAllowsBody(status int) bool {
	return status >= http.StatusOK &&
		status != http.StatusNoContent &&
		status != http.StatusNotModified
}
