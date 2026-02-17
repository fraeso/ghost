package server

import (
	"fmt"
	"net/http"
)

func NewHTTPServer(addr string) *http.Server {
	handler := newHTTPHandler()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", handler.handleProduce)
	mux.HandleFunc("GET /", handler.handleConsume)
	return &http.Server{
		Addr:    fmt.Sprintf(":%s", addr),
		Handler: mux,
	}
}

type httpHandler struct {
	Log *Log
}

func newHTTPHandler() *httpHandler {
	return &httpHandler{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (h *httpHandler) handleProduce(w http.ResponseWriter, r *http.Request) {
	req, err := ReadJSON[ProduceRequest](r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	off, err := h.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = WriteJSON(w, r, http.StatusOK, ProduceResponse{Offset: off})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *httpHandler) handleConsume(w http.ResponseWriter, r *http.Request) {
	req, err := ReadJSON[ConsumeRequest](r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rec, err := h.Log.Read(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = WriteJSON(w, r, http.StatusOK, ConsumeResponse{Record: rec})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
