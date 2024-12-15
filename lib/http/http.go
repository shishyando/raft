package rafthttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"shishraft/lib/helpers"
	"shishraft/lib/proto/pb"
	raft "shishraft/lib/raft"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

type HttpServer struct {
	raft *raft.RaftServer
}

func NewRaftHttpServer(raft *raft.RaftServer) *HttpServer {
	return &HttpServer{raft: raft}
}

type LogEntry struct {
	Term          int64     `json:"term"`
	OpType        pb.OpType `json:"opType"`
	Key           string    `json:"key"`
	Value         string    `json:"value,omitempty"`
	ExpectedValue string    `json:"expectedValue,omitempty"`
}

func (s *HttpServer) logResponse(entry LogEntry) raft.ApplyOpResult {
	op := &pb.LogEntry{OpType: entry.OpType, Key: entry.Key, Value: &entry.Value, ExpectedValue: &entry.ExpectedValue}
	log.Info().Str("op", helpers.DumpProtoMessageAsText(op)).Msg("http: got 1 new op")
	return s.raft.AppendOneEntryOnLeader(op)
}

func processResult(res raft.ApplyOpResult, w http.ResponseWriter, r *http.Request) {
	if res.Err != nil {
		http.Error(w, res.Err.Error(), http.StatusBadRequest)
		return
	}
	if res.Redirect {
		newUrl := fmt.Sprintf("http://%s%s", res.Node, r.URL.Path)
		if res.LogIndex >= 0 { // direct read by LogIndex
			newUrl += fmt.Sprintf("/%d", res.LogIndex)
		}
		http.Redirect(w, r, newUrl, http.StatusTemporaryRedirect)
		return
	}
	w.Write([]byte(res.Val))
}

func (s *HttpServer) CreateHandler(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	entry.OpType = pb.OpType_CREATE
	processResult(s.logResponse(entry), w, r)
}

func (s *HttpServer) ReadHandler(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}
	entry := LogEntry{OpType: pb.OpType_READ, Key: key}
	processResult(s.logResponse(entry), w, r)
}

func (s *HttpServer) ReadByIdHandler(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	id, err := strconv.Atoi(mux.Vars(r)["logId"])
	if key == "" || err != nil {
		http.Error(w, "Key and valid logId required", http.StatusBadRequest)
		return
	}

	processResult(s.raft.DirectReadById(id), w, r)
}

func (s *HttpServer) UpdateHandler(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	entry.OpType = pb.OpType_UPDATE
	processResult(s.logResponse(entry), w, r)
}

func (s *HttpServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	entry := LogEntry{OpType: pb.OpType_DELETE, Key: key}
	processResult(s.logResponse(entry), w, r)
}

func (s *HttpServer) CasHandler(w http.ResponseWriter, r *http.Request) {
	var entry LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	entry.OpType = pb.OpType_CAS
	processResult(s.logResponse(entry), w, r)
}
