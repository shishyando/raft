package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	pb "shishraft/lib/proto/pb"
	raft "shishraft/lib/raft"

	"google.golang.org/protobuf/encoding/protojson"
)

type HttpServer struct {
	raft *raft.RaftServer
}

type HttpResponse struct {
	Err      string `json:"err,omitempty"`
	Node     string `json:"node"`
	LogIndex int    `json:"logIndex"`
	LogTerm  int64  `json:"logTerm"`
}

type RedirectedReadRequest struct {
	LogIndex int `json:"logIndex"`
	LogTerm  int `json:"LogTerm"`
}

func NewRaftHttpServer(raft *raft.RaftServer) *HttpServer {
	return &HttpServer{raft: raft}
}

func (s *HttpServer) OpHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unable to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Decode the protobuf message
	op := &pb.LogEntry{}
	if err := protojson.Unmarshal(body, op); err != nil {
		fmt.Printf("Failed to convert JSON to protobuf: %v\n", err)
		return
	}

	switch op.OpType {
	case pb.OpType_CREATE:
		if r.Method != http.MethodPost || op.Value == nil {
			http.Error(w, "post with value", http.StatusBadRequest)
			return
		}
	case pb.OpType_READ:
		if r.Method != http.MethodGet {
			http.Error(w, "use get", http.StatusBadRequest)
			return
		}
	case pb.OpType_UPDATE:
		if r.Method != http.MethodPut || op.Value == nil {
			http.Error(w, "use put with value", http.StatusBadRequest)
			return
		}
	case pb.OpType_DELETE:
		if r.Method != http.MethodDelete {
			http.Error(w, "use delete", http.StatusBadRequest)
			return
		}
	case pb.OpType_CAS:
		if r.Method != http.MethodPatch || op.Value == nil || op.ExpectedValue == nil {
			http.Error(w, "use patch with both value and old value", http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "unsupported operation", http.StatusBadRequest)
		return
	}

	res := s.raft.AppendOneEntryOnLeader(op)
	if res == nil {
		http.Error(w, "failed to replicate", http.StatusRequestTimeout)
		return
	}

	httpResponse := &HttpResponse{}
	httpResponse.LogIndex = res.LogId.Index
	httpResponse.LogTerm = res.LogId.Term
	httpResponse.Node = res.Node
	switch res.Code {
	case raft.RES_CODE_OK:
		// reads are not processed here
		if err := json.NewEncoder(w).Encode(httpResponse); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	case raft.RES_CODE_NOT_A_LEADER:
		http.Error(w, fmt.Sprintf("leader: %s", res.Node), http.StatusBadRequest)
		return
	case raft.RES_CODE_REDIRECT_READ:
		http.Error(w, fmt.Sprintf("read that: %d %d %s", httpResponse.LogIndex, httpResponse.LogTerm, res.Node), http.StatusFound)
		return
	default:
		http.Error(w, "unexpected res code", http.StatusInternalServerError)
		return
	}

}

func (s *HttpServer) RedirectReadHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unable to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	req := &RedirectedReadRequest{}
	if err := json.Unmarshal(body, req); err != nil {
		http.Error(w, fmt.Sprintf("failed to convert request to json struct: %v\n", err), http.StatusBadRequest)
		return
	}

	val, err := s.raft.ReadFromReplica(raft.LogId{Index: req.LogIndex, Term: int64(req.LogTerm)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	_, err = w.Write([]byte(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
