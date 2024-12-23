package storage

import (
	"fmt"
	"sync"

	"shishraft/lib/helpers"
	"shishraft/lib/proto/pb"

	"github.com/rs/zerolog/log"
)

type Storage struct {
	kv sync.Map
}

func NewStorage() *Storage {
	return &Storage{kv: sync.Map{}}
}

func (s *Storage) ApplyOp(op *pb.LogEntry) (string, error) {
	switch op.OpType {
	case pb.OpType_READ:
		key := op.Key
		log.Info().Str("Get", key).Msg("storage: READ")
		val, was := s.kv.Load(key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		return val.(string), nil
	case pb.OpType_CREATE:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: CREATE")

		_, was := s.kv.LoadOrStore(op.Key, *op.Value)
		if was {
			err := fmt.Errorf("resource at key `%s` already created", op.Key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		return "", nil

	case pb.OpType_UPDATE:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: UPDATE")
		if op.Value == nil {
			err := fmt.Errorf("no value provided\n%s", helpers.DumpProtoMessageAsText(op))
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		_, was := s.kv.Load(op.Key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", op.Key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		s.kv.Store(op.Key, *op.Value)
		return "", nil
	case pb.OpType_DELETE:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: DELETE")
		_, was := s.kv.LoadAndDelete(op.Key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", op.Key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		return "", nil
	case pb.OpType_CAS:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: CAS")
		if op.ExpectedValue == nil || op.Value == nil {
			err := fmt.Errorf("no value provided\n%s", helpers.DumpProtoMessageAsText(op))
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		swapped := s.kv.CompareAndSwap(op.Key, *op.ExpectedValue, *op.Value)
		if !swapped {
			err := fmt.Errorf("unexpected value for CAS for key `%s`", op.Key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		return "", nil
	default:
		err := fmt.Errorf("unsupported operation")
		log.Error().Str("op", helpers.DumpProtoMessageAsText(op)).Err(err).Msg("storage: unsupported operation")
		return "", err
	}
}
