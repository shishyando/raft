package storage

import (
	"fmt"
	"sync"

	"shishraft/lib/helpers"
	"shishraft/lib/proto/pb"

	"github.com/rs/zerolog/log"
)

var storage sync.Map

func ApplyOp(op *pb.LogEntry) (string, error) {
	switch op.OpType {
	case pb.OpType_READ:
		key := op.Key
		log.Info().Str("Get", key).Msg("storage: Read")
		val, was := storage.Load(key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		return val.(string), nil
	case pb.OpType_CREATE:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: Create")

		_, was := storage.LoadOrStore(op.Key, *op.Value)
		if was {
			err := fmt.Errorf("resource at key `%s` already created", op.Key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		return "", nil

	case pb.OpType_UPDATE:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: Update")
		if op.Value == nil {
			err := fmt.Errorf("no value provided\n%s", helpers.DumpProtoMessageAsText(op))
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		_, was := storage.Load(op.Key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", op.Key)
			log.Warn().Err(err).Msg("storage: error")
			return "", err
		}
		storage.Store(op.Key, *op.Value)
		return "", nil
	case pb.OpType_DELETE:
		log.Info().Str("Op", helpers.DumpProtoMessageAsText(op)).Msg("storage: Delete")
		_, was := storage.LoadAndDelete(op.Key)
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
		swapped := storage.CompareAndSwap(op.Key, *op.ExpectedValue, *op.Value)
		if !swapped {
			oldValue, exists := storage.Load(op.Key)
			if exists {
				log.Info().Str("oldValue", oldValue.(string)).Msg("CAS failed")
			}
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
