package raft

import (
	"fmt"
	"sync"

	pb "shishraft/lib/proto/pb"

	"github.com/rs/zerolog/log"
)

var storage sync.Map

func Get(key *string) (string, error) {
	log.Info().Str("Get", *key).Msg("Read")

	val, was := storage.Load(key)
	if !was {
		err := fmt.Errorf("no resource at key `%d`", key)
		log.Warn().Err(err).Send()
		return "", err
	}
	return val.(string), nil
}

func ApplyOp(op *pb.LogEntry) error {
	switch op.OpType {
	case pb.OpType_CREATE:
		log.Info().Str("Op", op.String()).Msg("Create")

		_, was := storage.LoadOrStore(op.Key, op.Value)
		if was {
			err := fmt.Errorf("resource at key `%s` already created", op.Key)
			log.Warn().Err(err).Send()
			return err
		}
		return nil

	case pb.OpType_UPDATE:
		log.Info().Str("Op", op.String()).Msg("Update")
		_, was := storage.Load(op.Key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", op.Key)
			log.Warn().Err(err).Send()
			return err
		}
	case pb.OpType_DELETE:
		log.Info().Str("Op", op.String()).Msg("Delete")
		_, was := storage.LoadAndDelete(op.Key)
		if !was {
			err := fmt.Errorf("no resource at key `%s`", op.Key)
			log.Warn().Err(err).Send()
			return err
		}
		return nil
	case pb.OpType_CAS:
		log.Info().Str("Op", op.String()).Msg("CAS")
		if op.ExpectedValue == nil || op.Value == nil {
			err := fmt.Errorf("no value provided\n%s", op.String())
			log.Warn().Err(err).Send()
			return err
		}
		swapped := storage.CompareAndSwap(op.Key, *op.ExpectedValue, *op.Value)
		if !swapped {
			err := fmt.Errorf("unexpected value for CAS for key `%s`", op.Key)
			log.Warn().Err(err).Send()
			return err
		}
		return nil
	default:
		err := fmt.Errorf("unsupported operation")
		log.Error().Err(err).Msg(op.String())
		return err
	}

	return nil
}
