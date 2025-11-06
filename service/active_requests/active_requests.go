package active_requests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"
)

var hardStoreSizeEnforce = os.Getenv("HARD_STORE_SIZE_ENFORCE") == "true"

func NewActiveRequestsManager(logger *zap.Logger) *ActiveRequestsManager {
	return &ActiveRequestsManager{
		reqs:                   make(map[string]*activeRequestRecord),
		logger:                 logger,
		maxStoreSizePerRequest: 5000000000,
		maxStoreSize:           12000000000,
	}
}

type ActiveRequestsManager struct {
	reqs map[string]*activeRequestRecord
	sync.RWMutex
	maxStoreSizePerRequest uint64 // limit per request
	maxStoreSize           uint64 // limit for the whole instance
	logger                 *zap.Logger
}

type ActiveRequestsHandler struct {
	uniqueID string
	manager  *ActiveRequestsManager
}

func NewActiveRequestsHandler(manager *ActiveRequestsManager) *ActiveRequestsHandler {
	return &ActiveRequestsHandler{
		manager: manager,
	}
}

type activeRequestRecord struct {
	StartTime              time.Time
	cancelFunc             context.CancelCauseFunc
	TraceID                string
	OutputModuleHash       string
	SegmentNumber          uint64
	SegmentSize            uint64
	Stage                  uint32
	FullKVStoreMemoryBytes uint64
}

var ErrInstanceOutOfMemory = errors.New("instance out of memory")

func (arh *ActiveRequestsHandler) currentTotalLoadedSize() (totalSize uint64) {
	for _, req := range arh.manager.reqs {
		totalSize += req.FullKVStoreMemoryBytes
	}
	return totalSize
}

func (arh *ActiveRequestsHandler) CheckAvailable(size uint64) bool {
	totalSize := arh.currentTotalLoadedSize()
	return totalSize+size <= arh.manager.maxStoreSize
}

func (arh *ActiveRequestsHandler) LoadedFullKV(size uint64) {
	if size == 0 {
		return
	}
	arh.manager.Lock()
	defer arh.manager.Unlock()
	if req := arh.manager.reqs[arh.uniqueID]; req != nil {
		req.FullKVStoreMemoryBytes += size
		if size > arh.manager.maxStoreSizePerRequest {
			arh.manager.logger.Warn("sum of used stores is too big", zap.String("uniqueID", arh.uniqueID), zap.Uint64("size", size), zap.Uint64("totalBytes", req.FullKVStoreMemoryBytes))
			if hardStoreSizeEnforce {
				req.cancelFunc(connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("sum of all stores became too big, above maximum size: %d, (deterministic error)", arh.manager.maxStoreSizePerRequest)))
			}
			return
		}

		var totalSize uint64
		for _, req := range arh.manager.reqs {
			totalSize += req.FullKVStoreMemoryBytes
		}
		if totalSize > arh.manager.maxStoreSize {
			arh.manager.logger.Warn("sum of used stores on all requests is too big", zap.String("uniqueID", arh.uniqueID), zap.Uint64("size", size), zap.Uint64("totalBytes", totalSize))
			if hardStoreSizeEnforce {
				req.cancelFunc(connect.NewError(connect.CodeResourceExhausted, ErrInstanceOutOfMemory))
			}
		}
	} else {
		arh.manager.logger.Warn("LoadedFullKV called for unknown request", zap.String("uniqueID", arh.uniqueID))
	}
}

func (arr *ActiveRequestsManager) Add(cancel context.CancelCauseFunc, traceID string, outputModuleHash string, segmentNumber, segmentSize uint64, stage uint32) *ActiveRequestsHandler {
	uniqueID := reqID(traceID, segmentNumber, segmentSize, stage)
	arr.Lock()
	arr.reqs[uniqueID] = &activeRequestRecord{
		StartTime:              time.Now(),
		cancelFunc:             cancel,
		TraceID:                traceID,
		OutputModuleHash:       outputModuleHash,
		SegmentNumber:          segmentNumber,
		SegmentSize:            segmentSize,
		Stage:                  stage,
		FullKVStoreMemoryBytes: 0,
	}
	arr.Unlock()

	return &ActiveRequestsHandler{
		manager:  arr,
		uniqueID: uniqueID,
	}
}

func (arr *ActiveRequestsManager) Remove(reqHandler *ActiveRequestsHandler) {
	arr.Lock()
	delete(arr.reqs, reqHandler.uniqueID)
	arr.Unlock()
}

func (arr *ActiveRequestsManager) List() []*activeRequestRecord {
	var out []*activeRequestRecord
	arr.RLock()
	for _, req := range arr.reqs {
		out = append(out, req)
	}
	arr.RUnlock()
	return out
}

func (arr *ActiveRequestsManager) CancelRequest(traceID string, outputModuleHash string, segmentNumber, segmentSize *uint64, stage *uint32) (out []string) {
	arr.RLock()
	for k, req := range arr.reqs {
		if traceID != "" && traceID != req.TraceID {
			continue
		}
		if outputModuleHash != "" && outputModuleHash != req.OutputModuleHash {
			continue
		}
		if segmentNumber != nil && *segmentNumber != req.SegmentNumber {
			continue
		}
		if segmentSize != nil && *segmentSize != req.SegmentSize {
			continue
		}
		if stage != nil && *stage != req.Stage {
			continue
		}
		req.cancelFunc(fmt.Errorf("request forcefully cancelled, please try again"))
		out = append(out, k)
	}
	arr.RUnlock()
	return out
}

func reqID(traceID string, segmentNumber, segmentSize uint64, stage uint32) string {
	return fmt.Sprintf("%s-%d-%d-%d", traceID, segmentNumber, segmentSize, stage)
}
