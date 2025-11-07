package active_requests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/dustin/go-humanize"
	"github.com/pbnjay/memory"
	"go.uber.org/zap"
)

var GB = uint64(1024 * 1024 * 1024)

var enforceStoreSizeLimitPerRequest = os.Getenv("SUBSTREAMS_ENFORCE_STORE_SIZE_LIMIT_PER_REQUEST") == "true"
var enforceStoreSizeLimit = os.Getenv("SUBSTREAMS_ENFORCE_STORE_SIZE_LIMIT_TOTAL") == "true"
var storeSizeLimitPerRequest = parseUint64EnvVar("SUBSTREAMS_STORE_SIZE_LIMIT_PER_REQUEST", 5*GB)
var storeSizeLimitTotal = parseUint64EnvVar("SUBSTREAMS_STORE_SIZE_LIMIT_TOTAL", 30*GB)

func parseUint64EnvVar(envVar string, defaultValue uint64) uint64 {
	if val := os.Getenv(envVar); val != "" {
		if parsed, err := strconv.ParseUint(val, 10, 64); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func NewActiveRequestsManager(logger *zap.Logger) *ActiveRequestsManager {
	fmt.Println("free memory", memory.FreeMemory())
	//l, err := memlimit.FromSystem()
	//if err != nil {
	//	logger.Error("failed to get memory limit", zap.Error(err))
	//} else {
	//	logger.Info("memory limit", zap.Uint64("limit", l))
	//}
	return &ActiveRequestsManager{
		reqs:                   make(map[string]*activeRequestRecord),
		logger:                 logger,
		maxStoreSizePerRequest: storeSizeLimitPerRequest,
		maxStoreSize:           storeSizeLimitTotal,
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

func (arh *ActiveRequestsHandler) totalLoadedSize() (totalSize uint64) {
	for _, req := range arh.manager.reqs {
		totalSize += req.FullKVStoreMemoryBytes
	}
	return totalSize
}

func (arh *ActiveRequestsHandler) AdjustFullKVSize(size uint64) {
	arh.manager.Lock()
	defer arh.manager.Unlock()
	if req := arh.manager.reqs[arh.uniqueID]; req != nil {
	}
}

func (arh *ActiveRequestsHandler) AllocateFullKVSize(size uint64) {
	if size == 0 {
		return
	}
	arh.manager.Lock()
	defer arh.manager.Unlock()
	if req := arh.manager.reqs[arh.uniqueID]; req != nil {

		if size > arh.manager.maxStoreSizePerRequest {
			arh.manager.logger.Warn("size of stores used in this request is above maximum", zap.String("uniqueID", arh.uniqueID), zap.Uint64("size", size), zap.Uint64("totalBytes", arh.manager.maxStoreSizePerRequest))
			if enforceStoreSizeLimitPerRequest {
				req.cancelFunc(connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("size of stores used in this request is %q, above maximum: %q, (deterministic error)", humanize.Bytes(size), humanize.Bytes(arh.manager.maxStoreSizePerRequest))))
			}
			return
		}

		totalSize := arh.totalLoadedSize()
		if totalSize+size > arh.manager.maxStoreSize {
			arh.manager.logger.Warn("size of all stores used in this instance is above maximum", zap.String("uniqueID", arh.uniqueID), zap.Uint64("total_size", totalSize), zap.Uint64("requested_size", size), zap.Uint64("totalBytes", arh.manager.maxStoreSize))
			if enforceStoreSizeLimit {
				req.cancelFunc(connect.NewError(connect.CodeResourceExhausted, ErrInstanceOutOfMemory))
			}
		}

		req.FullKVStoreMemoryBytes += size
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
