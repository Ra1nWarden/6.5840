package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu            sync.Mutex
	data          map[string]string
	cacheRequest  map[int64]int64
	cacheResponse map[int64]string
}

func (kv *KVServer) checkCache(clientId int64, requestId int64, reply *string) bool {
	prevRequest, newClient := kv.cacheRequest[clientId]
	if !newClient || prevRequest != requestId {
		return false
	}
	*reply = kv.cacheResponse[clientId]
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	res, exist := kv.data[args.Key]
	if !exist {
		res = ""
	}
	reply.Value = res
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.checkCache(args.ClientId, args.RequestId, &reply.Value) {
		kv.mu.Unlock()
		return
	}
	kv.cacheRequest[args.ClientId] = args.RequestId
	kv.data[args.Key] = args.Value
	reply.Value = ""
	kv.cacheResponse[args.ClientId] = ""
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.checkCache(args.ClientId, args.RequestId, &reply.Value) {
		kv.mu.Unlock()
		return
	}
	kv.cacheRequest[args.ClientId] = args.RequestId
	res, exist := kv.data[args.Key]
	if !exist {
		res = ""
	}
	kv.cacheResponse[args.ClientId] = res
	reply.Value = res
	kv.data[args.Key] = res + args.Value
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.cacheResponse = make(map[int64]string)
	kv.cacheRequest = make(map[int64]int64)

	return kv
}
