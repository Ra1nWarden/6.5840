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
	mu   sync.Mutex
	data map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	res, exist := kv.data[args.Key]
	kv.mu.Unlock()
	if !exist {
		res = ""
	}
	reply.Value = res
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.data[args.Key] = args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	res, exist := kv.data[args.Key]
	if !exist {
		res = ""
	}
	kv.data[args.Key] = res + args.Value
	kv.mu.Unlock()
	reply.Value = res
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)

	return kv
}
