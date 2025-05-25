package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cond         *sync.Cond
	data         map[string]string
	prevRequest  map[int64]int64
	prevResponse map[int64]string
	latestIndex  int
	term         int
	isLeader     bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Operation: "Get",
		Key:       args.Key,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	if kv.prevRequest[args.ClientId] == args.RequestId {
		reply.Err = OK
		reply.Value = kv.prevResponse[args.ClientId]
		return
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		DPrintf("handle get in %d (%v): %v vs %v, %v vs %v, %v vs %v\n", kv.me, op, term, kv.term, isLeader, kv.isLeader, index, kv.latestIndex)
		if kv.term != term || !kv.isLeader {
			reply.Err = ErrWrongLeader
			break
		}
		if kv.latestIndex > index {
			reply.Err = ErrWrongLeader
			break
		} else if kv.latestIndex == index {
			reply.Err = OK
			reply.Value = kv.data[args.Key]
			break
		}
		kv.cond.Wait()
	}
}

func (kv *KVServer) PutAppend(command Op, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.prevRequest[command.ClientId] == command.RequestId {
		reply.Err = OK
		return
	}
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for {
		DPrintf("handle putappend in %d (%v): %v vs %v, %v vs %v, %v vs %v\n", kv.me, command, term, kv.term, isLeader, kv.isLeader, index, kv.latestIndex)
		if kv.term != term || !kv.isLeader {
			reply.Err = ErrWrongLeader
			break
		}
		if kv.latestIndex > index {
			reply.Err = ErrWrongLeader
			break
		} else if kv.latestIndex == index {
			reply.Err = OK
			break
		}
		kv.cond.Wait()
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation: "Put",
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	kv.PutAppend(op, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Operation: "Append",
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	kv.PutAppend(op, reply)
}

func (kv *KVServer) apply() {
	for msg := range kv.applyCh {
		DPrintf("apply in %d: %v\n", kv.me, msg)
		command := msg.Command.(Op)
		kv.mu.Lock()
		kv.latestIndex = msg.CommandIndex
		kv.cond.Broadcast()
		if kv.prevRequest[command.ClientId] == command.RequestId {
			kv.mu.Unlock()
			continue
		}
		kv.prevRequest[command.ClientId] = command.RequestId
		switch command.Operation {
		case "Put":
			kv.data[command.Key] = command.Value
		case "Append":
			kv.data[command.Key] += command.Value
		case "Get":
			kv.prevResponse[command.ClientId] = kv.data[command.Key]
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) ticker() {
	for !kv.killed() {
		kv.mu.Lock()
		term, isLeader := kv.rf.GetState()
		if kv.term != term || kv.isLeader != isLeader {
			kv.term = term
			kv.isLeader = isLeader
			kv.cond.Broadcast()
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.prevRequest = make(map[int64]int64)
	kv.prevResponse = make(map[int64]string)

	kv.latestIndex = 0
	kv.term = 0
	kv.isLeader = false

	go kv.apply()
	go kv.ticker()

	return kv
}
