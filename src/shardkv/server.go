package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister    *raft.Persister
	cond         *sync.Cond
	data         map[string]string
	prevRequest  map[int64]int64
	prevResponse map[int64]string
	latestIndex  int

	// Shard-specific fields
	mck    *shardctrler.Clerk
	config shardctrler.Config
}

type Snapshot struct {
	Data         map[string]string
	LatestIndex  int
	PrevRequest  map[int64]int64
	PrevResponse map[int64]string
	Config       shardctrler.Config
}

func (kv *ShardKV) waitForSuccessfulCommit(term int, index int) bool {
	timeout := time.After(10 * time.Second) // 10 second timeout
	startTime := time.Now()
	for {
		select {
		case <-timeout:
			DPrintf("ShardKV %d: waitForSuccessfulCommit timeout after %v for index %d", kv.me, time.Since(startTime), index)
			return false
		default:
			newTerm, newIsLeader := kv.rf.GetState()
			if newTerm != term || !newIsLeader {
				DPrintf("ShardKV %d: leader changed or term changed, giving up", kv.me)
				return false
			}
			if kv.latestIndex >= index {
				DPrintf("ShardKV %d: commit successful for index %d", kv.me, index)
				return true
			}
			kv.cond.Wait()
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.waitForSuccessfulCommit(term, index) {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = OK
	reply.Value = kv.data[args.Key]
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.waitForSuccessfulCommit(term, index) {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = OK
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) apply() {
	for msg := range kv.applyCh {
		//DPrintf("apply in %d: %v\n", kv.me, msg)
		if msg.CommandValid {
			command := msg.Command.(Op)
			kv.mu.Lock()
			kv.latestIndex = msg.CommandIndex
			kv.cond.Broadcast()
			if kv.prevRequest[command.ClientId] == command.RequestId {
				kv.mu.Unlock()
				continue
			}
			kv.prevRequest[command.ClientId] = command.RequestId
			kv.config = kv.mck.Query(-1)
			if kv.config.Shards[key2shard(command.Key)] != kv.gid {
				DPrintf("wrong group %d %d config is %v\n", kv.config.Shards[key2shard(command.Key)], kv.gid, kv.config)
				kv.mu.Unlock()
				continue
			}
			switch command.Operation {
			case "Put":
				kv.data[command.Key] = command.Value
			case "Append":
				kv.data[command.Key] += command.Value
			case "Get":
				kv.prevResponse[command.ClientId] = kv.data[command.Key]
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.applySnapshot(msg.Snapshot) {
				kv.cond.Broadcast()
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applySnapshot(snapshot []byte) bool {
	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	var data map[string]string
	var latestIndex int
	var prevRequest map[int64]int64
	var prevResponse map[int64]string
	var config shardctrler.Config
	if decoder.Decode(&data) != nil || decoder.Decode(&latestIndex) != nil ||
		decoder.Decode(&prevRequest) != nil || decoder.Decode(&prevResponse) != nil ||
		decoder.Decode(&config) != nil {
		return false
	} else {
		kv.data = data
		kv.latestIndex = latestIndex
		kv.prevRequest = prevRequest
		kv.prevResponse = prevResponse
		kv.config = config
		kv.rf.Snapshot(kv.latestIndex, snapshot)
		kv.persister.Save(kv.persister.ReadRaftState(), snapshot)
		return true
	}
}

func (kv *ShardKV) saveSnapshot() []byte {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(kv.data)
	encoder.Encode(kv.latestIndex)
	encoder.Encode(kv.prevRequest)
	encoder.Encode(kv.prevResponse)
	encoder.Encode(kv.config)
	return writer.Bytes()
}

func (kv *ShardKV) ticker() {
	for {
		kv.mu.Lock()
		kv.config = kv.mck.Query(-1)
		kv.cond.Broadcast()
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			snapshot := kv.saveSnapshot()
			kv.rf.Snapshot(kv.latestIndex, snapshot)
			kv.persister.Save(kv.persister.ReadRaftState(), snapshot)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	// Initialize data structures
	if kv.persister.ReadSnapshot() == nil || !kv.applySnapshot(kv.persister.ReadSnapshot()) {
		kv.data = make(map[string]string)
		kv.prevRequest = make(map[int64]int64)
		kv.prevResponse = make(map[int64]string)
		kv.latestIndex = 0
		kv.config = shardctrler.Config{}
	}

	go kv.apply()
	go kv.ticker()

	return kv
}
