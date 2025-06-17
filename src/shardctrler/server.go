package shardctrler

import (
	"log"
	"sort"
	"sync"

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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	cond         *sync.Cond
	prevRequest  map[int64]int64
	prevResponse map[int64]Config
	latestIndex  int
	term         int
	isLeader     bool

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation string
	Servers   map[int][]string
	Num       int
	GID       int
	Shard     int
	RequestId int64
	ClientId  int64
}

// This helper function takes a groups mapping and current shard allocation,
// and returns a new shard allocation that is balanced.
func rebalance(shards [NShards]int, groups map[int][]string) [NShards]int {
	allocation := make(map[int]int)
	gids := make([]int, 0, len(groups))
	for gid := range groups {
		allocation[gid] = 0
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	result := [NShards]int{}

	baseAllocation := len(shards) / len(groups)
	remainder := len(shards) % len(groups)

	queue := []int{}

	for shard, gid := range shards {
		_, ok := allocation[gid]
		if !ok {
			result[shard] = 0
			queue = append(queue, shard)
			continue
		}

		if allocation[gid] < baseAllocation {
			allocation[gid]++
			result[shard] = gid
		} else if allocation[gid] == baseAllocation && remainder > 0 {
			allocation[gid]++
			result[shard] = gid
			remainder--
		} else {
			result[shard] = 0
			queue = append(queue, shard)
		}
	}

	for _, gid := range gids {
		target := baseAllocation
		if remainder > 0 {
			target++
			remainder--
		}
		count := allocation[gid]
		for count < target {
			shard := queue[0]
			queue = queue[1:]
			result[shard] = gid
			count++
		}
	}

	return result

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	lastConfig := sc.configs[len(sc.configs)-1]

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	newConfig.Shards = rebalance(lastConfig.Shards, newConfig.Groups)

	sc.configs = append(sc.configs, newConfig)

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
