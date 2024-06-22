package raft

import (
	"fmt"
	"log"
	"time"
)

var debugStart = time.Now()

// Debugging
const Debug = true

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)
const (
	Leader = iota
	Follower
	Candidate
)

const (
	heartbeatTimeout   = 100
	minElectionTimeout = 300
	maxElectionTimeout = 450
)

type Role int

func DPrintf(topic logTopic, format string, a ...interface{}) {
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	//if Debug {
	//	log.Printf(format, a...)
	//}
	t := time.Since(debugStart).Microseconds()
	t /= 100
	prefix := fmt.Sprintf("%06d %v ", t, string(topic))
	format = prefix + format
	log.Printf(format, a...)
}
