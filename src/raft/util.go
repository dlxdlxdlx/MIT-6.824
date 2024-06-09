package raft

import (
	"fmt"
	"log"
	"time"
)

var debugStart = time.Now()

// Debugging
const Debug = true

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
