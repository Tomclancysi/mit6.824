package raft

import (
	"log"
)

// Debugging
const (
	Debug = true
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func MinInt(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func MaxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
