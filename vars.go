package main

var (
	clickHouseAddress           = "clickhouse:9000"
	minActiveConnection         = 0
	maxActiveConnection         = 1
	maxConnectionLifeTime int64 = 300000
	debug                       = false
	nodeId                int64
	snowFlake             *Node
)
