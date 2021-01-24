# Solution For MIT 6.824 2020

<!-- TOC -->autoauto- [Solution For MIT 6.824 2020](#solution-for-mit-6824-2020)auto    - [Introduction](#introduction)auto    - [Setup](#setup)auto    - [Labs](#labs)auto        - [Lab 1 MapReduce](#lab-1-mapreduce)auto        - [Lab 2 Raft](#lab-2-raft)auto        - [Lab 3 Fault-tolerant Key/Value Service](#lab-3-fault-tolerant-keyvalue-service)auto        - [Lab 4 Sharded Key/Value Service](#lab-4-sharded-keyvalue-service)autoauto<!-- /TOC -->

## Introduction

Solution for Labs of MIT 6.824: Distributed Systems.

The initial template is from [here](https://github.com/platoneko/6.824-golabs-2020/tree/be2e5603a1434ae23c44af4f34e1fdeee8ab3f42) (note: this is from the first commit)

The markdown file is from [here](https://github.com/kophy/6.824/blob/master/README.md)

## Setup

```bash
# Build the image
docker build -t mit-6-824 .
# start a new container

docker run -v //c/Users/jacka/OneDrive/Code_Temp/Go:/home/Go  --name mit-6-824 -dit mit-6-824 bash
```

## Labs

- [ ] Lab 1 MapReduce

### Lab 2 Raft

- [ ] 2A
- [ ] 2B
- [ ] 2C

### Lab 3 Fault-tolerant Key/Value Service

- [ ] Key/value service without log compaction
- [ ] Key/value service with log compaction

### Lab 4 Sharded Key/Value Service

- [ ] The Shard Master
- [ ] Sharded Key/Value Server
  - [ ] Garbage collection of state
  - [ ] Client requests during configuration changes