// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hstate, cstate, _ := c.Storage.InitialState()
	r := &Raft{
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Term:             hstate.Term,
		Vote:             hstate.Vote,
	}

	if c.peers == nil {
		c.peers = cstate.Nodes
	}

	prs := make(map[uint64]*Progress)
	for i := 0; i < len(c.peers); i++ {
		prs[c.peers[i]] = &Progress{
			0, r.RaftLog.LastIndex() + 1,
		}
	}
	r.Prs = prs

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	var entries []*pb.Entry
	for i := prevLogIndex + 1; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, r.RaftLog.Entry(i))
	}

	m := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Commit:  r.RaftLog.committed,
		Entries: entries,
	}
	DPrintf("[%d] append %d entries from {%d} to [%d].", r.id, len(entries), prevLogIndex+1, to)
	r.msgs = append(r.msgs, m)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, m)
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	m := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: term,
		MsgType: pb.MessageType_MsgRequestVote,
	}
	r.msgs = append(r.msgs, m)
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVoteResponse(to uint64, granted bool) {
	// Your Code Here (2A).
	m := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  !granted,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppendResponse sends a append RPC response to the given peer.
func (r *Raft) sendAppendResponse(to uint64, success bool, conflictTerm, conflictIndex uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  !success,
		LogTerm: conflictTerm,
		Index:   conflictIndex,
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) brocast(f func(uint64)) {
	for k, _ := range r.Prs {
		if k == r.id {
			continue
		}

		f(k)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			DPrintf("[%d] heartbeat elapsed, send hearbeat.", r.id)
			if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
				fmt.Printf("error occurred during checking sending heartbeat: %v", err)
			}
		}
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		DPrintf("[%d] election elapsed, send new election.", r.id)
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
			fmt.Printf("error occurred during election: %v", err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.State = StateFollower
	r.Vote = None
	r.reset()
	DPrintf("[%d] become FOLLOWER in term (%d).", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	r.Vote = r.id
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.reset()
	DPrintf("[%d] become CANDIDATE in term (%d).", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.reset()
	r.updateSelfProgress()
	for peer := range r.Prs {
		if peer != r.id {
			r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
		}
	}
	r.RaftLog.Add(&pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})
	DPrintf("[%d] become LEADER in term (%d).", r.id, r.Term)
	r.brocast(func(u uint64) { r.sendAppend(u) })
}

func (r *Raft) reset() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// startElection starts a new rounds of election
func (r *Raft) startElection() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	r.brocast(func(u uint64) { r.sendRequestVote(u) })
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		DPrintf("[%d] receive higher term (%d) -> (%d).", r.id, r.Term, m.Term)
		r.becomeFollower(m.Term, r.Lead)
	}

	switch r.State {
	case StateFollower:
		return r.FollowerMessageHandler(m)
	case StateCandidate:
		return r.CandidateMessageHandler(m)
	case StateLeader:
		return r.LeaderMessageHandler(m)
	}
	return nil
}

func (r *Raft) LeaderMessageHandler(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgBeat:
		r.brocast(func(u uint64) { r.sendHeartbeat(u) })
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.brocast(func(u uint64) { r.sendAppend(u) })
	}
	return nil
}

func (r *Raft) CandidateMessageHandler(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
	return nil
}

func (r *Raft) FollowerMessageHandler(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	if m.Reject {
		DPrintf("[%d] rejected voting by [%d].", r.id, m.From)
	} else {
		DPrintf("[%d] grant voting from [%d].", r.id, m.From)
	}

	voteCount := 0
	for _, v := range r.votes {
		if v {
			voteCount++
		}
	}

	if voteCount > len(r.Prs)/2 {
		DPrintf("[%d] received majority votes.", r.id)
		r.becomeLeader()
		return
	}

	if len(r.votes)-voteCount > len(r.Prs)/2 {
		DPrintf("[%d] request vote rejected by majorities.", r.id)
		r.becomeFollower(r.Term, None)
	}
}

// handlePropose handle Propose RPC request
func (r *Raft) handlePropose(m pb.Message) {
	for _, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + 1
		r.RaftLog.Add(entry)
		DPrintf("[%d] received new log from client: Index={%d}, Term=(%d).", r.id, entry.Index, r.Term)
	}

	r.updateSelfProgress()
	r.brocast(func(u uint64) { r.sendAppend(u) })
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From

	// Reply false if term < currentTerm (§5.1)
	if m.Term != None && m.Term < r.Term {
		DPrintf("[%d] reject entries from [%d] as term(%d) < currentTerm(%d).", r.id, m.From, m.Term, r.Term)
		r.sendAppendResponse(m.From, false, None, None)
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if m.Index > r.RaftLog.LastIndex() {
		DPrintf("[%d] reject entries from [%d] current log index is too shot.", r.id, m.From)
		r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex()+1)
		return
	}
	term, _ := r.RaftLog.Term(m.Index)
	if term != m.LogTerm {
		var conflictIndex uint64
		for index := uint64(1); index < m.Index+1; index++ {
			logTerm, _ := r.RaftLog.Term(index)
			if logTerm == term {
				conflictIndex = r.RaftLog.Entry(index).Index
				break
			}
		}

		DPrintf("[%d] reject entries from [%d] as log term not match.", r.id, m.From)
		r.sendAppendResponse(m.From, false, term, conflictIndex)
		return
	}

	for _, entry := range m.Entries {
		if entry.Index <= r.RaftLog.LastIndex() {
			term, _ := r.RaftLog.Term(entry.Index)

			if term != entry.Term {
				r.RaftLog.SetAndTruncate(entry.Index, entry)
			}
		} else {
			r.RaftLog.Add(entry)
		}
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		DPrintf("[%d] committed to {%d}.", r.id, r.RaftLog.committed)
	}

	r.sendAppendResponse(m.From, true, None, r.RaftLog.LastIndex())
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}

	if m.Reject {
		index := m.Index
		if index == None {
			return
		}

		if m.LogTerm != None {
			conflictTermIndex := -1
			for i := len(r.RaftLog.entries) - 1; i >= 1; i-- {
				if r.RaftLog.entries[i].Term == m.LogTerm {
					conflictTermIndex = i
					break
				}
			}

			if conflictTermIndex != -1 {
				index = r.RaftLog.Entry(uint64(conflictTermIndex + 1)).Index
			}
		}

		r.Prs[m.From].Next = index
		DPrintf("[%d] append reject by [%d], set NextIndex={%d}, retrying.", r.id, m.From, index)
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	r.updateLeaderCommitIndex()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term != None && m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.sendHeartbeatResponse(m.From, false)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		DPrintf("[%d] reject vote for [%d] as its term is low (%d)->(%d).", r.id, m.From, m.Term, r.Term)
		r.sendRequestVoteResponse(m.From, false)
		return
	}

	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if (r.Vote == None || r.Vote == m.From) && isLeastUpToDate(m.Index, m.LogTerm, r.RaftLog.LastIndex(), lastTerm) {
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	if !(r.Vote == None || r.Vote == m.From) {
		DPrintf("[%d] reject vote for [%d] as it already vote for [%d].", r.id, m.From, r.Vote)
	} else {
		DPrintf("[%d] reject vote for [%d] as its log is not up to date.", r.id, m.From)
	}

	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) updateLeaderCommitIndex() {
	for n := r.RaftLog.committed + 1; n <= r.RaftLog.LastIndex(); n++ {
		if r.RaftLog.Entry(n).Term == r.Term {
			validCount := 0
			for id, pr := range r.Prs {
				if id == r.id {
					validCount++
				} else {
					if pr.Match >= n {
						validCount++
					}
				}
			}

			if validCount > len(r.Prs)/2 {
				r.RaftLog.committed = n
				DPrintf("[%d] leader commit to {%d}.", r.id, r.RaftLog.committed)
				r.brocast(func(u uint64) { r.sendAppend(u) })
			}
		}
	}
}

func (r *Raft) updateSelfProgress() {
	// update progress for leader itself
	r.Prs[r.id].Match = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
