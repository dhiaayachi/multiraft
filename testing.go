package multiraft

import (
	"context"
	"errors"
	"fmt"
	"github.com/dhiaayachi/multiraft/partition"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/dhiaayachi/raft"
	"github.com/hashicorp/go-hclog"
	"testing"
	"time"
)

type cluster struct {
	t               *testing.T
	mr              []*MultiRaft
	addr            []raft.ServerAddress
	id              []raft.ServerID
	trans           []*raft.InmemTransport
	logger          hclog.Logger
	longStopTimeout time.Duration
	startTime       time.Time
	observationCh   chan raft.Observation
}

func (c *cluster) GetInState(s raft.RaftState, partition partition.Typ) []*raft.Raft {
	c.logger.Info("starting stability test", "raft-state", s)
	limitCh := time.After(c.longStopTimeout)
	if len(c.mr) < 1 {
		c.t.Fatalf("cluster empty")
	}

	// An election should complete after 2 * max(HeartbeatTimeout, ElectionTimeout)
	// because of the randomised timer expiring in 1 x interval ... 2 x interval.
	// We add a bit for propagation delay. If the election fails (e.g. because
	// two elections start at once), we will have got something through our
	// observer channel indicating a different state (i.e. one of the nodes
	// will have moved to candidate state) which will reset the timer.
	//
	// Because of an implementation peculiarity, it can actually be 3 x timeout.
	timeout := c.mr[0].conf.HeartbeatTimeout
	if timeout < c.mr[0].conf.ElectionTimeout {
		timeout = c.mr[0].conf.ElectionTimeout
	}
	timeout = 2*timeout + c.mr[0].conf.CommitTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Wait until we have a stable instate slice. Each time we see an
	// observation a state has changed, recheck it and if it has changed,
	// restart the timer.
	pollStartTime := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		inState := c.pollState(s, partition)
		inStateTime := time.Now()

		// Filter will wake up whenever we observe a RequestVote.
		filter := func(ob *raft.Observation) bool {
			switch ob.Data.(type) {
			case raft.RaftState:
				return true
			case raft.RequestVoteRequest:
				return true
			default:
				return false
			}
		}

		eventCh := c.WaitEventChan(ctx, filter)
		select {

		case <-limitCh:
			c.t.Fatalf("timeout waiting for stable %s state", s)

		case <-eventCh:
			c.logger.Debug("resetting stability timeout")

		case t, ok := <-timer.C:
			if !ok {
				c.t.Fatalf("timer channel errored")
			}

			c.logger.Info(fmt.Sprintf("stable state for %s reached at %s (%d nodes), %s from start of poll, %s from cluster start. Timeout at %s, %s after stability",
				s, inStateTime, len(inState), inStateTime.Sub(pollStartTime), inStateTime.Sub(c.startTime), t, t.Sub(inStateTime)))
			return inState
		}
	}
}

// pollState takes a snapshot of the state of the cluster. This might not be
// stable, so use GetInState() to apply some additional checks when waiting
// for the cluster to achieve a particular state.
func (c *cluster) pollState(s raft.RaftState, partition partition.Typ) []*raft.Raft {
	in := make([]*raft.Raft, 0, 1)

	i := 0
	rafts := make([]*raft.Raft, 0)
	for _, mr := range c.mr {
		mr.raftsLock.RLock()
		rs := mr.rafts
		r, ok := rs[partition]
		if ok {
			rafts = append(rafts, r)
		}
		i++
		mr.raftsLock.RUnlock()
	}

	for _, r := range rafts {
		if r.State() == s {
			in = append(in, r)
		}
	}
	return in
}

// WaitEventChan returns a channel which will signal if an observation is made
// or a timeout occurs. It is possible to set a filter to look for specific
// observations. Setting timeout to 0 means that it will wait forever until a
// non-filtered observation is made.
func (c *cluster) WaitEventChan(ctx context.Context, filter raft.FilterFn) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case o, ok := <-c.observationCh:
				if !ok || filter == nil || filter(&o) {
					return
				}
			}
		}
	}()
	return ch
}

func createCluster(t *testing.T, num int) (*cluster, error) {

	mrs := make([]*MultiRaft, 0)
	addresses := make([]raft.ServerAddress, 0)
	ids := make([]raft.ServerID, 0)
	transports := make([]*raft.InmemTransport, 0)
	for i := 0; i < num; i++ {
		config := raft.DefaultConfig()
		addr, transportRaft := raft.NewInmemTransport("")
		config.LocalID = raft.ServerID(fmt.Sprintf("id%d", i))
		multiRaft, err := NewMultiRaft(
			config,
			func() raft.FSM {
				return &raft.MockFSM{}
			},
			func() raft.LogStore {
				return raft.NewInmemStore()
			},
			func() raft.StableStore {
				return raft.NewInmemStore()
			},
			func() raft.SnapshotStore {
				return raft.NewDiscardSnapshotStore()
			},
			transport.NewMuxTransport(transportRaft, hclog.Default()),
		)
		if err != nil {
			return nil, err
		}
		mrs = append(mrs, multiRaft)
		addresses = append(addresses, addr)
		ids = append(ids, config.LocalID)
		transports = append(transports, transportRaft)
	}
	return &cluster{t: t, mr: mrs, addr: addresses, id: ids, trans: transports, logger: hclog.Default(), startTime: time.Now(), longStopTimeout: 5 * time.Second}, nil

}

func waitForLeader(c *cluster, partition partition.Typ) error {
	count := 0
	for count < 100 {
		r := c.GetInState(raft.Leader, partition)
		if len(r) >= 1 {
			return nil
		}
		count++
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("no leader elected")
}
