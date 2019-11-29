package main

import (
	"time"

	log "github.com/sirupsen/logrus"
)

//
// dataMsgRouter is a router of dataMsgs (collects from one in
// channel, and routes to one of a number of output channels).
// The routing decision algorithm is parameterised and dictated by the
// owner. Behaviour on congestion is also parameterised
type dataMsgRouter struct {
	shutdownChan    chan struct{}
	dataChanIn      chan dMsg
	dataChansOut    []chan dMsg
	route           func(dMsg, int) int
	handleCongested func(dMsg, int, int) dataMsgRouterCongestionAction
	timeout         time.Duration
	logctx          *log.Entry
}

type dataMsgRouterCongestionAction int

const (
	datamsgRouterDrop = iota
	datamsgRouterReroute
	datamsgRouterSendAndBlock
)

func (r *dataMsgRouter) handleMsg(msg dMsg, timeout *time.Timer) {

	for i := 0; true; i++ {

		outChanIndex := r.route(msg, i)
		if len(r.dataChansOut[outChanIndex]) < cap(r.dataChansOut[outChanIndex]) {
			// Easy optimisation. No need to mess with timers. Just hand it on.
			r.dataChansOut[outChanIndex] <- msg
			return
		}

		//
		// Channel backed up and we're about to block. Check whether to block
		// or drop.
		switch r.handleCongested(msg, i, outChanIndex) {
		case datamsgRouterReroute:
			// Do be careful when rerouting to make sure that you do indeed
			// reroute.
			continue
		case datamsgRouterDrop:
			tcLogCtxt.WithFields(log.Fields{
				"file":     "message_router.go",
				"function": "run",
			}).Info("message router drop")
			return
		case datamsgRouterSendAndBlock:
			//
			// We are going to send and block, or timeout.
			timeout.Reset(r.timeout)
			select {
			case r.dataChansOut[outChanIndex] <- msg:
				//
				// Message shipped. Clean out timer, and get out.
				if !timeout.Stop() {
					<-timeout.C
				}
				return
			case <-timeout.C:
				//
				// Let go round one more time.
			}
		}
	}
}

func (r *dataMsgRouter) run() {

	tcLogCtxt.WithFields(log.Fields{
		"file":     "message_router.go",
		"function": "run",
	}).Info("dataMsg router running")

	//
	// Setup stopped timer once.
	timeout := time.NewTimer(r.timeout)
	timeout.Stop()

	for {
		select {
		case <-r.shutdownChan:
			//
			// We're done, and queues all drained
			tcLogCtxt.WithFields(log.Fields{
				"file":     "message_router.go",
				"function": "run",
			}).Info("dataMsg router shutting down")
			//
			// Drain queues. We don't currently close the dataChan
			// before we send shutdown on ctrl chan, but we do
			// unhook input stages. Service as many as there are in
			// queue.
			drain := len(r.dataChanIn)
			for i := 0; i < drain; i++ {
				r.handleMsg(<-r.dataChanIn, timeout)
			}
			for _, c := range r.dataChansOut {
				//
				// conventional pattern to serialise consuming last
				// batch of messages, then shutting down.
				close(c)
			}
			tcLogCtxt.WithFields(log.Fields{
				"file":     "message_router.go",
				"function": "run",
			}).Info("dataMsg router shut down")
			return

		case msg := <-r.dataChanIn:
			r.handleMsg(msg, timeout)
		}
	}
}
