package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// tapOutput
type tapOutput struct {
	name             string
	filename         string
	countOnly        bool
	rawDump          bool
	dataChannelDepth int
	cChan            chan *cMsg
	dChan            chan dMsg
}

func tapOutputCapabilityNew() outputCapability {
	return &tapOutput{}
}

// Setup a tap output entity
func (t *tapOutput) initialize(name string, ec entityConfig) (
	chan<- dMsg, chan<- *cMsg, error) {

	var err error

	t.name = name

	t.filename, err = ec.config.GetString(name, "file")
	if err != nil {
		return nil, nil, err
	}

	t.dataChannelDepth, err = ec.config.GetInt(name, "datachanneldepth")
	if err != nil {
		t.dataChannelDepth = dataChannelDepth
	}

	t.countOnly, _ = ec.config.GetBool(name, "countonly")

	t.rawDump, _ = ec.config.GetBool(name, "raw")
	if t.rawDump {
		if t.countOnly {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{"name": name}).Error(
				"tap config: 'countonly' is incompatible with 'raw'")
			return nil, nil, err
		}
	}

	// Setup control and data channels
	t.cChan = make(chan *cMsg)
	t.dChan = make(chan dMsg, t.dataChannelDepth)

	go t.tapOutputLoop()

	return t.dChan, t.cChan, nil

}

func (t *tapOutput) tapOutputLoop() {
	var stats msgStats

	// Period, in seconds, to dump stats if only counting.
	const TIMEOUT = 10
	timeout := make(chan bool, 1)

	tcLogCtxt.WithFields(
		log.Fields{
			"name":      t.name,
			"filename":  t.filename,
			"countonly": t.countOnly,
			"rawDump":   t.rawDump,
		})

	// Prepare dump file for writing
	f, err := os.OpenFile(t.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		0660)
	if err != nil {
		tcLogCtxt.WithError(err).Error("Tap failed to open dump file")
		return
	}
	defer f.Close()

	tcLogCtxt.Info("Starting up tap")

	if t.countOnly {
		go func() {
			time.Sleep(TIMEOUT * time.Second)
			timeout <- true
		}()
	}

	w := bufio.NewWriter(f)

	for {
		select {

		case <-timeout:

			go func() {
				time.Sleep(TIMEOUT * time.Second)
				timeout <- true
			}()
			w.WriteString(fmt.Sprintf(
				"%s:%s: rxed msgs: %v\n",
				t.name, time.Now().Local(), stats.msgsOK))
			w.Flush()

		case dMsg, ok := <-t.dChan:

			if !ok {
				// Channel has been closed. Our demise
				// is near. SHUTDOWN is likely to be
				t.dChan = nil
				continue
			}

			if t.countOnly {
				stats.msgsOK++
				continue
			}

			stats.msgsOK++
			msgType := dMsg.getDataMsgDescription()
			o := dMsg.getDataMsgOrigin()

			w.WriteString(fmt.Sprintf("\n------- %v -------\n", time.Now()))
			w.WriteString(fmt.Sprintf("From : %s ; Summary: %s\n", o, msgType))
			//var out bytes.Buffer
			//json.Indent(&out, b, "", "    ")
			fmt.Printf("Tap module raw data received: %#v \n", dMsg)
			d := dMsg.getDataMsgBody()
			fmt.Printf("Tap module raw data received after function: %#v \n", *d)
			js, _ := json.MarshalIndent(*d, "", "  ")
			fmt.Printf("Tap module data received: %s \n", js)
			w.WriteString(string(js))
			//w.WriteString(out.String())

			w.Flush()

		case msg := <-t.cChan:
			switch msg.id {
			case report:
				content, _ := json.Marshal(stats)
				resp := &cMsg{
					id:       ack,
					data:     content,
					respChan: nil,
				}
				msg.respChan <- resp

			case shutdown:

				w.Flush()
				tcLogCtxt.Info("tap feeder loop, rxed SHUTDOWN")

				// Dump detailed stats here

				resp := &cMsg{
					id:       ack,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				tcLogCtxt.Info("tap feeder loop, unknown ctrl message")
			}
		}

	}
}
