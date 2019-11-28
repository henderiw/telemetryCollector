package main

import "fmt"

type dMsg interface {
	getDataMsgDescription() string
	getDataMsgBody() *dMsgBody
	getDataMsgOrigin() string
	produceMetrics(*metricsSpec, metricsOutputHandler, metricsOutputContext) error
	//getMetaDataIdentifier() (string, error)
	//getMetaData() *dataMsgMetaData
}

//
// Concrete meta data can be returned from message types.
type dMsgMetaData struct {
	Path       string
	Identifier string
}

type dMsgJSON struct {
	DMsgType   string
	DMsgBody   dMsgBody
	DMsgOrigin string
}

type dMsgBody struct {
	Timestamp    int64
	Path         string
	Updates      map[string]interface{}
	Deletes      []string
	SyncResponse bool
}

func (d *dMsgJSON) getDataMsgDescription() string {
	//_, id := m.getMetaDataIdentifier()
	return fmt.Sprintf("JSON message [%s]", d.DMsgType)
}

func (d *dMsgJSON) getDataMsgBody() *dMsgBody {
	return &d.DMsgBody
}

func (d *dMsgJSON) produceMetrics(spec *metricsSpec, outputHandler metricsOutputHandler, buf metricsOutputContext,
) error {

	node, ok := spec.specDB[d.DMsgBody.Path]
	if !ok {
		return nil
	}
	tags := make([]metricsAtom, 2, 8)
	tags[0].key = "Path"
	tags[0].val = d.DMsgBody.Path
	tags[1].key = "Producer"
	tags[1].val = d.DMsgOrigin

	return d.produceJSONMetrics(
		spec,
		node,
		outputHandler,
		tags,
		buf)
}

func (d *dMsgJSON) produceJSONMetrics(
	spec *metricsSpec,
	node *metricsSpecNode,
	outputHandler metricsOutputHandler,
	tags []metricsAtom,
	buf metricsOutputContext) error {

	return nil

}

func (d *dMsgJSON) getDataMsgOrigin() string {
	return d.DMsgOrigin
}

type msgID int

const (
	// Used to request to shutdown, expects ACK on respChan
	shutdown msgID = iota
	// Request to report back on pipeline node state
	report
	// Acknowledge a request.
	ack
)

// Control message channel type
type cMsg struct {
	id       msgID
	data     []byte
	respChan chan *cMsg
}

type msgStats struct {
	msgsOK  uint64
	msgsNOK uint64
}
