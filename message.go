package main

import "fmt"

type dMsg interface {
	getDataMsgDescription() string
	getMataMsg() *dMsgData
	//getMetaDataIdentifier() (string, error)
	//getMetaData() *dataMsgMetaData
}

//
// Concrete meta data can be returned from message types.
type dMsgMetaData struct {
	Path       string
	Identifier string
}

type dMsgData struct {
	dMsgType   string
	dMsgBody   dMsgBody
	dMsgOrigin string
}

type dMsgBody struct {
	timestamp    int64
	path         string
	updates      map[string]interface{}
	deletes      []string
	syncResponse bool
}

func (d *dMsgData) getDataMsgDescription() string {
	//_, id := m.getMetaDataIdentifier()
	return fmt.Sprintf("JSON message [%s]", d.dMsgType)
}

func (d *dMsgData) getMataMsg() *dMsgData {
	return d
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
