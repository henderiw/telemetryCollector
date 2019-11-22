package main

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	kafkaDefaultTopic     = "telemetry"
	kafkaTopicMaxlen      = 250
	kafkaReconnectTimeout = 1
	kafkaCheckpointFlush  = 1
)

// kafkaOutputProducer
type kafkaOutputProducer struct {
	name             string
	brokerList       []string
	dataChannelDepth int
	topic            string
	key              string
	logData          bool
	requiredAcks     sarama.RequiredAcks
	cChan            chan *cMsg
	dChan            chan dMsg
	stats            msgStats
}

func kafkaOutputCapabilityNew() outputCapability {
	return &kafkaOutputProducer{}
}

// Setup a kafka output producer entity
func (k *kafkaOutputProducer) initialize(name string, ec entityConfig) (
	chan<- dMsg, chan<- *cMsg, error) {

	k.name = name

	brokers, err := ec.config.GetString(name, "brokers")
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"broker configuration is required to talk to the kafka cluster")
		return nil, nil, err
	}
	k.brokerList = strings.Split(brokers, ",")

	k.dataChannelDepth, err = ec.config.GetInt(name, "datachanneldepth")
	if err != nil {
		k.dataChannelDepth = dataChannelDepth
	}

	k.topic, err = ec.config.GetString(name, "topic")
	if err != nil {
		k.topic = kafkaDefaultTopic
	}

	k.key, err = ec.config.GetString(name, "key")
	if err != nil {
		k.key = ""
	}

	k.logData, _ = ec.config.GetBool(name, "logdata")
	if err != nil {
		k.logData = false
	}

	requiredAcks, err := ec.config.GetString(name, "requiredAcks")
	if err != nil {
		k.requiredAcks = sarama.NoResponse
	} else {
		switch {
		case requiredAcks == "none":
			k.requiredAcks = sarama.NoResponse
		case requiredAcks == "local":
			k.requiredAcks = sarama.WaitForLocal
		case requiredAcks == "commit":
			k.requiredAcks = sarama.WaitForAll
		default:
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{"name": name}).Error(
				"'requiredAcks' option expects 'local' or 'commit'")
		}
	}

	tcLogCtxt.WithFields(
		log.Fields{
			"name":         name,
			"topic":        k.topic,
			"brokers":      k.brokerList,
			"requiredAcks": k.requiredAcks,
		})

	// Create the required channels; a sync ctrl channel and a data channel.
	k.cChan = make(chan *cMsg)
	k.dChan = make(chan dMsg, dataChannelDepth)

	go k.kafkaOutputProducerLoop()

	return k.dChan, k.cChan, nil

}

func (k *kafkaOutputProducer) kafkaOutputProducerLoop() {
	var producer sarama.SyncProducer
	var err error

	for {
		config := sarama.NewConfig()
		config.Producer.RequiredAcks = k.requiredAcks
		config.Producer.Retry.Max = 5
		config.Producer.Return.Successes = true
		producer, err = sarama.NewSyncProducer(k.brokerList, config)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(log.Fields{
				"name":         k.name,
				"topic":        k.topic,
				"brokers":      k.brokerList,
				"requiredAcks": k.requiredAcks,
			}).Error("kafka producer setup failed (will retry)")

			select {
			case <-time.After(kafkaReconnectTimeout * time.Second):
				// Time to try again
				break
			case msg := <-k.cChan:
				if k.kafkaHandleCtrlMsg(msg) {
					// We're going down. Given up and leave.
					return
				}
			}
			continue
		} else {
			break
		}
	}
	// We're all setup and ready to go...
	k.kafkaFeederLoop(producer)

}

// Loop taking content on channel and pushing out to kafka
func (k *kafkaOutputProducer) kafkaFeederLoop(producer sarama.SyncProducer) {

	tcLogCtxt.WithFields(log.Fields{
		"name":         k.name,
		"topic":        k.topic,
		"brokers":      k.brokerList,
		"requiredAcks": k.requiredAcks,
	}).Info("kafka producer configured")

	for {
		select {
		case dMsg, ok := <-k.dChan:

			if !ok {
				// Channel has been closed. Our demise
				// is near. SHUTDOWN is likely to be done
				k.dChan = nil
				continue
			}
			k.stats.msgsOK++
			d := dMsg.getDataMsgBody()
			js, _ := json.MarshalIndent(*d, "", "  ")

			msg := &sarama.ProducerMessage{
				Topic: k.topic,
				Value: sarama.StringEncoder(string(js)),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				panic(err)
			}

			if err != nil {
				k.stats.msgsNOK++

				tcLogCtxt.WithError(err).WithFields(log.Fields{
					"name":         k.name,
					"topic":        k.topic,
					"brokers":      k.brokerList,
					"requiredAcks": k.requiredAcks,
					"partition":    partition,
					"offset":       offset,
				}).Error("kafka feeder loop, failed message")

			} else {
				k.stats.msgsOK++
				tcLogCtxt.WithFields(log.Fields{
					"name":         k.name,
					"topic":        k.topic,
					"brokers":      k.brokerList,
					"requiredAcks": k.requiredAcks,
					"partition":    partition,
					"offset":       offset,
				}).Info("message is stored")
			}

		case msg := <-k.cChan:
			if k.kafkaHandleCtrlMsg(msg) {
				// Close connection to kafka.
				if err := producer.Close(); err !=

					nil {
					tcLogCtxt.WithError(err).WithFields(log.Fields{
						"name":         k.name,
						"topic":        k.topic,
						"brokers":      k.brokerList,
						"requiredAcks": k.requiredAcks,
					}).Error("Failed to shut down producer cleanly")

				}
				return
			}
		}
	}
}

func (k *kafkaOutputProducer) kafkaHandleCtrlMsg(msg *cMsg) bool {

	switch msg.id {

	case report:
		content, _ := json.Marshal(k.stats)
		resp := &cMsg{
			id:       ack,
			data:     content,
			respChan: nil,
		}
		msg.respChan <- resp

	case shutdown:
		tcLogCtxt.WithFields(log.Fields{
			"name":         k.name,
			"topic":        k.topic,
			"brokers":      k.brokerList,
			"requiredAcks": k.requiredAcks,
		}).Info("kafka producer rxed shutdown")

		resp := &cMsg{
			id:       ack,
			respChan: nil,
		}
		msg.respChan <- resp
		return true

	default:
		tcLogCtxt.WithError(err).WithFields(log.Fields{
			"name":         k.name,
			"topic":        k.topic,
			"brokers":      k.brokerList,
			"requiredAcks": k.requiredAcks,
		}).Error("kafka loop, unknown ctrl message")
	}

	return false
}
