package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	cluster "gopkg.in/bsm/sarama-cluster.v2"
)

// kafkaInputConsumer
type kafkaInputConsumer struct {
	name          string
	brokerList    []string
	consumerGroup string
	topic         string
	key           string
	logData       bool
	cChan         chan *cMsg
	dChan         chan dMsg
	stats         msgStats
}

func kafkaInputCapabilityNew() inputCapability {
	return &kafkaInputConsumer{}
}

// Setup a kafka intput consumer entity
func (k *kafkaInputConsumer) initialize(name string, ec entityConfig, dChans []chan<- dMsg) (
	chan<- *cMsg, error) {

	k.name = name

	brokers, err := ec.config.GetString(name, "brokers")
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"broker configuration is required to talk to the kafka cluster")
		return nil, err
	}
	k.brokerList = strings.Split(brokers, ",")

	k.consumerGroup, err = ec.config.GetString(name, "consumergroup")
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{"name": name}).Error(
			"kafka consumer requires 'consumergroup' identifying consumer")
		return nil, err
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
	cChan := make(chan *cMsg)
	go k.kafkaConsumerConnection(cChan, dChans)

	return cChan, nil
}

func (k *kafkaInputConsumer) kafkaConsumerConnection(cChan <-chan *cMsg, dChans []chan<- dMsg) {
	var consumer *cluster.Consumer
	var client *cluster.Client
	var notifications <-chan *cluster.Notification
	var errorChan <-chan error
	var msgChan <-chan *sarama.ConsumerMessage
	var checkpointMsg *sarama.ConsumerMessage

	// Timeout channel
	timeout := make(chan bool, 1)

	// Checkpoint offset timer
	checkpointFlush := make(chan bool, 1)
	checkpointFlushScheduled := false

	for {

		//var r bool

		// Get a default configuration
		clusterConfig := cluster.NewConfig()

		// Register to receive notifications on rebalancing of
		// partition to consumer mapping. This allows us to
		// have multiple consumers in consumer group, and
		// allows us to adapt as we add and remove such
		// consumers.
		clusterConfig.Group.Return.Notifications = true
		clusterConfig.Consumer.Return.Errors = true

		client, err = cluster.NewClient(k.brokerList, clusterConfig)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"name":    k.name,
					"topic":   k.topic,
					"group":   k.consumerGroup,
					"brokers": k.brokerList,
				}).Error("kafka consumer client failure issue (will retry)")
			//retry to be build in
		}

		if client != nil {
			consumer, err = cluster.NewConsumerFromClient(
				client, k.consumerGroup, []string{k.topic})

			if err != nil {
				tcLogCtxt.WithError(err).WithFields(
					log.Fields{
						"name":    k.name,
						"topic":   k.topic,
						"group":   k.consumerGroup,
						"brokers": k.brokerList,
					}).Error("kafka consumer create failure (will retry)")

			}
		}

		if consumer != nil {
			// Listen to notifications, for visibility only
			notifications = consumer.Notifications()

			// Listen to and log errors
			errorChan = consumer.Errors()

			// Listen for messages...
			msgChan = consumer.Messages()

			tcLogCtxt.WithFields(
				log.Fields{
					"name":    k.name,
					"topic":   k.topic,
					"group":   k.consumerGroup,
					"brokers": k.brokerList,
				}).Info("kafka consumer connected")

		} else {
			// Go into select loop, with scheduled retry
			// in a second. Note that this will make sure we
			// remain responsive and handle cleanup signal
			// correctly if necessary.
			go func() {
				time.Sleep(
					kafkaReconnectTimeout * time.Second)
				timeout <- true
			}()
		}
		restart := false
		for {

			select {
			//
			// Handle rebalancing, shutdown, and retry in
			// this select clause
			case remoteErr := <-errorChan:
				tcLogCtxt.WithError(remoteErr).WithFields(
					log.Fields{
						"name":    k.name,
						"topic":   k.topic,
						"group":   k.consumerGroup,
						"brokers": k.brokerList,
					}).Error("kafka consumer rxed Error")
			case restartRequest := <-timeout:
				// Handle timeout, simply restart the whole
				// sequence trying to connect to kafka as
				// consumer.
				if restartRequest {
					tcLogCtxt.WithFields(
						log.Fields{
							"name":    k.name,
							"topic":   k.topic,
							"group":   k.consumerGroup,
							"brokers": k.brokerList,
						}).Debug("kafka consumer, attempt reconnect")
					restart = true
				}
			case <-checkpointFlush:
				checkpointFlushScheduled = false
				if checkpointMsg != nil {
					consumer.MarkOffset(checkpointMsg, "ok")
				}
				checkpointMsg = nil
			case data := <-msgChan:
				checkpointMsg = data
				if !checkpointFlushScheduled {
					checkpointFlushScheduled = true
					go func() {
						time.Sleep(kafkaCheckpointFlush * time.Second)
						checkpointFlush <- true
					}()
				}

				fmt.Printf("Kafka Data: %s \n", data.Value)
				d := &dMsgBody{}
				err := json.Unmarshal([]byte(data.Value), d)
				if err != nil {
					tcLogCtxt.WithError(err).WithFields(
						log.Fields{
							"name":    k.name,
							"topic":   k.topic,
							"group":   k.consumerGroup,
							"brokers": k.brokerList,
						}).Error("unmarshal error")
				}
				msg := &dMsgJSON{
					DMsgBody:   *d,
					DMsgOrigin: d.Node,
				}
				/*
					fmt.Printf("Kafka Data Timestamp: %d \n", d.Timestamp)
					fmt.Printf("Kafka Data Node: %s \n", d.Node)
					fmt.Printf("Kafka Data Path: %s \n", d.Path)
					for update, value := range d.Updates {
						fmt.Printf("Kafka Data Update Key: %s \n", update)
						fmt.Printf("Kafka Data Update Value: %s \n", value)
					}
				*/

				for i, dChan := range dChans {
					if cap(dChan) == len(dChan) {
						//
						// Data channel full; blocking.
						// If we choose to add tail drop option to avoid
						// head-of-line blocking, this is where we would
						// drop.
						tcLogCtxt.WithFields(
							log.Fields{
								"name":    k.name,
								"topic":   k.topic,
								"group":   k.consumerGroup,
								"brokers": k.brokerList,
								"channel": i,
							}).Debug("kafka consumer overrun (increase 'datachanneldepth'?)")
					}
					//
					// Pass it on. Make sure we handle shutdown
					// gracefully too.
					select {
					case dChan <- msg:
						continue
					case msg := <-cChan:
						if k.kafkaHandleCtrlMsg(msg) {
							return
						}
					}
				}

			case notification := <-notifications:
				//
				// Rebalancing activity. Setup and
				// teardown partition readers
				// according to rebalance.
				tcLogCtxt.WithFields(
					log.Fields{
						"name":     k.name,
						"topic":    k.topic,
						"group":    k.consumerGroup,
						"brokers":  k.brokerList,
						"claimed":  notification.Claimed,
						"released": notification.Released,
						"current":  notification.Current,
					}).Info("kafka consumer, notification")

			case msg := <-cChan:
				if k.kafkaHandleCtrlMsg(msg) {
					return
				}
			}

			if restart {
				break
			}

		}

	}
}

func (k *kafkaInputConsumer) kafkaHandleCtrlMsg(msg *cMsg) bool {

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
			"name":    k.name,
			"topic":   k.topic,
			"group":   k.consumerGroup,
			"brokers": k.brokerList,
		}).Info("kafka consumer rxed shutdown")

		resp := &cMsg{
			id:       ack,
			respChan: nil,
		}
		msg.respChan <- resp
		return true

	default:
		tcLogCtxt.WithError(err).WithFields(log.Fields{
			"name":    k.name,
			"topic":   k.topic,
			"group":   k.consumerGroup,
			"brokers": k.brokerList,
		}).Error("kafka loop, unknown ctrl message")
	}

	return false
}
