package main

import (
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type metricsOutputType int
type metricsOutputContext interface{}

const (
	metricsTypePrometheus = iota
	metricsTypeInflux
	metricsTypeFile
)

var metricsTypeMap = map[string]metricsOutputType{
	"prometheus": metricsTypePrometheus,
	"influx":     metricsTypeInflux,
	"file":       metricsTypeFile,
}

// metricOutput
type metricsOutput struct {
	name string
	//inputSpecFile     string
	//inputSpec         metricsSpec
	output            string
	outputHandler     metricsOutputHandler
	dataChannelDepth  int
	cChan             chan *cMsg
	dChan             chan dMsg
	shutdownSyncPoint sync.WaitGroup
	shutdownChan      chan struct{}
}

type metricsSpec struct {
	specSet []metricsSpecForBasepath
	specDB  map[string]*metricsSpecNode
	//stats   *statRecordTable
}

type metricsSpecForBasepath struct {
	Path    string
	Updates *metricsSpecNode
}

type metricsOutputHandler interface {
	setupWorkers(module *metricsOutput)
	//buildMetric(tags []metricsAtom, sensor metricsAtom, ts uint64,
	//	context metricsOutputContext)
	//flushMetric(tags []metricsAtom, ts uint64, context metricsOutputContext)
	//adaptSensorName(name string) string
	//adaptTagName(name string) string
}

// Structures used to collect metrics and tags.
type metricsAtom struct {
	key string
	val interface{}
}

type metricsSpecNode struct {
	Name  string
	Tag   bool
	Track bool
}

func metricOutputCapabilityNew() outputCapability {
	return &metricsOutput{}
}

// Setup a tap output entity
func (m *metricsOutput) initialize(name string, ec entityConfig) (
	chan<- dMsg, chan<- *cMsg, error) {

	var err error

	m.name = name

	/*

		m.inputSpecFile, err = ec.config.GetString(name, "metricsInputFileSpec")
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"name": name,
				}).Error("metrics initialize: metricsInputFileSpec is required")
			return nil, nil, err
		}

		specByteStream, err := ioutil.ReadFile(m.inputSpecFile)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"name": name,
				}).Error("metrics initialize: metricsInputFileSpec error reading file")
			return nil, nil, err
		}

		err = json.Unmarshal(specByteStream, &m.inputSpec.specSet)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"name": name,
					"file": m.inputSpecFile,
				}).Error("metrics initialize: error parsing JSON metric spec")
			return nil, nil, err

		}
	*/

	m.output, err = ec.config.GetString(name, "output")
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name":    name,
				"options": metricsTypeMap,
			}).Error("metrics initialize: output option: required")
		return nil, nil, err

	}

	outputType, ok := metricsTypeMap[m.output]
	if !ok {
		err = fmt.Errorf(
			"invalid 'output' [%s], must be one of [%v]",
			m.output, metricsTypeMap)
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name":    name,
				"options": metricsTypeMap,
			}).Error("metrics initialize: output option: unsupported")
		return nil, nil, err
	}

	// Let output handler set itself up.
	switch outputType {
	case metricsTypePrometheus:
		m.outputHandler, err = metricsPrometheusNew(name, ec)
		if err != nil {
			return nil, nil, err
		}
		/*
			case metricsTypeInflux:
				m.outputHandler, err = metricsInfluxNew(name, ec)
				if err != nil {
					//
					// Errors logged in called function
					return nil, nil, err
				}
			case metricsTypeFile:
				m.outputHandler = testOutputHandler
				if m.outputHandler == nil {
					return nil, nil, fmt.Errorf("test metric extraction unset")
				}
		*/
	case metricsTypeInflux:
		m.outputHandler, err = metricsInfluxNew(name, ec)
		if err != nil {
			return nil, nil, err
		}
	default:
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name":   name,
				"output": m.output,
			}).Error("metrics initialize: output option: failed to setup")
		return nil, nil, err

	}
	/*
		err = m.buildSpecDB()
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"name": name,
				}).Error("metrics initialize: building Spec DB failed")
			return nil, nil, err
		}
	*/

	m.dataChannelDepth, err = ec.config.GetInt(name, "datachanneldepth")
	if err != nil {
		m.dataChannelDepth = dataChannelDepth
	}

	tcLogCtxt.WithFields(
		log.Fields{
			"name":   name,
			"output": m.output,
			//"file":       m.inputSpecFile,
			//"metricSpec": m.inputSpec,
		}).Info("metrics export configured")

	//
	// Setup control and data channels
	m.cChan = make(chan *cMsg)
	m.dChan = make(chan dMsg, m.dataChannelDepth)
	m.shutdownChan = make(chan struct{})

	go m.metricLoop()

	return m.dChan, m.cChan, nil

}

func (m *metricsOutput) metricLoop() {
	var stats msgStats

	// Kick off data handlers and run control loop
	m.outputHandler.setupWorkers(m)

	for {
		select {
		case msg := <-m.cChan:
			switch msg.id {
			case report:
				data, _ := json.Marshal(stats)
				resp := &cMsg{
					id:       ack,
					data:     data,
					respChan: nil,
				}
				msg.respChan <- resp

			case shutdown:
				tcLogCtxt.Info("metrics loop, received shutdown")

				// Signal any children that we are done.
				close(m.shutdownChan)
				m.shutdownSyncPoint.Wait()

				// We're done pass it on. Would have been so nice to
				// use this wait group pattern trhoughout.
				resp := &cMsg{
					id:       ack,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				tcLogCtxt.Error("metrics producer, unknown ctrl message")
			}
		}
	}

}
