package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/template"

	"github.com/dlintw/goconf"
	log "github.com/sirupsen/logrus"
)

// define base variable to support the go telemtry collector project
var version = "0.1.0"
var err error

// system constants
const (
	dataChannelDepth = 1000
)

// initializing logging function
var tcLog = log.New()
var tcLogCtxt = tcLog.WithFields(log.Fields{
	"tag": "telemetryCollector",
})

// tc holds the information of the telemtry Collector
var tc tcInfo

// tcInfo type defines the information of the telemetry Collector
type tcInfo struct {
	// Configuration File to support the telemtry collector
	configFile string
	// Output Capability
	outputCapabilities map[string](func() outputCapability)
	// Input entity keyed by name
	outputEntity map[string]*outputEntity
	// Input Capability
	inputCapabilities map[string](func() inputCapability)
	// Input entity keyed by name
	inputEntity map[string]*inputEntity
}

// types to support extracting the configuration from go text/templates
type configEnv struct {
}

type entityConfig struct {
	config *goconf.ConfigFile
}

// entity type defines the information elements associated with it
type entity struct {
	name  string
	cChan chan<- *cMsg
}

// outputCapability defines the function definition mapping to the various
// capabilities the telemetry collector supports.
// It provides a uniform definition amongst all modules/capabilities the TC supports
type outputCapability interface {
	initialize(entityName string, ec entityConfig) (
		chan<- dMsg, chan<- *cMsg, error)
}

// outputEntity type defines the specific information elements for the output entity
type outputEntity struct {
	entity
	dataChan chan<- dMsg
}

// inputCapability defines the function definition mapping to the various
// capabilities the telemetry collector supports.
// It provides a uniform definition amongst all modules/capabilities the TC supports
type inputCapability interface {
	initialize(entityName string, ec entityConfig, chans []chan<- dMsg) (
		chan<- *cMsg, error)
}

// inputEntity type defines the specific information elements for the input entity
type inputEntity struct {
	entity
}

// loadingConfig loads the configuration and initializes the telemetry collector.
func loadingConfig() error {
	var configBuffer bytes.Buffer
	var ec entityConfig
	var dataChans = make([]chan<- dMsg, 0)

	// Loading file using go text templates
	configAsTemplate, err := template.ParseFiles(tc.configFile)
	if err != nil {
		return fmt.Errorf("failed to open configuration file %s, %v",
			tc.configFile, err)
	}
	err = configAsTemplate.Execute(&configBuffer, &configEnv{})
	if err != nil {
		return err
	}
	cfg, err := goconf.ReadConfigBytes(configBuffer.Bytes())
	if err != nil {
		return fmt.Errorf("read configuration file %s, %v",
			tc.configFile, err)
	}

	ec.config = cfg

	// Loading the output sections - To do
	for _, section := range cfg.GetSections() {
		if section == "default" {
			continue
		}
		goal, err := cfg.GetString(section, "goal")
		if err != nil {
			return fmt.Errorf("Sections require a 'goal': %v", err)
		}
		if goal != "output" {
			continue
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "loadingConfig",
			"section":  section,
			"goal":     goal,
		}).Info("Processing section, stage ...")

		goalType, err := cfg.GetString(section, "type")
		if err != nil {
			return fmt.Errorf("Sections require a 'type' attribute: %v", err)
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "loadingConfig",
			"section":  section,
			"goal":     goal,
			"type":     goalType,
		}).Info("Processing section, stage, type ...")
		outFunction, ok := tc.outputCapabilities[goalType]
		if !ok {
			return fmt.Errorf("Unsupported 'type' attribute for input module [%s]", goalType)
		}
		entityFunction := outFunction()

		dChan, cChan, err := entityFunction.initialize(section, ec)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(log.Fields{
				"file":     "telemetryCollector.go",
				"function": "loadingConfig",
				"section":  section,
				"goal":     goal,
				"type":     goalType,
			}).Error("dataCollector failed to start up section")
			continue
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "loadingConfig",
			"section":  section,
			"goal":     goal,
			"type":     goalType,
		}).Info("dataCollector starting up section")
		ie := new(outputEntity)
		ie.name = section
		ie.cChan = cChan
		ie.dataChan = dChan
		dataChans = append(dataChans, dChan)
		tc.outputEntity[section] = ie
	}

	// Loading the input sections
	for _, section := range cfg.GetSections() {
		if section == "default" {
			continue
		}
		goal, err := cfg.GetString(section, "goal")
		if err != nil {
			return fmt.Errorf("Sections require a 'goal': %v", err)
		}
		if goal != "input" {
			continue
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "loadingConfig",
			"section":  section,
			"goal":     goal,
		}).Info("Processing section, stage ...")

		goalType, err := cfg.GetString(section, "type")
		if err != nil {
			return fmt.Errorf("Sections require a 'type' attribute: %v", err)
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "loadingConfig",
			"section":  section,
			"goal":     goal,
			"type":     goalType,
		}).Info("Processing section, stage, type ...")
		inFunction, ok := tc.inputCapabilities[goalType]
		if !ok {
			return fmt.Errorf("Unsupported 'type' attribute for input module [%s]", goalType)
		}
		entityFunction := inFunction()

		inCtrl, err := entityFunction.initialize(section, ec, dataChans)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(log.Fields{
				"file":     "telemetryCollector.go",
				"function": "loadingConfig",
				"section":  section,
				"goal":     goal,
				"type":     goalType,
			}).Error("dataCollector failed to start up section")
			continue
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "loadingConfig",
			"section":  section,
			"goal":     goal,
			"type":     goalType,
		}).Info("dataCollector starting up section")
		ie := new(inputEntity)
		ie.name = section
		ie.cChan = inCtrl
		tc.inputEntity[section] = ie

	}
	return nil
}

// run the loop waiting for messages, polling state periodically to
// report what is going on, and waiting for interrupt to exit clean.
func run() {
	// Wait for a SIGINT, (typically triggered from CTRL-C), TERM,
	// QUIT. Run cleanup when signal is received. Ideally use os
	// independent os.Interrupt, Kill (but need an exhaustive
	// list.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		syscall.SIGTERM, // ^C
		syscall.SIGINT,  // kill
		syscall.SIGQUIT, // QUIT
		syscall.SIGABRT)
	cleanupDone := make(chan bool)
	tcLogCtxt.WithFields(log.Fields{
		"file":     "telemetryCollector.go",
		"function": "run",
	}).Info("Telemetry Collector watching for shutdown...")
	go func() {
		for range signalChan {
			tcLogCtxt.WithFields(log.Fields{
				"file":     "telemetryCollector.go",
				"function": "run",
			}).Info("Interrupt, stopping gracefully...")

			for name, entity := range tc.inputEntity {
				tcLogCtxt.WithFields(log.Fields{
					"file":     "telemetryCollector.go",
					"function": "run",
					"name":     name,
				}).Info("Stopping entity...")
				respChan := make(chan *cMsg)
				request := &cMsg{
					id:       shutdown,
					respChan: respChan,
				}
				//
				// Send shutdown message
				entity.cChan <- request
				// Wait for ACK
				<-respChan
				close(entity.cChan)
			}
			// Output to do

			// Now that they are all done. Unblock
			cleanupDone <- true
		}
	}()
	//
	// Block here waiting for cleanup. This is likely to be in a
	// main select along other possible conditions (like a timeout
	// to update stats?)
	<-cleanupDone

	tcLogCtxt.WithFields(log.Fields{
		"file":     "telemetryCollector.go",
		"function": "run",
	}).Info(" Bye...")
}

func main() {

	// Log as default ASCII formatter.
	tcLog.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	// Only log the info severity or above.
	tcLog.SetLevel(log.InfoLevel)

	activeOutputCapability := map[string](func() outputCapability){
		"tap": tapOutputCapabilityNew,
	}

	// initializes the active input capabilities the telemetry
	// collector supports
	activeInputCapability := map[string](func() inputCapability){
		"grpc": grpcInputCapabilityNew,
	}

	// initial init of the telemtry collector
	tc = tcInfo{
		configFile:         "telemetryCollector.conf",
		outputCapabilities: activeOutputCapability,
		inputCapabilities:  activeInputCapability,
		inputEntity:        make(map[string]*inputEntity),
	}

	// loading/initializing the telemetry collector from the configuration file
	err = loadingConfig()
	if err == nil {
		tcLogCtxt.WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "main",
		}).Info("Loading configuration succeeded")
		fmt.Printf("Wait for ^C to shutdown\n")
		run()
		os.Exit(0)
	} else {
		tcLogCtxt.WithError(err).WithFields(log.Fields{
			"file":     "telemetryCollector.go",
			"function": "main",
		}).Error("Loading configuration failed")
		os.Exit(-1)
	}

}
