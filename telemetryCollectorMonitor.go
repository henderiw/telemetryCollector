package main

import (
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

// tcMonitor type hold the struct for prometheus monitor descriptions
type tcMonitor struct {
	NumGoroutine  *prometheus.Desc
	NumCPU        *prometheus.Desc
	MemAlloc      *prometheus.Desc
	MemTotalAlloc *prometheus.Desc
	MemMallocs    *prometheus.Desc
	MemFrees      *prometheus.Desc
	MemNumGC      *prometheus.Desc
	UpTime        *prometheus.Desc
	InEntity      *prometheus.Desc
	OutEntity     *prometheus.Desc
	ChanOccupancy *prometheus.Desc
}

// Describe sends the Descriptor objects for the Metrics we intend to collect.
func (tcm tcMonitor) Describe(ch chan<- *prometheus.Desc) {
	ch <- tcm.NumGoroutine
	ch <- tcm.NumCPU
	ch <- tcm.MemAlloc
	ch <- tcm.MemTotalAlloc
	ch <- tcm.MemMallocs
	ch <- tcm.MemFrees
	ch <- tcm.MemNumGC
	ch <- tcm.UpTime
	ch <- tcm.InEntity
	ch <- tcm.OutEntity
	ch <- tcm.ChanOccupancy
}

// If monitoring is setup, the function will be called periodically
// to collect and report on the telemetryCollector state
func (tcm tcMonitor) Collect(ch chan<- prometheus.Metric) {

	// Collect memory statistics
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	ch <- prometheus.MustNewConstMetric(
		tcm.NumGoroutine,
		prometheus.GaugeValue,
		float64(runtime.NumGoroutine()),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.NumCPU,
		prometheus.GaugeValue,
		float64(runtime.NumCPU()),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.MemAlloc,
		prometheus.GaugeValue,
		float64(ms.Alloc),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.MemTotalAlloc,
		prometheus.GaugeValue,
		float64(ms.TotalAlloc),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.MemMallocs,
		prometheus.GaugeValue,
		float64(ms.Mallocs),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.MemFrees,
		prometheus.GaugeValue,
		float64(ms.Frees),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.MemNumGC,
		prometheus.CounterValue,
		float64(ms.NumGC),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.UpTime,
		prometheus.CounterValue,
		float64(time.Since(tc.Started)))

	ch <- prometheus.MustNewConstMetric(
		tcm.InEntity,
		prometheus.GaugeValue,
		float64(len(tc.inputEntity)),
	)

	ch <- prometheus.MustNewConstMetric(
		tcm.OutEntity,
		prometheus.GaugeValue,
		float64(len(tc.outputEntity)),
	)

	for entityname, entity := range tc.outputEntity {
		ch <- prometheus.MustNewConstMetric(
			tcm.ChanOccupancy,
			prometheus.GaugeValue,
			float64(100*len(entity.dataChan)/cap(entity.dataChan)),
			entityname)
	}

}

func tcMonitorInit() {
	// Initialise TC monitoring and setup TC monitor
	tc.monitor = &tcMonitor{
		NumGoroutine: prometheus.NewDesc(
			"goroutines",
			"currently active goroutines",
			nil, nil),
		NumCPU: prometheus.NewDesc(
			"cpus",
			"available CPUs for pipeline",
			nil, nil),
		MemAlloc: prometheus.NewDesc(
			"alloc",
			"currently allocated memory",
			nil, nil),
		MemTotalAlloc: prometheus.NewDesc(
			"totalalloc",
			"allocated memory, including historic allocations",
			nil, nil),
		MemMallocs: prometheus.NewDesc(
			"mallocs",
			"Calls to malloc memory",
			nil, nil),
		MemFrees: prometheus.NewDesc(
			"frees",
			"Calls to free memory",
			nil, nil),
		MemNumGC: prometheus.NewDesc(
			"gcruns",
			"Garbage collection invocations",
			nil, nil),
		UpTime: prometheus.NewDesc(
			"uptime",
			"Time since pipeline started",
			nil, nil),
		InEntity: prometheus.NewDesc(
			"cfginputs",
			"Configured input sections",
			nil, nil),
		OutEntity: prometheus.NewDesc(
			"cfgoutputs",
			"Configured output sections",
			nil, nil),
		ChanOccupancy: prometheus.NewDesc(
			"chanoccupancy",
			"Channel occupancy",
			[]string{"section"}, nil),
	}
	prometheus.MustRegister(tc.monitor)

}

func tcMonitorStart(ec entityConfig) {
	path, err := ec.config.GetString("default", "prometheusResource")
	if err != nil {
		tcLogCtxt.Info("TC monitor: not configured")
		return
	}

	server, err := ec.config.GetString("default", "prometheusServer")
	if err != nil {
		server = ":8080"
	}

	tcLogCtxt.WithFields(log.Fields{
		"resource": path,
		"name":     "default",
		"server":   server,
	}).Info("TC monitor: serving TC metrics to prometheus")

	http.Handle(path, promhttp.Handler())
	err = http.ListenAndServe(server, nil)
	tcLogCtxt.WithError(err).Error("TC monitor: stop serving metrics")

}
