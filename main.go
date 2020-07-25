package main

import (
	"greenplum-exporter/collector"

	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/http"
)

/**
 * 参考教程：https://www.cnblogs.com/momoyan/p/9943268.html
 * 官方文档：https://godoc.org/github.com/prometheus/client_golang/prometheus
 * 官方文档：https://gp-docs-cn.github.io/docs/admin_guide/monitoring/monitoring.html
 */

var (
	listenAddress         = kingpin.Flag("web.listen-address", "web endpoint").Default("0.0.0.0:9297").String()
	metricPath            = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
	disableDefaultMetrics = kingpin.Flag("disableDefaultMetrics", "do not report default metrics(go metrics and process metrics)").Default("true").Bool()
)

var scrapers = map[collector.Scraper]bool{
	collector.NewLocksScraper():         true,
	collector.NewClusterStateScraper():  true,
	collector.NewDatabaseSizeScraper():  true,
	collector.NewConnectionsScraper():   true,
	collector.NewMaxConnScraper():       true,
	collector.NewSegmentScraper():       true,
	collector.NewConnDetailScraper():    true,
	collector.NewUsersScraper():         true,
	collector.NewBgWriterStateScraper(): true,

	collector.NewSystemScraper():        false,
	collector.NewQueryScraper():         false,
	collector.NewDynamicMemoryScraper(): false,
	collector.NewDiskScraper():          false,
}

var gathers prometheus.Gatherers

func main() {
	kingpin.Version("1.0.0")
	kingpin.HelpFlag.Short('h')

	logger.AddFlags(kingpin.CommandLine)
	kingpin.Parse()

	metricsHandleFunc := newHandler(*disableDefaultMetrics, scrapers)

	mux := http.NewServeMux()

	mux.HandleFunc(*metricPath, metricsHandleFunc)

	logger.Warnf("Greenplum exporter is starting and will listening on : %s", *listenAddress)

	logger.Error(http.ListenAndServe(*listenAddress, mux).Error())
}

func newHandler(disableDefaultMetrics bool, scrapers map[collector.Scraper]bool) http.HandlerFunc {

	registry := prometheus.NewRegistry()

	enabledScrapers := make([]collector.Scraper, 0, 16)

	for scraper, enable := range scrapers {
		if enable {
			enabledScrapers = append(enabledScrapers, scraper)
		}
	}

	greenPlumCollector := collector.NewCollector(enabledScrapers)

	registry.MustRegister(greenPlumCollector)

	if disableDefaultMetrics {
		gathers = prometheus.Gatherers{registry}
	} else {
		gathers = prometheus.Gatherers{registry, prometheus.DefaultGatherer}
	}

	handler := promhttp.HandlerFor(gathers, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})

	return handler.ServeHTTP
}
