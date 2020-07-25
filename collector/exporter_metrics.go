package collector

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace         = "greenplum"
	subSystemServer   = "server"
	subsystemExporter = "exporter"
	subSystemCluster  = "cluster"
	subSystemNode     = "node"
)

// 定义指标类型结构体
type ExporterMetrics struct {
	totalScraped   prometheus.Counter
	totalError     prometheus.Counter
	scrapeDuration prometheus.Gauge
	greenPlumUp    prometheus.Gauge
}

/**
* 函数：NewMetrics
* 功能：指标的生成工厂方法
 */
func NewMetrics() *ExporterMetrics {
	return &ExporterMetrics{
		totalScraped: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystemExporter,
				Name:      "total_scraped",
				Help:      "Total scraped",
			},
		),
		totalError: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystemExporter,
				Name:      "total_error",
				Help:      "Total error scraping",
			},
		),
		scrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystemExporter,
				Name:      "scrape_duration_second",
				Help:      "Elapsed of each scrape",
			},
		),
		greenPlumUp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "up",
				Help:      "Whether greenPlum cluster is reachable",
			},
		),
	}
}
