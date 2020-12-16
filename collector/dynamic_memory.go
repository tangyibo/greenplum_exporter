package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  实时动态内存抓取器
 */

const (
	//?????????????????????????????????????
	dynamicMemorySql = `select hostname, dynamic_memory_used_mb, dynamic_memory_available_mb from  memory_info order by 1 DESC limit 1;`
)

var (
	dynamicMemUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "dynamic_memory_used_mb"),
		"The amount of dynamic memory in MB allocated to query processes running on this segment host",
		[]string{"hostname"}, nil,
	)

	dynamicMemAvailableDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "dynamic_memory_available_mb"),
		"The amount of additional dynamic memory (in MB) available to the query processes running on this segment host",
		[]string{"hostname"}, nil,
	)
)

func NewDynamicMemoryScraper() Scraper {
	return &dynamicMemoryScraper{}
}

type dynamicMemoryScraper struct{}

func (dynamicMemoryScraper) Name() string {
	return "dynamic_mem_scraper"
}

func (dynamicMemoryScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	rows, err := db.Query(dynamicMemorySql)
	logger.Infof("Query Database: %s",dynamicMemorySql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)
	for rows.Next() {
		var hostname string
		var used, available float64
		err = rows.Scan(&hostname, &used, &available)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(dynamicMemUsedDesc, prometheus.GaugeValue, used, hostname)
		ch <- prometheus.MustNewConstMetric(dynamicMemAvailableDesc, prometheus.GaugeValue, available, hostname)
	}

	return combineErr(errs...)
}
