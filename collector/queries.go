package collector

import (
	"database/sql"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  SQL查询信息抓取器
 */

const (
	//?????????????????????????????????????
	queriesSql = `select * from database_now;`
)

var (
	totalQueriesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "total_queries"),
		"The total number of queries in Greenplum Database at data collection time",
		nil, nil,
	)

	runningQueriesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "running_queries"),
		"The number of active queries running at data collection time",
		nil, nil,
	)

	queuedQueriesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "queued_queries"),
		"The number of queries waiting in a resource group or resource queue",
		nil, nil,
	)
)

func NewQueryScraper() Scraper {
	return queriesScraper{}
}

type queriesScraper struct{}

func (queriesScraper) Name() string {
	return "queriesScraper"
}

func (queriesScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	rows, err := db.Query(queriesSql)
	logger.Infof("Query Database: %s",queriesSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var cTime string
		var total, running, queued sql.NullFloat64
		err = rows.Scan(&cTime, &total, &running, &queued)

		if err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(totalQueriesDesc, prometheus.GaugeValue, total.Float64)
		ch <- prometheus.MustNewConstMetric(runningQueriesDesc, prometheus.GaugeValue, running.Float64)
		ch <- prometheus.MustNewConstMetric(queuedQueriesDesc, prometheus.GaugeValue, queued.Float64)

		return nil
	}

	return errors.New("queries info not found")
}
