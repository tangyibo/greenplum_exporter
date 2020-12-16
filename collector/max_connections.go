package collector

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  最大连接抓取器
 */

const (
	maxConnectionsSql = `show max_connections`
	suReservedSql     = `show superuser_reserved_connections`
)

var (
	maxConnDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "max_connections"),
		"Max connection of greenPlum cluster",
		nil, nil,
	)
)

func NewMaxConnScraper() Scraper {
	return maxConnScraper{}
}

type maxConnScraper struct{}

func (maxConnScraper) Name() string {
	return "max_connection_scraper"
}

func (maxConnScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	maxConn, err := showConnections(db, maxConnectionsSql)

	if err != nil {
		return err
	}

	reserved, err := showConnections(db, suReservedSql)

	if err != nil {
		logger.Warn(err.Error())
	}

	//这里的最大连接数应为max_connections减去superuser_reserved_connections
	ch <- prometheus.MustNewConstMetric(maxConnDesc, prometheus.GaugeValue, maxConn-reserved)

	return nil
}

func showConnections(db *sql.DB, sql string) (conn float64, err error) {
	rows, err := db.Query(sql)
	logger.Infof("Query Database: %s",sql)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&conn)

		return
	}

	err = errors.New(fmt.Sprintf("%s not found", sql))
	return
}
