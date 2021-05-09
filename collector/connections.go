package collector

import (
	"database/sql"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  连接数量抓取器
 */

const (
	connectionsSql_V6 = `select 
                         count(*) total, 
                         count(*) filter(where state<>'active') idle, 
                         count(*) filter(where state='active') active,
                         count(*) filter(where state='active' and not waiting) running,
                         count(*) filter(where state='active' and waiting) waiting
						 from pg_stat_activity where pid <> pg_backend_pid();`
	connectionsSql_V5 = `select 
                         count(*) total, 
                         count(*) filter(where current_query='<IDLE>') idle, 
                         count(*) filter(where current_query<>'<IDLE>') active,
                         count(*) filter(where current_query<>'<IDLE>' and not waiting) running,
                         count(*) filter(where current_query<>'<IDLE>' and waiting) waiting
                         from pg_stat_activity where procpid <> pg_backend_pid();`
)

var (
	currentConnDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "total_connections"),
		"Current connections of GreenPlum cluster at scrape time",
		nil, nil,
	)

	idleConnDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "idle_connections"),
		"Idle connections of GreenPlum cluster at scape time",
		nil, nil,
	)

	activeConnDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "active_connections"),
		"Active connections of GreenPlum cluster at scape time",
		nil, nil,
	)

	runningConnDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "running_connections"),
		"Running sql count of GreenPlum cluster at scape time",
		nil, nil,
	)

	queuingConnDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "waiting_connections"),
		"Waiting sql count of GreenPlum cluster at scape time",
		nil, nil,
	)
)

func NewConnectionsScraper() Scraper {
	return &connectionsScraper{}
}

type connectionsScraper struct{}

func (connectionsScraper) Name() string {
	return "connections_scraper"
}

func (connectionsScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	querySql := connectionsSql_V6
	if ver < 6 {
		querySql = connectionsSql_V5
	}

	rows, err := db.Query(querySql)
	logger.Infof("Query Database: %s", querySql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var total, idle, active, running, waiting float64

		err = rows.Scan(&total, &idle, &active, &running, &waiting)

		if err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(currentConnDesc, prometheus.GaugeValue, total)
		ch <- prometheus.MustNewConstMetric(idleConnDesc, prometheus.GaugeValue, idle)
		ch <- prometheus.MustNewConstMetric(activeConnDesc, prometheus.GaugeValue, active)
		ch <- prometheus.MustNewConstMetric(runningConnDesc, prometheus.GaugeValue, running)
		ch <- prometheus.MustNewConstMetric(queuingConnDesc, prometheus.GaugeValue, waiting)

		return nil
	}

	return errors.New("connections not found")
}
