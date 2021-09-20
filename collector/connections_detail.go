package collector

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  连接详情抓取器
 */

const (
	connectionsByUserSql_V6 = `select usename, 
                                      count(*) total, 
                                      count(*) filter(where state<>'active') idle, 
                                      count(*) filter(where state='active') active 
							   from pg_stat_activity group by 1;`
	connectionsByUserSql_V5 = `select usename, 
                                      count(*) total, 
                                      count(*) filter(where current_query='<IDLE>') idle, 
                                      count(*) filter(where current_query<>'<IDLE>') active 
                               from pg_stat_activity group by 1;`
	connectionsByClientAddressSql_V6 = `select client_addr,
                                        count(*) total,
                                        count(*) filter(where state<>'active') idle,
                                        count(*) filter(where state='active') active
								from pg_stat_activity where pid <> pg_backend_pid() group by 1;`
	connectionsByClientAddressSql_V5 = `select client_addr,
                                               count(*) total,
                                               count(*) filter(where current_query='<IDLE>') idle,
                                               count(*) filter(where current_query<>'<IDLE>') active
                                from pg_stat_activity where procpid <> pg_backend_pid() group by 1;`
)

var (
	totalPerUserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "total_connections_per_user"),
		"Total connections of specified database user",
		[]string{"usename"}, nil,
	)

	activePerUserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "active_connections_per_user"),
		"Active connections of specified database user",
		[]string{"usename"}, nil,
	)

	idlePerUserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "idle_connections_per_user"),
		"Idle connections of specified database user",
		[]string{"usename"}, nil,
	)

	totalPerClientDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "total_connections_per_client"),
		"Total connections of specified database user",
		[]string{"client"}, nil,
	)

	activePerClientDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "active_connections_per_client"),
		"Active connections of specified database user",
		[]string{"client"}, nil,
	)

	idlePerClientDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "idle_connections_per_client"),
		"Idle connections of specified database user",
		[]string{"client"}, nil,
	)

	totalCountClientDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "total_client_count"),
		"The total client count of greenplum database",
		nil, nil,
	)

	totalCountOnlineUsersDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemCluster, "total_online_user_count"),
		"The total online user count of greenplum database",
		nil, nil,
	)
)

func NewConnDetailScraper() Scraper {
	return connectionsDetailScraper{}
}

type connectionsDetailScraper struct{}

func (connectionsDetailScraper) Name() string {
	return "connections_detail_scraper"
}

func (connectionsDetailScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	errU := scrapeLoadByUser(db, ch, ver)
	errC := scrapeLoadByClient(db, ch, ver)

	return combineErr(errC, errU)
}

func scrapeLoadByUser(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	querySql := connectionsByUserSql_V6
	if ver < 6 {
		querySql = connectionsByUserSql_V5
	}

	rows, err := db.Query(querySql)

	logger.Infof("Query Database: %s", querySql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	var totalOnlineUserCount int = 0
	for rows.Next() {
		var usename string
		var total, idle, active float64

		err = rows.Scan(&usename, &total, &idle, &active)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(totalPerUserDesc, prometheus.GaugeValue, total, usename)
		ch <- prometheus.MustNewConstMetric(idlePerUserDesc, prometheus.GaugeValue, idle, usename)
		ch <- prometheus.MustNewConstMetric(activePerUserDesc, prometheus.GaugeValue, active, usename)

		totalOnlineUserCount++
	}

	ch <- prometheus.MustNewConstMetric(totalCountOnlineUsersDesc, prometheus.GaugeValue, float64(totalOnlineUserCount))

	return combineErr(errs...)
}

func scrapeLoadByClient(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	querySql := connectionsByClientAddressSql_V6
	if ver < 6 {
		querySql = connectionsByClientAddressSql_V5
	}

	rows, err := db.Query(querySql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	var totalClientCount int = 0
	for rows.Next() {
		var client sql.NullString
		var total, idle, active float64

		err = rows.Scan(&client, &total, &idle, &active)

		if err != nil {
			errs = append(errs, err)
		}

		ch <- prometheus.MustNewConstMetric(totalPerClientDesc, prometheus.GaugeValue, total, client.String)
		ch <- prometheus.MustNewConstMetric(idlePerClientDesc, prometheus.GaugeValue, idle, client.String)
		ch <- prometheus.MustNewConstMetric(activePerClientDesc, prometheus.GaugeValue, active, client.String)

		totalClientCount++
	}

	ch <- prometheus.MustNewConstMetric(totalCountClientDesc, prometheus.GaugeValue, float64(totalClientCount))

	return combineErr(errs...)
}
