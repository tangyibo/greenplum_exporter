package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  用户信息抓取器
 */

const (
	usersSql = `SELECT usename from pg_catalog.pg_user;`
)

var (
	usersCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "users_total_count"),
		"Total user account number for current greenplum database",
		nil,
		nil,
	)

	usersNameDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "users_name_list"),
		"Each user account name for current greenplum database",
		[]string{"username"},
		nil,
	)
)

func NewUsersScraper() Scraper {
	return usersScraper{}
}

type usersScraper struct{}

func (usersScraper) Name() string {
	return "users_scraper"
}

func (usersScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	rows, err := db.Query(usersSql)
	logger.Infof("Query Database: %s", usersSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	count := 1
	for rows.Next() {
		var username string

		err := rows.Scan(&username)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		count++
		ch <- prometheus.MustNewConstMetric(usersNameDesc, prometheus.GaugeValue, 1, username)
	}

	ch <- prometheus.MustNewConstMetric(usersCountDesc, prometheus.GaugeValue, float64(count))

	return combineErr(errs...)
}
