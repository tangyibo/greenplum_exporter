package collector

import (
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 * 数据库锁信息抓取器
 */

const (
	locksQuerySql_V6 = ` 
		SELECT pg_locks.pid
			 , pg_database.datname
			 , pg_stat_activity.usename
			 , locktype
			 , mode
			 , pg_stat_activity.application_name
			 , state
			 , CASE
						WHEN granted='f' THEN
							'wait_lock'
						WHEN granted='t' THEN
							'get_lock'
					END lock_satus
			 , pg_stat_activity.query
			 , least(query_start,xact_start) start_time
			 , count(*)::float
		  FROM pg_locks
		  JOIN pg_database ON pg_locks.database=pg_database.oid
		  JOIN pg_stat_activity on pg_locks.pid=pg_stat_activity.pid
		WHERE NOT pg_locks.pid=pg_backend_pid()
		AND pg_stat_activity.application_name<>'pg_statsinfod'
		GROUP BY pg_locks.pid, pg_database.datname,pg_stat_activity.usename, locktype, mode,
		pg_stat_activity.application_name, state , lock_satus ,pg_stat_activity.query, start_time
		ORDER BY start_time
		`
	locksQuerySql_V5 = ` 
		SELECT pg_locks.pid
			 , pg_database.datname
			 , pg_stat_activity.usename
			 , locktype
			 , mode
			 , pg_stat_activity.application_name
			 , 'unkown' as state
			 , CASE
						WHEN granted='f' THEN
							'wait_lock'
						WHEN granted='t' THEN
							'get_lock'
					END lock_satus
			 , pg_stat_activity.current_query
			 , least(query_start,xact_start) start_time
			 , count(*)::float
		  FROM pg_locks
		  JOIN pg_database ON pg_locks.database=pg_database.oid
		  JOIN pg_stat_activity on pg_locks.pid=pg_stat_activity.procpid
		WHERE NOT pg_locks.pid=pg_backend_pid()
		AND pg_stat_activity.application_name<>'pg_statsinfod'
		GROUP BY pg_locks.pid, pg_database.datname,pg_stat_activity.usename, locktype, mode,
		pg_stat_activity.application_name, state , lock_satus ,pg_stat_activity.current_query, start_time
		ORDER BY start_time
		`
)

var (
	locksDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "locks_table_detail"),
		"Table locks detail for greenplum database",
		[]string{"pid", "datname", "usename", "locktype", "mode", "application_name", "state", "lock_satus", "query"},
		nil,
	)
)

func NewLocksScraper() Scraper {
	return &locksScraper{}
}

type locksScraper struct{}

func (locksScraper) Name() string {
	return "locks_scraper"
}

func (locksScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	querySql := locksQuerySql_V6
	if ver < 6 {
		querySql = locksQuerySql_V5
	}

	rows, err := db.Query(querySql)
	logger.Infof("Query Database: %s", querySql)

	if err != nil {
		logger.Errorf("get metrics for scraper, error:%v", err.Error())
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var pid, datname, usename, locktype, mode, application_name, state, lock_satus, query string
		var startTime time.Time
		var count int64

		err = rows.Scan(&pid,
			&datname,
			&usename,
			&locktype,
			&mode,
			&application_name,
			&state,
			&lock_satus,
			&query,
			&startTime,
			&count)
		if err != nil {
			logger.Errorf("get metrics for scraper, error:%v", err.Error())
			return err
		}

		ch <- prometheus.MustNewConstMetric(locksDesc, prometheus.GaugeValue, float64(startTime.UTC().Unix()), pid, datname, usename, locktype, mode, application_name, state, lock_satus, query)
	}

	return nil
}
