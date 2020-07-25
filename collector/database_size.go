package collector

import (
	"container/list"
	"context"
	"os"
	"strings"
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
	"time"
)

/**
 *  各个数据库存储大小抓取器
 */

const (
	databaseSizeSql = `SELECT sodddatname as database_name,sodddatsize/(1024*1024) as database_size_mb from gp_toolkit.gp_size_of_database;`
	tableCountSql   = `SELECT count(*) as total from information_schema.tables where table_schema not in ('gp_toolkit','information_schema','pg_catalog');`
	hitCacheRateSql = `select sum(blks_hit)/(sum(blks_read)+sum(blks_hit))*100 from pg_stat_database;`
	txCommitRateSql = `select sum(xact_commit)/(sum(xact_commit)+sum(xact_rollback))*100 from pg_stat_database;`
)

var (
	databaseSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "database_name_mb_size"), //指标的名称
		"Total MB size of each database name in the file system",                  //帮助信息，显示在指标的上面作为注释
		[]string{"dbname"},                                                        //定义的label名称数组
		nil,                                                                       //定义的Labels
	)

	tablesCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "database_table_total_count"),
		"Total table count of each database name in the file system",
		[]string{"dbname"},
		nil,
	)

	hitCacheRateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "database_hit_cache_percent_rate"),
		"Cache hit percent rat for all of database in greenplum server system",
		nil,
		nil,
	)

	txCommitRateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "database_transition_commit_percent_rate"),
		"Transition commit percent rat for all of database in greenplum server system",
		nil,
		nil,
	)
)

func NewDatabaseSizeScraper() Scraper {
	return databaseSizeScraper{}
}

type databaseSizeScraper struct{}

func (databaseSizeScraper) Name() string {
	return "database_size_scraper"
}

func (databaseSizeScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)

	defer cancel()

	logger.Infof("Query Database: %s", databaseSizeSql)
	rows, err := db.QueryContext(ctx, databaseSizeSql)
	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	names := list.New()
	for rows.Next() {
		var dbname string
		var mbSize float64

		err := rows.Scan(&dbname, &mbSize)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(databaseSizeDesc, prometheus.GaugeValue, mbSize, dbname)
		names.PushBack(dbname)
	}

	for item := names.Front(); nil != item; item = item.Next() {
		dbname := item.Value.(string)
		count, err := queryTablesCount(dbname)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(tablesCountDesc, prometheus.GaugeValue, count, dbname)
	}

	errM := queryHitCacheRate(db, ch)
	if errM != nil {
		errs = append(errs, errM)
	}

	errN := queryTxCommitRate(db, ch)
	if errN != nil {
		errs = append(errs, errN)
	}

	return combineErr(errs...)
}

func queryTablesCount(dbname string) (count float64, err error) {
	dataSourceName := os.Getenv("GPDB_DATA_SOURCE_URL")
	newDataSourceName := strings.Replace(dataSourceName, "/postgres", "/"+dbname, 1)
	logger.Infof("Connection string is : %s", newDataSourceName)
	conn, err := sql.Open("postgres", newDataSourceName)

	if err != nil {
		return
	}

	defer conn.Close()

	rows, err := conn.Query(tableCountSql)
	logger.Infof("Query Database: %s", tableCountSql)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&count)
		break
	}

	return
}

func queryHitCacheRate(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(hitCacheRateSql)
	logger.Infof("Query Database: %s", hitCacheRateSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var rate float64
		err = rows.Scan(&rate)

		ch <- prometheus.MustNewConstMetric(hitCacheRateDesc, prometheus.GaugeValue, rate)

		break
	}

	return nil
}

func queryTxCommitRate(db *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := db.Query(txCommitRateSql)
	logger.Infof("Query Database: %s", txCommitRateSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var rate float64
		err = rows.Scan(&rate)

		ch <- prometheus.MustNewConstMetric(txCommitRateDesc, prometheus.GaugeValue, rate)

		break
	}

	return nil
}
