package collector

import (
	"container/list"
	"context"
	"database/sql"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  各个数据库存储大小、数据膨胀列表、数据倾斜列表、缓存命中率、事务提交率等
 */

const (
	databaseSizeSql = `SELECT sodddatname as database_name,sodddatsize/(1024*1024) as database_size_mb from gp_toolkit.gp_size_of_database;`
	tableCountSql   = `SELECT count(*) as total from information_schema.tables where table_schema not in ('gp_toolkit','information_schema','pg_catalog');`
	bloatTableSql   = `
		SELECT current_database(),bdinspname,bdirelname,bdirelpages,bdiexppages,(
		case 
			when position('moderate' in bdidiag)>0 then 1 
			when position('significant' in bdidiag)>0 then 2 
			else 0 
		end) as bloat_state 
		FROM gp_toolkit.gp_bloat_diag ORDER BY bloat_state desc
	`
	skewTableSql = `
		SELECT current_database(),schema_name,table_name,max_div_avg,pg_size_pretty(total_size) table_size 
		FROM (
			SELECT schema_name,table_name,
				MAX(size)/(AVG(size)+0.001) AS max_div_avg,
				CAST(SUM(size) AS BIGINT) total_size
			FROM
				(
			SELECT o.gp_segment_id,
						n.nspname as schema_name,
						o.relname as table_name,
						pg_relation_size(o.oid) size
				FROM gp_dist_random('pg_class') o
					LEFT JOIN pg_namespace n on o.relnamespace=n.oid
				WHERE o.relkind='r'
				AND o.relstorage IN ('a','h')
			) t
			GROUP BY schema_name,table_name
			)tab 
		WHERE total_size >= 1024*1024*1024
		AND max_div_avg>1.5
		ORDER BY total_size DESC;
	`
	hitCacheRateSql = `select sum(blks_hit)/(sum(blks_read)+sum(blks_hit))*100 from pg_stat_database;`
	txCommitRateSql = `select sum(xact_commit)/(sum(xact_commit)+sum(xact_rollback))*100 from pg_stat_database;`
)

var (
	databaseSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "database_name_mb_size"), //指标的名称
		"Total MB size of each database name in the file system",                  //帮助信息，显示在指标的上面作为注释
		[]string{"dbname"}, //定义的label名称数组
		nil,                //定义的Labels
	)

	tablesCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "database_table_total_count"),
		"Total table count of each database name in the file system",
		[]string{"dbname"},
		nil,
	)

	bloatTableDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "database_table_bloat_list"),
		"Bloat table list of each database name in greenplum cluster",
		[]string{"dbname", "schema", "table", "relpages", "exppages"},
		nil,
	)

	skewTableDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemServer, "database_table_skew_list"),
		"Skew table list of each database name in greenplum cluster",
		[]string{"dbname", "schema", "table", "size"},
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

func (databaseSizeScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)

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
		count, err := queryTablesCount(dbname, ch)
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

func queryTablesCount(dbname string, ch chan<- prometheus.Metric) (count float64, err error) {
	dataSourceName := os.Getenv("GPDB_DATA_SOURCE_URL")
	newDataSourceName := strings.Replace(dataSourceName, "/postgres", "/"+dbname, 1)
	logger.Infof("Connection string is : %s", newDataSourceName)
	conn, errA := sql.Open("postgres", newDataSourceName)

	if errA != nil {
		err = errA
		return
	}

	defer conn.Close()

	rows, errB := conn.Query(tableCountSql)
	logger.Infof("Query Database: %s", tableCountSql)

	if errB != nil {
		err = errB
		return
	}

	defer rows.Close()

	for rows.Next() {
		errC := rows.Scan(&count)
		if errC != nil {
			err = errC
			return
		}
	}

	// errD := queryBloatTables(conn, ch)
	// if errD != nil {
	// 	err=errD
	// 	return
	// }

	// errF := querySkewTables(conn, ch)
	// if errF != nil {
	// 	err = errF
	// 	return
	// }

	return
}

func queryBloatTables(conn *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(bloatTableSql)
	logger.Infof("Query bloat tables sql: %s", bloatTableSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	for rows.Next() {
		var dbname, schema, table, relpages, exppages string
		var bloatstate float64
		err = rows.Scan(&dbname, &schema, &table, &relpages, &exppages, &bloatstate)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(bloatTableDesc, prometheus.GaugeValue, bloatstate, dbname, schema, table, relpages, exppages)
	}

	return combineErr(errs...)
}

func querySkewTables(conn *sql.DB, ch chan<- prometheus.Metric) error {
	rows, err := conn.Query(skewTableSql)
	logger.Infof("Query skew tables sql: %s", skewTableSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	for rows.Next() {
		var dbname, schema, table, size string
		var slope float64
		err = rows.Scan(&dbname, &schema, &table, &slope, &size)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(skewTableDesc, prometheus.GaugeValue, slope, dbname, schema, table, size)
	}

	return combineErr(errs...)
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
