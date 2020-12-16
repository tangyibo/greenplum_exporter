package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  存储磁盘抓取器
 */

const (
	//?????????????????????????????????????
	fileSystemSql = `select * from diskspace_now;`
)

var (
	fsTotalDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "fs_total_bytes"),
		"Total bytes in the file system",
		[]string{"hostname", "filesystem"}, nil,
	)

	fsUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "fs_used_bytes"),
		"Total bytes used in the file system",
		[]string{"hostname", "filesystem"}, nil,
	)

	fsAvailableDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "fs_available_bytes"),
		"Total bytes available in the file system",
		[]string{"hostname", "filesystem"}, nil,
	)
)

func NewDiskScraper() Scraper {
	return diskScraper{}
}

type diskScraper struct{}

func (diskScraper) Name() string {
	return "filesystem_scraper"
}

func (diskScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	rows, err := db.Query(fileSystemSql)
	logger.Infof("Query Database: %s",fileSystemSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	errs := make([]error, 0)

	for rows.Next() {
		var cTime, hostname, fs string
		var total, used, available float64

		err := rows.Scan(&cTime, &hostname, &fs, &total, &used, &available)

		if err != nil {
			errs = append(errs, err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(fsTotalDesc, prometheus.GaugeValue, total, hostname, fs)
		ch <- prometheus.MustNewConstMetric(fsUsedDesc, prometheus.GaugeValue, used, hostname, fs)
		ch <- prometheus.MustNewConstMetric(fsAvailableDesc, prometheus.GaugeValue, available, hostname, fs)
	}

	return combineErr(errs...)
}
