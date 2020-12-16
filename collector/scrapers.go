package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
)

// 抓取器Scraper接口定义
// 实现包括：
//  clusterStateScraper、connectionsScraper、connectionsDetailScraper、diskScraper、
//  dynamicMemoryScraper、maxConnScraper、queriesScraper、segmentScraper、systemScraper
type Scraper interface {

	// Scraper的名称. 需要唯一.
	Name() string

	// 从数据库连接中获取数据信息，并发送到数据类型为prometheus metric的通道里.
	Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error
}
