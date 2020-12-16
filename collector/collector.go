package collector

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"greenplum-exporter/stopwatch"
	logger "github.com/prometheus/common/log"
	"os"
	"sync"
	"time"
)

const verMajorSql=`select (select regexp_matches((select (select regexp_matches((select version()), 'Greenplum Database \d{1,}\.\d{1,}\.\d{1,}'))[1] as version), '\d{1,}'))[1];`

// 定义采集器数据类型结构体
type GreenPlumCollector struct {
	mu sync.Mutex

	db       *sql.DB
	ver       int
	metrics  *ExporterMetrics
	scrapers []Scraper
}

/**
* 函数：NewCollector
* 功能：采集器的生成工厂方法
 */
func NewCollector(enabledScrapers []Scraper) *GreenPlumCollector {
	return &GreenPlumCollector{
		metrics:  NewMetrics(),
		scrapers: enabledScrapers,
	}
}

/**
* 接口：Collect
* 功能：抓取最新的数据，传递给channel
 */
func (c *GreenPlumCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.scrape(ch)

	ch <- c.metrics.totalScraped
	ch <- c.metrics.totalError
	ch <- c.metrics.scrapeDuration
	ch <- c.metrics.greenPlumUp
}

/**
* 接口：Describe
* 功能：传递结构体中的指标描述符到channel
 */
func (c *GreenPlumCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.metrics.greenPlumUp.Desc()
	ch <- c.metrics.scrapeDuration.Desc()
	ch <- c.metrics.totalScraped.Desc()
	ch <- c.metrics.totalError.Desc()
}

/**
* 函数：scrape
* 功能：执行实际的数据抓取
 */
func (c *GreenPlumCollector) scrape(ch chan<- prometheus.Metric) {
	start := time.Now()
	watch := stopwatch.New("scrape")

	// 检查并与Greenplum建立连接
	c.metrics.totalScraped.Inc()
	watch.MustStart("check connections")
	err := c.checkGreenPlumConn()
	watch.MustStop()
	if err != nil {
		c.metrics.totalError.Inc()
		c.metrics.scrapeDuration.Set(time.Since(start).Seconds())
		c.metrics.greenPlumUp.Set(0)

		logger.Errorf("check database connection failed, error:%v", err)

		return
	}

	defer c.db.Close()

	logger.Info("check connections ok!")
	c.metrics.greenPlumUp.Set(1)

	// 遍历执行MAP中的所有抓取器
	for _, scraper := range c.scrapers {
		logger.Info("#### scraping start : " + scraper.Name())
		watch.MustStart("scraping: " + scraper.Name())
		err := scraper.Scrape(c.db, ch, c.ver)
		watch.MustStop()
		if err != nil {
			logger.Errorf("get metrics for scraper:%s failed, error:%v", scraper.Name(), err.Error())
		}
		logger.Info("#### scraping end : " + scraper.Name())
	}

	c.metrics.scrapeDuration.Set(time.Since(start).Seconds())

	logger.Info(fmt.Sprintf("prometheus scraped grennplum exporter successfully at %v, detail elapsed:%s", time.Now(), watch.PrettyPrint()))
}

/**
* 函数：checkGreenPlumConn
* 功能：检查Greenplum数据库的连接
 */
func (c *GreenPlumCollector) checkGreenPlumConn() (err error) {
	if c.db == nil {
		return c.getGreenPlumConnection()
	}

	if err = c.getGreenplumMajorVersion(c.db); err == nil {
		return nil
	} else {
		_ = c.db.Close()
		c.db = nil
		return c.getGreenPlumConnection()
	}
}

/**
* 函数：getGreenPlumConnection
* 功能：获取Greenplum数据库的连接
 */
func (c *GreenPlumCollector) getGreenPlumConnection() error {
	//使用PostgreSQL的驱动连接数据库，可参考如下教程：
	//参考：https://blog.csdn.net/u010412301/article/details/85037685
	dataSourceName := os.Getenv("GPDB_DATA_SOURCE_URL")

	db, err := sql.Open("postgres", dataSourceName)

	if err != nil {
		return err
	}

	if err = c.getGreenplumMajorVersion(db); err != nil {
		_ = db.Close()
		return err
	}

	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	c.db = db

	return nil
}

/**
* 函数：getGreenplumMajorVersion
* 功能：获取Greenplum数据库的主版本号
 */
func (c *GreenPlumCollector) getGreenplumMajorVersion(db *sql.DB) error {
	err := db.Ping()

	if err != nil {
		return err
	}

	rows, err := db.Query(verMajorSql)

	if err != nil {
		return err
	}

	for rows.Next() {
		var verMajor int
		errC := rows.Scan(&verMajor)
		if errC != nil {
			return errC
		}

		c.ver=verMajor
	}

	defer rows.Close()

	return nil
}
