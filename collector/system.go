package collector

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	logger "github.com/prometheus/common/log"
)

/**
 *  系统信息抓取器
 */

const (
	//?????????????????????????????????????
	systemMetricsSql = `select * from system_now;`
)

var (
	memTotalDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "mem_total_bytes"),
		"Segment or master hostname associated with these system metrics",
		[]string{"hostname"}, nil,
	)

	memUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "mem_used_bytes"),
		"Total system memory in Bytes for this host",
		[]string{"hostname"}, nil,
	)

	memActualUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "mem_actual_used_bytes"),
		"Used actual memory in Bytes for this host",
		[]string{"hostname"}, nil,
	)

	memActualFreeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "mem_actual_free_bytes"),
		"Free actual memory in Bytes for this host",
		[]string{"hostname"}, nil,
	)

	swapTotalDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "swap_total_bytes"),
		"Total swap space in Bytes for this host",
		[]string{"hostname"}, nil,
	)

	swapUsedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "swap_used_bytes"),
		"Used swap space in Bytes for this host",
		[]string{"hostname"}, nil,
	)

	swapPageInDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "swap_page_in"),
		"Number of swap pages in",
		[]string{"hostname"}, nil,
	)

	swapPageOutDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "swap_page_out"),
		"Number of swap pages out",
		[]string{"hostname"}, nil,
	)

	cpuUserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "cpu_user_percent"),
		"CPU usage by the Greenplum system user",
		[]string{"hostname"}, nil,
	)

	cpuSysDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "cpu_sys_percent"),
		"CPU usage for this host",
		[]string{"hostname"}, nil,
	)

	cpuIdleDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "cpu_idle_percent"),
		"Idle CPU capacity at metric collection time",
		[]string{"hostname"}, nil,
	)

	cpuAvg1mDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "cpu_avg_usage_1m_percent"),
		"CPU load average for the prior one-minute period",
		[]string{"hostname"}, nil,
	)

	cpuAvg5mDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "cpu_avg_usage_5m_percent"),
		"CPU load average for the prior five-minutes period",
		[]string{"hostname"}, nil,
	)

	cpuAvg15mDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "cpu_avg_usage_15m_percent"),
		"CPU load average for the prior fifteen-minutes period",
		[]string{"hostname"}, nil,
	)

	diskRoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "disk_ro_rate"),
		"Disk read operations per second",
		[]string{"hostname"}, nil,
	)

	diskWoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "disk_wo_rate"),
		"Disk write operations per second",
		[]string{"hostname"}, nil,
	)

	diskRbDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "disk_rb_rate"),
		"Bytes per second for disk read operations",
		[]string{"hostname"}, nil,
	)

	diskWbDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "disk_wb_rate"),
		"Bytes per second for disk write operations",
		[]string{"hostname"}, nil,
	)

	netRpDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "net_rp_rate"),
		"Packets per second on the system network for read operations",
		[]string{"hostname"}, nil,
	)

	netWpDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "net_wp_rate"),
		"Packets per second on the system network for write operations",
		[]string{"hostname"}, nil,
	)

	netRbDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "net_rb_rate"),
		"Bytes per second on the system network for read operations",
		[]string{"hostname"}, nil,
	)

	netWbDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subSystemNode, "net_wb_rate"),
		"Bytes per second on the system network for write operations",
		[]string{"hostname"}, nil,
	)
)

func NewSystemScraper() Scraper {
	return systemScraper{}
}

type systemScraper struct{}

func (systemScraper) Name() string {
	return "systemScraper"
}

func (systemScraper) Scrape(db *sql.DB, ch chan<- prometheus.Metric, ver int) error {
	rows, err := db.Query(systemMetricsSql)
	logger.Infof("Query Database: %s",systemMetricsSql)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var cTime, hostname string
		var memTotal, memUsed, memActualUsed, memActualFree, swapTotal, swapUsed, swapPageIn, swapPageOut,
			cpuUser, cpuSys, cpuIdle, load1m, load5m, load15m, quantum, diskRo, diskWo, diskRb, diskWb, netRp, netWp, netRb, netWb float64

		err = rows.Scan(&cTime, &hostname, &memTotal, &memUsed, &memActualUsed, &memActualFree, &swapTotal,
			&swapUsed, &swapPageIn, &swapPageOut, &cpuUser, &cpuSys, &cpuIdle, &load1m, &load5m, &load15m,
			&quantum, &diskRo, &diskWo, &diskRb, &diskWb, &netRp, &netWp, &netRb, &netWb)

		if err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(memTotalDesc, prometheus.GaugeValue, memTotal, hostname)
		ch <- prometheus.MustNewConstMetric(memUsedDesc, prometheus.GaugeValue, memUsed, hostname)
		ch <- prometheus.MustNewConstMetric(memActualUsedDesc, prometheus.GaugeValue, memActualUsed, hostname)
		ch <- prometheus.MustNewConstMetric(memActualFreeDesc, prometheus.GaugeValue, memActualFree, hostname)
		ch <- prometheus.MustNewConstMetric(swapTotalDesc, prometheus.GaugeValue, swapTotal, hostname)
		ch <- prometheus.MustNewConstMetric(swapUsedDesc, prometheus.GaugeValue, swapUsed, hostname)
		ch <- prometheus.MustNewConstMetric(swapPageInDesc, prometheus.GaugeValue, swapPageIn, hostname)
		ch <- prometheus.MustNewConstMetric(swapPageOutDesc, prometheus.GaugeValue, swapPageOut, hostname)
		ch <- prometheus.MustNewConstMetric(cpuUserDesc, prometheus.GaugeValue, cpuUser, hostname)
		ch <- prometheus.MustNewConstMetric(cpuSysDesc, prometheus.GaugeValue, cpuSys, hostname)
		ch <- prometheus.MustNewConstMetric(cpuIdleDesc, prometheus.GaugeValue, cpuIdle, hostname)
		ch <- prometheus.MustNewConstMetric(cpuAvg1mDesc, prometheus.GaugeValue, load1m, hostname)
		ch <- prometheus.MustNewConstMetric(cpuAvg5mDesc, prometheus.GaugeValue, load5m, hostname)
		ch <- prometheus.MustNewConstMetric(cpuAvg15mDesc, prometheus.GaugeValue, load15m, hostname)
		ch <- prometheus.MustNewConstMetric(diskRoDesc, prometheus.GaugeValue, diskRo, hostname)
		ch <- prometheus.MustNewConstMetric(diskWoDesc, prometheus.GaugeValue, diskWo, hostname)
		ch <- prometheus.MustNewConstMetric(diskRbDesc, prometheus.GaugeValue, diskRb, hostname)
		ch <- prometheus.MustNewConstMetric(diskWbDesc, prometheus.GaugeValue, diskWb, hostname)
		ch <- prometheus.MustNewConstMetric(netRpDesc, prometheus.GaugeValue, netRp, hostname)
		ch <- prometheus.MustNewConstMetric(netWpDesc, prometheus.GaugeValue, netWp, hostname)
		ch <- prometheus.MustNewConstMetric(netRbDesc, prometheus.GaugeValue, netRb, hostname)
		ch <- prometheus.MustNewConstMetric(netWbDesc, prometheus.GaugeValue, netWb, hostname)
	}

	return nil
}
