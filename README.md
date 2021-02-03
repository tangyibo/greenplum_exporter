# Greenplum-exporter

基于go语言为Greenplum集成普罗米修斯(prometheus)的监控数据采集器。

**项目地址：**

- Github: https://github.com/tangyibo/greenplum_exporter

- Gitee: https://gitee.com/inrgihc/greenplum_exporter

### 一、编译方法

- centos系统下编译

(1) 环境安装
```
wget https://gomirrors.org/dl/go/go1.14.12.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.14.12.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.io,direct
```

(2) 软件编译
```
git clone https://github.com/tangyibo/greenplum_exporter
cd greenplum_exporter/ && make build
cd bin && ls -l
```

- docker环境下编译

```
git clone https://github.com/tangyibo/greenplum_exporter
cd greenplum_exporter/
sh docker-build.sh
```

### 二、 启动采集器

- centos系统下

```
export GPDB_DATA_SOURCE_URL=postgres://gpadmin:password@10.17.20.11:5432/postgres?sslmode=disable
./greenplum_exporter --web.listen-address="0.0.0.0:9297" --web.telemetry-path="/metrics" --log.level=error
```

- docker运行

```
docker run -d -p 9297:9297 -e GPDB_DATA_SOURCE_URL=postgres://gpadmin:password@10.17.20.11:5432/postgres?sslmode=disable inrgihc/greenplum-exporter:latest 
```

注：环境变量GPDB_DATA_SOURCE_URL指定了连接Greenplum数据库的连接串（请使用gpadmin账号连接postgres库），该连接串以postgres://为前缀，具体格式如下：
```
postgres://gpadmin:password@10.17.20.11:5432/postgres?sslmode=disable
postgres://[数据库连接账号，必须为gpadmin]:[账号密码，即gpadmin的密码]@[数据库的IP地址]:[数据库端口号]/[数据库名称，必须为postgres]?[参数名]=[参数值]&[参数名]=[参数值]
```

然后访问监控指标的URL地址： *http://127.0.0.1:9297/metrics*

更多启动参数：

```
usage: greenplum_exporter [<flags>]

Flags:
  -h, --help                   Show context-sensitive help (also try --help-long and --help-man).
      --web.listen-address="0.0.0.0:9297"  
                               web endpoint
      --web.telemetry-path="/metrics"  
                               Path under which to expose metrics.
      --disableDefaultMetrics  do not report default metrics(go metrics and process metrics)
      --version                Show application version.
      --log.level="info"       Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal]
      --log.format="logger:stderr"  
                               Set the log target and format. Example: "logger:syslog?appname=bob&local=7" or "logger:stdout?json=true"

```

### 三、支持的监控指标

| No. | 指标名称	| 类型 | 标签组 |	度量单位 |	指标描述	| 数据源获取方法 |
|:----:|:----|:----|:----|:----|:----|:----|
|  1 | greenplum_cluster_state	| Gauge| version; master(master主机名)；standby(standby主机名) | boolean	| gp 可达状态 ?：1→ 可用;0→ 不可用 | SELECT count(\*) from gp_dist_random('gp_id'); select version(); SELECT hostname from p_segment_configuration where content=-1 and role='p'; |
|  2 | greenplum_cluster_uptime | Gauge | - | int | 启动持续的时间 | select extract(epoch from now() - pg_postmaster_start_time()); |
|  3 | greenplum_cluster_sync | Gauge | - | int | Master同步Standby状态? 1→ 正常;0→ 异常 | SELECT count(*) from pg_stat_replication where state='streaming' |
|  4 | greenplum_cluster_max_connections | Gauge | - | int | 最大连接个数 | show max_connections; show superuser_reserved_connections; |
|  5 | greenplum_cluster_total_connections	| Gauge | - |	int |	当前连接个数	| select count(\*) total, count(\*) filter(where current_query='<IDLE>') idle, count(\*) filter(where current_query<>'<IDLE>') active, count(\*) filter(where current_query<>'<IDLE>' and not waiting) running, count(\*) filter(where current_query<>'<IDLE>' and waiting) waiting from pg_stat_activity where procpid <> pg_backend_pid(); |
|  6 | greenplum_cluster_idle_connections | Gauge| - | int |	idle连接数 | 同上 |
|  7 | greenplum_cluster_active_connections | Gauge | - | int | active query | 同上 |
|  8 | greenplum_cluster_running_connections	| Gauge |	- | int |	query executing | 同上 |
|  9 | greenplum_cluster_waiting_connections	| Gauge | - | int | query waiting execute | 同上 |
| 10 | greenplum_node_segment_status | Gauge | hostname; address; dbid; content; preferred_role; port; replication_port | int	| segment的状态status: 1(U)→ up; 0(D)→ down | select * from gp_segment_configuration; |
| 11 | greenplum_node_segment_role | Gauge | hostname; address; dbid; content; preferred_role; port; replication_port | int	| segment的role角色: 1(P)→ primary; 2(M)→ mirror | 同上 |
| 12 | greenplum_node_segment_mode | Gauge | hostname; address; dbid; content; preferred_role; port; replication_port | int | segment的mode：1(S)→ Synced; 2(R)→ Resyncing; 3(C)→ Change Tracking; 4(N)→ Not Syncing | 同上|
| 13 | greenplum_node_segment_disk_free_mb_size | Gauge | hostname | MB | segment主机磁盘空间剩余大小（MB) | SELECT dfhostname as segment_hostname,sum(dfspace)/count(dfspace)/(1024*1024) as segment_disk_free_gb from gp_toolkit.gp_disk_free GROUP BY dfhostname|
| 14 | greenplum_cluster_total_connections_per_client | Gauge | client | int | 每个客户端的total连接数 |select usename, count(*) total, count(*) filter(where current_query='<IDLE>') idle, count(*) filter(where current_query<>'<IDLE>') active from pg_stat_activity group by 1; |
| 15 | greenplum_cluster_idle_connections_per_client | Gauge | client |	int |	每个客户端的idle连接数 | 同上 |
| 16 | greenplum_cluster_active_connections_per_client | Gauge | client |	int |	每个客户端的active连接数 | 同上 |
| 17 | greenplum_cluster_total_online_user_count | Gauge	| - | int | 在线账号数 |	同上 |
| 18 | greenplum_cluster_total_client_count  | Gauge | - |	int |	当前所有连接的客户端个数 | 同上 |
| 19 | greenplum_cluster_total_connections_per_user | Gauge |	usename |	int |	每个账号的total连接数	| select client_addr, count(*) total, count(*) filter(where current_query='<IDLE>') idle, count(*) filter(where current_query<>'<IDLE>') active from pg_stat_activity group by 1; |
| 20 | greenplum_cluster_idle_connections_per_user | Gauge | usename | int | 每个账号的idle连接数 | 同上 |
| 21 | greenplum_cluster_active_connections_per_user | Gauge | usename | int | 每个账号的active连接数 | 同上 |
| 22 | greenplum_cluster_config_last_load_time_seconds | Gauge	| - | int | 系统配置加载时间 |	SELECT pg_conf_load_time()  |
| 23 | greenplum_node_database_name_mb_size | Gauge | dbname | MB | 每个数据库占用的存储空间大小 |  SELECT dfhostname as segment_hostname,sum(dfspace)/count(dfspace)/(1024*1024) as segment_disk_free_gb from gp_toolkit.gp_disk_free GROUP BY dfhostname |
| 24 | greenplum_node_database_table_total_count | Gauge | dbname | - | 每个数据库内表的总数量 | SELECT count(*) as total from information_schema.tables where table_schema not in ('gp_toolkit','information_schema','pg_catalog');  |
| 25 | greenplum_exporter_total_scraped | Counter	| -| int | - | - |
| 26 | greenplum_exporter_total_error | Counter	| - | int	| - | - |
| 27 | greenplum_exporter_scrape_duration_second | Gauge	| - | int | - |	- |
| 28 | greenplum_server_users_name_list | Gauge	| - | int | 用户总数 |	SELECT usename from pg_catalog.pg_user; |
| 29 | greenplum_server_users_total_count | Gauge	| - | int | 用户明细 |	同上 |
| 30 | greenplum_server_locks_table_detail | Gauge	| pid;datname;usename;locktype;mode;application_name;state;lock_satus;query | int | 锁信息 |	 SELECT * from pg_locks |
| 31 | greenplum_server_database_hit_cache_percent_rate | Gauge	| - | float | 缓存命中率 |	select sum(blks_hit)/(sum(blks_read)+sum(blks_hit))*100 from pg_stat_database; |
| 32 | greenplum_server_database_transition_commit_percent_rate | Gauge	| - | float | 事务提交率 |	select sum(xact_commit)/(sum(xact_commit)+sum(xact_rollback))*100 from pg_stat_database; |
| 32 | greenplum_server_database_table_bloat_list | Gauge	| - | int | 数据膨胀列表 |	select * from gp_toolkit.gp_bloat_diag; |
| 33 | greenplum_server_database_table_skew_list | Gauge	| - | int | 数据倾斜列表 |	select * from  gp_toolkit.gp_skew_coefficients; |

### 四、Grafana图

- Dashboard

Grafana Dashboard ID: 13822

Grafana Dashboard URL: https://grafana.com/grafana/dashboards/13822

- 配置教程

可参考文章：https://blog.csdn.net/inrgihc/article/details/108686638

### 五、问题反馈

如果您看到或使用了本工具，或您觉得本工具对您有价值，请为此项目**点个赞**!!
