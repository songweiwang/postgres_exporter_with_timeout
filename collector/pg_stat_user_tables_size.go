// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	userTableSizeSubsystem = "stat_user_tables_size"
	timeout_seconds = 1
)

func init() {
	registerCollector(userTableSizeSubsystem, defaultEnabled, NewPGStatUserTablesSizeCollector)
}

type PGStatUserTablesSizeCollector struct {
	log log.Logger
}

func NewPGStatUserTablesSizeCollector(config collectorConfig) (Collector, error) {
	return &PGStatUserTablesSizeCollector{log: config.logger}, nil
}

var (	
	statUserTablesTotalSize = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, userTableSubsystem, "size_bytes"),
		"Total disk space used by this table, in bytes, including all indexes and TOAST data",
		[]string{"datname", "schemaname", "relname"},
		prometheus.Labels{},
	)

	statUserTablesSizeQuery = `SELECT
		current_database() datname,
		schemaname,
		relname,		
		pg_total_relation_size(relid) as total_size
	FROM
		pg_stat_user_tables`
)

func (c *PGStatUserTablesSizeCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()

	ctx_timeout, cancel := context.WithTimeout(context.Background(), timeout_seconds*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx_timeout, statUserTablesSizeQuery)

	if  ctx_timeout.Err() != nil{
		if rows != nil {
			rows.Close()
		}
		cancel()
		return ctx_timeout.Err()
	}
	
	if err != nil {
        return err
    }
	
	defer rows.Close()

	for rows.Next() {
		var datname, schemaname, relname sql.NullString
		var totalSize sql.NullInt64

		if err := rows.Scan(&datname, &schemaname, &relname, &totalSize); err != nil {
			return err
		}

		datnameLabel := "unknown"
		if datname.Valid {
			datnameLabel = datname.String
		}
		schemanameLabel := "unknown"
		if schemaname.Valid {
			schemanameLabel = schemaname.String
		}
		relnameLabel := "unknown"
		if relname.Valid {
			relnameLabel = relname.String
		}

		totalSizeMetric := 0.0
		if totalSize.Valid {
			totalSizeMetric = float64(totalSize.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statUserTablesTotalSize,
			prometheus.GaugeValue,
			totalSizeMetric,
			datnameLabel, schemanameLabel, relnameLabel,
		)
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
