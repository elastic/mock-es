package api

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	rootTotalMetrics           metric.Int64Counter
	licenseTotalMetrics        metric.Int64Counter
	bulkCreateTotalMetrics     metric.Int64Counter
	bulkCreateDuplicateMetrics metric.Int64Counter
	bulkCreateTooManyMetrics   metric.Int64Counter
	bulkCreateNonIndexMetrics  metric.Int64Counter
	bulkCreateOkMetrics        metric.Int64Counter
	bulkCreateTooLargeMetrics  metric.Int64Counter
	bulkIndexTotalMetrics      metric.Int64Counter
	bulkUpdateTotalMetrics     metric.Int64Counter
	bulkDeleteTotalMetrics     metric.Int64Counter
}

func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	m := &metrics{}
	meter := provider.Meter("github.com/elastic/mock-es")

	for k, v := range map[string]*metric.Int64Counter{
		"root.total":            &m.rootTotalMetrics,
		"license.total":         &m.licenseTotalMetrics,
		"bulk.create.total":     &m.bulkCreateTotalMetrics,
		"bulk.create.duplicate": &m.bulkCreateDuplicateMetrics,
		"bulk.create.too_many":  &m.bulkCreateTooManyMetrics,
		"bulk.create.non_index": &m.bulkCreateNonIndexMetrics,
		"bulk.create.ok":        &m.bulkCreateOkMetrics,
		"bulk.create.too_large": &m.bulkCreateTooLargeMetrics,
		"bulk.index.total":      &m.bulkIndexTotalMetrics,
		"bulk.update.total":     &m.bulkUpdateTotalMetrics,
		"bulk.delete.total":     &m.bulkDeleteTotalMetrics,
	} {
		if err := newCounter(meter, v, k); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func newCounter(meter metric.Meter, counter *metric.Int64Counter, name string) error {
	c, err := meter.Int64Counter(name)
	if err != nil {
		return fmt.Errorf("failed to create counter: %s", name)
	}

	*counter = c
	return nil
}
