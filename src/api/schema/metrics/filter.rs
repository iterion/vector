use std::collections::BTreeMap;

use async_stream::stream;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};

use super::{
    EventsInTotal, EventsOutTotal, ProcessedBytesTotal, ProcessedEventsTotal, ReceivedEventsTotal,
    SentEventsTotal,
};
use crate::{
    config::ComponentKey,
    event::{Metric, MetricValue},
    metrics::Controller,
};

fn get_controller() -> &'static Controller {
    Controller::get().expect("Metrics system not initialized. Please report.")
}

/// Sums an iteratable of `&Metric`, by folding metric values. Convenience function typically
/// used to get aggregate metrics.
fn sum_metrics<'a, I: IntoIterator<Item = &'a Metric>>(metrics: I) -> Option<Metric> {
    let mut iter = metrics.into_iter();
    let m = iter.next()?;

    Some(iter.fold(
        m.clone(),
        |mut m1, m2| {
            if m1.update(&m2) {
                m1
            } else {
                m2.clone()
            }
        },
    ))
}

/// Sums an iteratable of `Metric`, by folding metric values. Convenience function typically
/// used to get aggregate metrics.
fn sum_metrics_owned<I: IntoIterator<Item = Metric>>(metrics: I) -> Option<Metric> {
    let mut iter = metrics.into_iter();
    let m = iter.next()?;

    Some(iter.fold(m, |mut m1, m2| if m1.update(&m2) { m1 } else { m2 }))
}

pub trait MetricsFilter<'a> {
    fn processed_events_total(&self) -> Option<ProcessedEventsTotal>;
    fn processed_bytes_total(&self) -> Option<ProcessedBytesTotal>;
    fn received_events_total(&self) -> Option<ReceivedEventsTotal>;
    fn events_in_total(&self) -> Option<EventsInTotal>;
    fn events_out_total(&self) -> Option<EventsOutTotal>;
    fn sent_events_total(&self) -> Option<SentEventsTotal>;
}

impl<'a> MetricsFilter<'a> for Vec<Metric> {
    fn processed_events_total(&self) -> Option<ProcessedEventsTotal> {
        let sum = sum_metrics(self.iter().filter(|m| m.name() == "processed_events_total"))?;

        Some(ProcessedEventsTotal::new(sum))
    }

    fn processed_bytes_total(&self) -> Option<ProcessedBytesTotal> {
        let sum = sum_metrics(self.iter().filter(|m| m.name() == "processed_bytes_total"))?;

        Some(ProcessedBytesTotal::new(sum))
    }

    fn events_in_total(&self) -> Option<EventsInTotal> {
        let sum = sum_metrics(self.iter().filter(|m| m.name() == "events_in_total"))?;

        Some(EventsInTotal::new(sum))
    }

    fn received_events_total(&self) -> Option<ReceivedEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "component_received_events_total"),
        )?;

        Some(ReceivedEventsTotal::new(sum))
    }

    fn events_out_total(&self) -> Option<EventsOutTotal> {
        let sum = sum_metrics(self.iter().filter(|m| m.name() == "events_out_total"))?;

        Some(EventsOutTotal::new(sum))
    }

    fn sent_events_total(&self) -> Option<SentEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "component_sent_events_total"),
        )?;

        Some(SentEventsTotal::new(sum))
    }
}

impl<'a> MetricsFilter<'a> for Vec<&'a Metric> {
    fn processed_events_total(&self) -> Option<ProcessedEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "processed_events_total")
                .copied(),
        )?;

        Some(ProcessedEventsTotal::new(sum))
    }

    fn processed_bytes_total(&self) -> Option<ProcessedBytesTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "processed_bytes_total")
                .copied(),
        )?;

        Some(ProcessedBytesTotal::new(sum))
    }

    fn received_events_total(&self) -> Option<ReceivedEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "component_received_events_total")
                .copied(),
        )?;

        Some(ReceivedEventsTotal::new(sum))
    }

    fn events_in_total(&self) -> Option<EventsInTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "events_in_total")
                .copied(),
        )?;

        Some(EventsInTotal::new(sum))
    }

    fn events_out_total(&self) -> Option<EventsOutTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "events_out_total")
                .copied(),
        )?;

        Some(EventsOutTotal::new(sum))
    }

    fn sent_events_total(&self) -> Option<SentEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "component_sent_events_total")
                .copied(),
        )?;

        Some(SentEventsTotal::new(sum))
    }
}

/// Returns a stream of `Metric`s, collected at the provided millisecond interval.
pub fn get_metrics(interval: i32) -> impl Stream<Item = Metric> {
    let controller = get_controller();
    let mut interval = tokio::time::interval(Duration::from_millis(interval as u64));

    stream! {
        loop {
            interval.tick().await;
            for m in controller.capture_metrics() {
                yield m;
            }
        }
    }
}

pub fn get_all_metrics(interval: i32) -> impl Stream<Item = Vec<Metric>> {
    let controller = get_controller();
    let mut interval = tokio::time::interval(Duration::from_millis(interval as u64));

    stream! {
        loop {
            interval.tick().await;
            yield controller.capture_metrics()
        }
    }
}

/// Return Vec<Metric> based on a component id tag.
pub fn by_component_key(component_key: &ComponentKey) -> Vec<Metric> {
    get_controller()
        .capture_metrics()
        .into_iter()
        .filter_map(|m| m.tag_matches("component_id", component_key.id()).then(|| m))
        .collect()
}

type MetricFilterFn = dyn Fn(&Metric) -> bool + Send + Sync;

/// Returns a stream of `Vec<Metric>`, where `metric_name` matches the name of the metric
/// (e.g. "processed_events_total"), and the value is derived from `MetricValue::Counter`. Uses a
/// local cache to match against the `component_id` of a metric, to return results only when
/// the value of a current iteration is greater than the previous. This is useful for the client
/// to be notified as metrics increase without returning 'empty' or identical results.
pub fn component_counter_metrics(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = Vec<Metric>> {
    let mut cache = BTreeMap::new();

    get_all_metrics(interval).map(move |m| {
        m.into_iter()
            .filter(filter_fn)
            .filter_map(|m| m.tag_value("component_id").map(|id| (id, m)))
            .fold(BTreeMap::new(), |mut map, (id, m)| {
                map.entry(id).or_insert_with(Vec::new).push(m);
                map
            })
            .into_iter()
            .filter_map(|(id, metrics)| {
                let m = sum_metrics_owned(metrics)?;
                match m.value() {
                    MetricValue::Counter { value }
                        if cache.insert(id, *value).unwrap_or(0.00) < *value =>
                    {
                        Some(m)
                    }
                    _ => None,
                }
            })
            .collect()
    })
}

/// Returns the throughput of a 'counter' metric, sampled over `interval` milliseconds
/// and filtered by the provided `filter_fn`.
pub fn counter_throughput(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = (Metric, f64)> {
    let mut last = 0.00;

    get_metrics(interval)
        .filter(filter_fn)
        .filter_map(move |m| match m.value() {
            MetricValue::Counter { value } if *value > last => {
                let throughput = value - last;
                last = *value;
                Some((m, throughput))
            }
            _ => None,
        })
        // Ignore the first, since we only care about sampling between `interval`
        .skip(1)
}

/// Returns the throughput of a 'counter' metric, sampled over `interval` milliseconds
/// and filtered by the provided `filter_fn`, aggregated against each component.
pub fn component_counter_throughputs(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = Vec<(Metric, f64)>> {
    let mut cache = BTreeMap::new();

    get_all_metrics(interval)
        .map(move |m| {
            m.into_iter()
                .filter(filter_fn)
                .filter_map(|m| m.tag_value("component_id").map(|id| (id, m)))
                .fold(BTreeMap::new(), |mut map, (id, m)| {
                    map.entry(id).or_insert_with(Vec::new).push(m);
                    map
                })
                .into_iter()
                .filter_map(|(id, metrics)| {
                    let m = sum_metrics_owned(metrics)?;
                    match m.value() {
                        MetricValue::Counter { value } => {
                            let last = cache.insert(id, *value).unwrap_or(0.00);
                            let throughput = value - last;
                            Some((m, throughput))
                        }
                        _ => None,
                    }
                })
                .collect()
        })
        // Ignore the first, since we only care about sampling between `interval`
        .skip(1)
}
