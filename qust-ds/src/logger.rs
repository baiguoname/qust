use serde_json::{json, Map, Value};
use tracing::{field::Visit, Subscriber};
use tracing_subscriber::{
    filter, fmt::{self, format::Writer,  FmtContext, FormatFields}, layer::{Layer, SubscriberExt}, 
    registry::LookupSpan, util::SubscriberInitExt, Registry,
    reload::{self, Handle}
};
use tracing_appender::non_blocking::WorkerGuard;
use chrono::Local;
use std::{ fs, thread, time::Duration };
use crate::prelude::*;

struct JsonVisitor {
    map: Map<String, Value>,
}

impl JsonVisitor {
    fn new() -> Self {
        JsonVisitor {
            map: Map::new(),
        }
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.map.insert(field.name().to_string(), json!(value));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.map.insert(field.name().to_string(), json!(format!("{:?}", value)));
    }
}

trait ExtractString {
    fn extract_string(self) -> String;
}

impl ExtractString for Option<Value> {
    fn extract_string(self) -> String {
        match self {
            Some(Value::String(s)) => s,           
            Some(Value::Number(n)) => n.to_string(),
            _ => "".to_string(),                    
        }
    }
}

struct CustomJsonFormatter;

impl<S, N> fmt::FormatEvent<S, N> for CustomJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let mut visitor = JsonVisitor::new();
        event.record(&mut visitor);
        writeln!(
            writer, "{} {} {:<150} {}:{}", 
            Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            visitor.map.remove("message").extract_string(),
            "",
            visitor.map.remove("log.file").extract_string(),
            visitor.map.remove("log.line").extract_string(),
        )
    }
}

type LayerType = Box<dyn Layer<Registry> + Send + Sync>;
type HandleType = Handle<Vec<LayerType>, Registry>;
pub fn setup_logging(p: &str, date: da, ticker_vec: &[String], handle: Option<HandleType>) -> (Vec<WorkerGuard>, HandleType) {
    p.build_an_empty_dir();
    // p.create_a_dir();
    let log_dir = format!("{}/{}", p, date);
    fs::create_dir_all(&log_dir).unwrap();
    let mut str_vec = ticker_vec.to_vec();
    str_vec.push("ctp".into());
    str_vec.push("spy".into());
    str_vec.push("stra".into());
    let mut guard_vec = Vec::with_capacity(ticker_vec.len());
    let layers = str_vec
        .into_iter()
        .map(|s| {

            let (non_blocking, guard) = tracing_appender::non_blocking(tracing_appender::rolling::never(
                &log_dir,
                format!("{}.log", s),
            ));
            let filter_ticker = filter::filter_fn(move |metadata| {
                metadata.target() == s
            });
            let layer_ticker = fmt::layer()
                .event_format(CustomJsonFormatter)
                .with_writer(non_blocking)
                .with_filter(filter_ticker);
            guard_vec.push(guard);
            layer_ticker.boxed()
        })
        .collect::<Vec<_>>();

    match handle {
        Some(handle) => {
            handle.modify(|x| *x = layers).unwrap();
            (guard_vec, handle)
        }
        None => {
            let (layer, handle) = reload::Layer::new(layers);
            tracing_subscriber::registry().with(layer).init();
            (guard_vec, handle)
        }
   }
}

pub fn logging_service(p: String, ticker_vec: Vec<String>) {
    let mut last_date = Local::now().date_naive();
    let (mut guard_vec, mut handle) = setup_logging(&p, last_date, &ticker_vec, None);
    thread::spawn(move || {
        loop {
            // let now_date = Local::now().date_naive().succ_opt().unwrap();
            let now_date = last_date;
            if now_date != last_date {
                guard_vec.clear();
                last_date = now_date;
                thread::sleep(Duration::from_secs(1));
                let (new_guard_vec, new_handle) = setup_logging(&p, last_date, &ticker_vec, Some(handle));
                guard_vec = new_guard_vec;
                handle = new_handle;
            }
            thread::sleep(Duration::from_secs(100)); 
        }
    });
}

pub fn jupyter_logging<T: std::fmt::Debug>(tickers: &[T]) -> Vec<WorkerGuard> {
    setup_logging(
        "./logs",
        Local::now().date_naive(),
        &tickers.map(|x| format!("{:?}", x)),
        None,
    ).0
}
