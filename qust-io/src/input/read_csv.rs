use std::path::Path;

use csv::StringRecord;
use qust::prelude::{dt, KlineData, PriceOri, TickData};


trait ReadRecord {
    type Output;
    fn read_record(&self, record: &StringRecord) -> Self::Output;
}

pub trait ReadCsv {
    type Output;
    fn read_csv(&self, path: impl AsRef<Path>) -> Self::Output;
}

pub struct DiReader<T> {
    pub t: T,
    pub o: T,
    pub h: T,
    pub l: T,
    pub c: T,
    pub v: T,
    pub t_format: Option<&'static str>,
    pub has_header: bool,
}

struct DiReaderRecord {
    t: usize,
    o: usize,
    h: usize,
    l: usize,
    c: usize,
    v: usize,
    t_format: &'static str,
}


impl ReadRecord for DiReaderRecord {
    type Output = KlineData;
    fn read_record(&self, record: &StringRecord) -> Self::Output {
        KlineData {
            t: {
                let t = record[self.t].trim();
                dt::parse_from_str(t, self.t_format)
                    .unwrap_or_else(|_| panic!("failed to parse time, provided: {}, format: {}", t, self.t_format))
            },
            o: record[self.o].trim().parse().unwrap(),
            h: record[self.h].trim().parse().unwrap(),
            l: record[self.l].trim().parse().unwrap(),
            c: record[self.c].trim().parse().unwrap(),
            v: record[self.v].trim().parse().unwrap(),
            ki: Default::default(),
        }
    }
}


impl ReadCsv for DiReader<usize> {
    type Output = PriceOri;
    fn read_csv(&self, path: impl AsRef<Path>) -> Self::Output {
        let mut reader = csv::Reader::from_path(path).unwrap();
        let di_reader_record = DiReaderRecord {
            t: self.t,
            o: self.o,
            h: self.h,
            l: self.l,
            c: self.c,
            v: self.v,
            t_format: self.t_format.unwrap_or("%Y-%m-%dT%H:%M:%S%.f"),
        };
        let skip_n = if self.has_header { 1 } else { 0 };
        let mut price_ori = PriceOri::with_capacity(100_000);
        for record in reader.records().skip(skip_n) {
            let record = record.unwrap();
            let kline_data = di_reader_record.read_record(&record);
            price_ori.update(&kline_data);
        }
        price_ori.shrink_to_fit();
        price_ori
    }
}

pub struct TickReader<T> {
    pub t: T,
    pub c: T,
    pub v: T,
    pub ask1: T,
    pub bid1: T,
    pub ask1_v: T,
    pub bid1_v: T,
    pub t_format: Option<&'static str>,
    pub has_header: bool,
}

struct TickReaderRecord {
    t: usize,
    c: usize,
    v: usize,
    ask1: usize,
    bid1: usize,
    ask1_v: usize,
    bid1_v: usize,
    t_format: &'static str,
}

impl ReadRecord for TickReaderRecord {
    type Output = TickData;
    fn read_record(&self, record: &StringRecord) -> Self::Output {
        TickData {
            t: {
                let t = record[self.t].trim();
                dt::parse_from_str(t, self.t_format)
                    .unwrap_or_else(|_| panic!("failed to parse time, provided: {}, format: {}", t, self.t_format))
            },
            c: record[self.c].trim().parse().unwrap(),
            v: record[self.v].trim().parse().unwrap(),
            ask1: record[self.ask1].trim().parse().unwrap(),
            bid1: record[self.bid1].trim().parse().unwrap(),
            ask1_v: record[self.ask1_v].trim().parse().unwrap(),
            bid1_v: record[self.bid1_v].trim().parse().unwrap(),
            ct: 1,
        }

    }
}


impl ReadCsv for TickReader<usize> {
    type Output = Vec<TickData>;
    fn read_csv(&self, path: impl AsRef<Path>) -> Self::Output {
        let mut reader = csv::Reader::from_path(path).unwrap();
        let tick_reader_record = TickReaderRecord {
            t: self.t,
            c: self.c,
            v: self.v,
            ask1: self.ask1,
            bid1: self.bid1,
            ask1_v: self.ask1_v,
            bid1_v: self.bid1_v,
            t_format: self.t_format.unwrap_or("%Y-%m-%dT%H:%M:%S%.f"),
        };
        let mut res = Vec::with_capacity(100000);
        for record in reader.records() {
            let record = record.unwrap();
            let tick_data = tick_reader_record.read_record(&record);
            res.push(tick_data);
        }
        res.shrink_to_fit();
        res
    }
}
