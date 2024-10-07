use crate::prelude::{gen_inter, KlineInfo, Ticker};
use crate::trade::di::{Di, PriceArc, PriceOri, PriceTick};
use chrono::Duration;
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use std::fmt::Debug;

#[derive(Debug, Clone, Default)]
pub struct KlineData {
    pub t: dt,
    pub o: f32,
    pub h: f32,
    pub l: f32,
    pub c: f32,
    pub v: f32,
    pub ki: KlineInfo,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TickData {
    pub t: dt,
    pub c: f32,
    pub v: f32,
    pub bid1: f32,
    pub ask1: f32,
    pub bid1_v: f32,
    pub ask1_v: f32,
    pub ct: i32,
}

#[derive(Default)]
pub struct KlineWithState {
    pub data: KlineData,
    pub last: KlineState,
    pub current: KlineState,
}

impl TickData {
    pub fn from_a_str(data: &str) -> Result<TickData, serde_json::Error> {
        serde_json::from_str::<Self>(data)
    }

    pub fn from_bytes(data: &[u8]) -> Result<TickData, serde_json::Error> {
        TickData::from_a_str(&String::from_utf8_lossy(data))
    }
}

pub trait UpdateData<T> {
    fn update_begin(&mut self, data: &T);
    fn update_merging(&mut self, data: &T);
    fn update_ignor(&mut self, data: &T);
    fn update_finish(&mut self, data: &T);
}

impl UpdateData<TickData> for KlineData {
    fn update_begin(&mut self, data: &TickData) {
        self.t = data.t;
        self.o = data.c;
        self.h = data.c;
        self.l = data.c;
        self.c = data.c;
        self.v = data.v;
        self.ki.open_time = data.t;
        self.ki.pass_this = 1;
        self.ki.contract = data.ct;
    }

    fn update_merging(&mut self, data: &TickData) {
        self.t = data.t;
        self.h = self.h.max(data.c);
        self.l = self.l.min(data.c);
        self.c = data.c;
        self.v += data.v;
        self.ki.pass_this += 1;
    }

    fn update_ignor(&mut self, _data: &TickData) {
        self.ki.pass_last += 1;
    }

    fn update_finish(&mut self, data: &TickData) {
        self.update_merging(data);
    }
}

impl UpdateData<KlineData> for KlineData {
    fn update_begin(&mut self, data: &KlineData) {
        self.t = data.t;
        self.o = data.o;
        self.h = data.h;
        self.l = data.l;
        self.c = data.c;
        self.v = data.v;
        self.ki.open_time = data.ki.open_time;
        self.ki.pass_this = data.ki.pass_this;
        self.ki.contract = data.ki.contract;
        self.ki.pass_last += data.ki.pass_last.max(1) - 1;
    }

    fn update_merging(&mut self, data: &KlineData) {
        self.t = data.t;
        self.h = self.h.max(data.h);
        self.l = self.l.min(data.l);
        self.c = data.c;
        self.v += data.v;
        self.ki.pass_this += data.ki.pass_last + data.ki.pass_this;
    }
    fn update_ignor(&mut self, data: &KlineData) {
        self.ki.pass_last += data.ki.pass_last;
    }
    fn update_finish(&mut self, data: &KlineData) {
        self.update_merging(data);
    }
}

pub trait UpdateDataState<T> {
    fn update(&mut self, data: &T);
}

impl<T> UpdateDataState<T> for KlineWithState
where
    KlineData: UpdateData<T>,
{
    fn update(&mut self, data: &T) {
        if let KlineState::Finished = self.last {
            self.data.ki.pass_last = 1;
        }
        match self.current {
            KlineState::Ignor => self.data.update_ignor(data),
            KlineState::Begin => self.data.update_begin(data),
            KlineState::Merging => self.data.update_merging(data),
            KlineState::Finished => self.data.update_finish(data),
        }
        self.last = self.current.clone();
    }
}

impl KlineWithState {
    pub fn update_to<T>(&mut self, data: &T, price: &mut PriceOri)
    where
        KlineData: UpdateData<T>,
    {
        self.update(data);
        if let KlineState::Finished = self.last {
            price.update(&self.data);
        }
    }
}

impl PriceOri {
    pub fn update(&mut self, data: &KlineData) {
        self.t.push(data.t);
        self.o.push(data.o);
        self.h.push(data.h);
        self.l.push(data.l);
        self.c.push(data.c);
        self.v.push(data.v);
        self.ki.push(data.ki.clone());
    }

    pub fn to_kline_data(&self) -> Vec<KlineData> {
        izip!(
            self.t.iter(),
            self.o.iter(),
            self.h.iter(),
            self.l.iter(),
            self.c.iter(),
            self.v.iter(),
            self.ki.iter(),
        )
        .map(|(&t, &o, &h, &l, &c, &v, ki)| KlineData {
            t,
            o,
            h,
            l,
            c,
            v,
            ki: ki.clone(),
        })
        .collect_vec()
    }
}

impl PriceTick {
    pub fn update(&mut self, data: &TickData) {
        self.t.push(data.t);
        self.c.push(data.c);
        self.v.push(data.v);
        self.bid1.push(data.bid1);
        self.ask1.push(data.ask1);
        self.bid1_v.push(data.bid1_v);
        self.ask1_v.push(data.ask1_v);
        self.ct.push(data.ct);
    }

    pub fn to_tick_data(&self) -> Vec<TickData> {
        izip!(
            self.t.iter(),
            self.c.iter(),
            self.v.iter(),
            self.bid1.iter(),
            self.ask1.iter(),
            self.bid1_v.iter(),
            self.ask1_v.iter(),
            self.ct.iter(),
        )
        .map(
            |(&t, &c, &v, &bid1, &ask1, &bid1_v, &ask1_v, &ct)| TickData {
                t,
                c,
                v,
                bid1,
                ask1,
                bid1_v,
                ask1_v,
                ct,
            },
        )
        .collect_vec()
    }

    pub fn from_tick_data(tick_data: &[TickData]) -> Self {
        let mut price_tick = PriceTick::with_capacity(tick_data.len());
        tick_data
            .iter()
            .for_each(|x| {
                price_tick.update(x);
            });
        price_tick
    }
}

#[derive(Default, Clone, Debug)]
pub enum KlineState {
    Ignor,
    Begin,
    Merging,
    #[default]
    Finished,
}

impl From<bool> for KlineState {
    fn from(value: bool) -> Self {
        if value {
            KlineState::Finished
        } else {
            KlineState::Ignor
        }
    }
}
impl From<KlineState> for bool {
    fn from(value: KlineState) -> Self {
        matches!(value, KlineState::Finished)
    }
}
impl From<&KlineState> for bool {
    fn from(value: &KlineState) -> Self {
        matches!(value, KlineState::Finished)
    }
}

/* #region fn */
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Interval {
    Time(tt, tt),
    DayJump(tt, i64, tt),
}

impl Default for Interval {
    fn default() -> Self {
        Self::Time(Default::default(), Default::default())
    }
}

impl Interval {
    pub fn is_in(&self, _date: &da, time: &tt) -> bool {
        match self {
            Interval::Time(start, end) => time >= start && time <= end,
            Interval::DayJump(start, _, end) => time >= start || time <= end,
        }
    }

    fn is_end(&self, state: &mut (da, i64), date: &da, time: &tt) -> bool {
        match self {
            Interval::Time(_start, end) => time >= end || date != &state.0,
            Interval::DayJump(start, len, end) => {
                if date > &state.0 || (state.1 == 0 && time < start) {
                    state.0 = *date;
                    state.1 += 1;
                }
                &state.1 > len || (&state.1 == len && time >= end)
            }
        }
    }

    pub fn get_time_end<'a>(intervals: &'a [Interval], date: &da, time: &tt) -> Option<&'a Self> {
        intervals.iter().find(|x| x.is_in(date, time))
    }

    pub fn end_time(&self) -> tt {
        match self {
            Interval::Time(_, end_time) => *end_time,
            Interval::DayJump(_, _, end_time) => *end_time,
        }
    }
}
/* #endregion */

/* #region PriceOriFromTickData */
#[clone_trait]
pub trait Tri {
    fn gen_price_ori(&self, price_tick: &PriceTick) -> PriceOri {
        let num_days = (price_tick.t.last().unwrap().date() - price_tick.t.first().unwrap().date())
            .num_days() as usize;
        PriceOri::with_capacity(num_days * 50)
    }
    fn update_tick_func(&self, _ticker: Ticker) -> UpdateFuncTick;
}
/* #endregion */

/* #region PriceOriFromKlineData */
#[clone_trait]
pub trait Pri {
    fn gen_price_ori(&self, price_tick: &PriceArc) -> PriceOri {
        let num_days = (price_tick.t.last().unwrap().date() - price_tick.t.first().unwrap().date())
            .num_days() as usize;
        PriceOri::with_capacity(num_days * 50)
    }
    fn update_kline_func(&self, di: &Di, price: &PriceArc) -> UpdateFuncKline;
}
/* #endregion */

/* #region Inter Types */
#[derive(Default)]
pub struct KlineStateInter {
    pub kline_state: KlineWithState,
    pub record: (da, i64),
    pub time_range: Interval,
    pub intervals: Vec<Interval>,
}

impl UpdateDataState<TickData> for KlineStateInter {
    fn update(&mut self, data: &TickData) {
        self.kline_state.current = self.check_datetime(&data.t);
        self.kline_state.update(data);
    }
}

impl UpdateDataState<KlineData> for KlineStateInter {
    fn update(&mut self, data: &KlineData) {
        self.kline_state.current = self.check_datetime(&data.t);
        self.kline_state.update(data);
    }
}

impl KlineStateInter {
    pub fn from_intervals(intervals: Vec<Interval>) -> Self {
        Self {
            time_range: intervals[0].clone(),
            intervals,
            ..Default::default()
        }
    }

    pub fn check_datetime(&mut self, t: &dt) -> KlineState {
        let (date, time) = (t.date(), t.time());
        match self.kline_state.last {
            KlineState::Finished | KlineState::Ignor => {
                match Interval::get_time_end(&self.intervals, &date, &time) {
                    None => KlineState::Ignor,
                    Some(tc) => {
                        self.time_range = tc.clone();
                        self.record = (date, 0);
                        KlineState::Begin
                    }
                }
            }
            KlineState::Begin | KlineState::Merging => {
                if self.time_range.is_end(&mut self.record, &date, &time) {
                    KlineState::Finished
                } else {
                    KlineState::Merging
                }
            }
        }
    }
}

#[clone_trait]
pub trait Inter {
    fn intervals(&self) -> Vec<Interval> {
        vec![]
    }
}

#[typetag::serde]
impl Tri for InterBox {
    fn update_tick_func(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut kline = KlineStateInter::from_intervals(self.intervals());
        Box::new(move |tick_data, price_ori| {
            kline.update(tick_data);
            if let KlineState::Finished = kline.kline_state.last {
                price_ori.update(&kline.kline_state.data);
            }
            kline.kline_state.last.clone()
        })
    }
}

#[typetag::serde]
impl Pri for InterBox {
    fn update_kline_func(&self, _di: &Di, _price: &PriceArc) -> UpdateFuncKline {
        let mut kline = KlineStateInter::from_intervals(self.intervals());
        Box::new(move |kline_data, price_ori, _i| {
            kline.update(kline_data);
            if let KlineState::Finished = kline.kline_state.last {
                price_ori.update(&kline.kline_state.data);
            }
            kline.kline_state.last.clone()
        })
    }
}

/* #region fn */
pub fn even_slice_time(start: tt, end: tt, step: Duration, offset: Duration) -> Vec<Interval> {
    let mut res = vec![];
    let mut start_last = start;
    let step_offset = step - offset;
    loop {
        let end_last = start_last + step_offset;
        if end_last < start_last {
            let interval_ = Interval::DayJump(start_last, 1, end_last);
            res.push(interval_);
            break;
        } else if start_last <= end && end_last >= end {
            let interval_ = Interval::Time(start_last, end);
            res.push(interval_);
            break;
        } else {
            let interval_ = Interval::Time(start_last, end_last);
            res.push(interval_);
            start_last = end_last + offset;
        }
    }
    res
}
pub fn even_slice_time_usize(start: usize, end: usize, step: i64) -> Vec<Interval> {
    [even_slice_time(
        start.to_tt(),
        end.to_tt(),
        Duration::seconds(step),
        Duration::milliseconds(500),
    )]
    .concat()
}
/* #endregion */

pub type UpdateFuncTick = Box<dyn FnMut(&TickData, &mut PriceOri) -> KlineState>;
pub type UpdateFuncKline = Box<dyn FnMut(&KlineData, &mut PriceOri, usize) -> KlineState>;

gen_inter!(
    Rlast,
    vec![Interval::DayJump(210000.0.to_tt(), 1, 145550.0.to_tt())],
    rlast
);

gen_inter!(
    Rl5m,
    vec![
        (091000.0, 091259.5),(091300.0, 091759.5),(091800.0, 092259.5),(092300.0, 092759.5),
        (092800.0, 093259.5),(093300.0, 093759.5),(093800.0, 094259.5),(094300.0, 094759.5),
        (094800.0, 095259.5),(095300.0, 095759.5),(095800.0, 100259.5),(100300.0, 100759.5),
        (100800.0, 101450.5),(103000.0, 103259.5),(103300.0, 103759.5),(103800.0, 104259.5),
        (104300.0, 104759.5),(104800.0, 105259.5),(105300.0, 105759.5),(105800.0, 110259.5),
        (110300.0, 110759.5),(110800.0, 111259.5),(111300.0, 111759.5),(111800.0, 112259.5),
        (112300.0, 112950.5),(133000.0, 133259.5),(133300.0, 133759.5),(133800.0, 134259.5),
        (134300.0, 134759.5),(134800.0, 135259.5),(135300.0, 135759.5),(135800.0, 140259.5),
        (140300.0, 140759.5),(140800.0, 141259.5),(141300.0, 141759.5),(141800.0, 142259.5),
        (142300.0, 142759.5),(142800.0, 143259.5),(143300.0, 143759.5),(143800.0, 144259.5),
        (144300.0, 144759.5),(144800.0, 145259.5),(145300.0, 145659.5),
    ].iter()
        .map(|(x, y)| Interval::Time(x.to_tt(), y.to_tt()))
        .collect(),
    rl5m
);

gen_inter!(
    Rl5m_cut,
    Rl5m.intervals()[10..].to_vec(),
    rl5m_cut
);

gen_inter!(
    Rl30mDay,
    vec![
        Interval::Time(90530.to_tt(), 93030.5.to_tt()),
        Interval::Time(93031.to_tt(), 95930.5.to_tt()),
        Interval::Time(95931.to_tt(), 101450.to_tt()),
        Interval::Time(103000.to_tt(), 105930.5.to_tt()),
        Interval::Time(105931.to_tt(), 112930.to_tt()),
        Interval::Time(133000.to_tt(), 135930.5.to_tt()),
        Interval::Time(135931.to_tt(), 142930.5.to_tt()),
        Interval::Time(142931.to_tt(), 145900.to_tt()),
    ],
    rl30mday
);