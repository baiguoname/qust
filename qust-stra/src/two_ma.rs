use qust::prelude::*;
use qust_ds::prelude::*;
use qust_derive::*;
use ta::indicators::SimpleMovingAverage as SMA;
use ta::Next;

#[ta_derive2]
pub struct TwoMaStra {
    pub short_period: usize,
    pub long_period: usize,
}

#[typetag::serde]
impl Ktn for TwoMaStra {
    fn ktn(&self,_di: &Di) -> RetFnKtn {
        let mut last_norm_hold = NormHold::No;
        let mut short_ma = SMA::new(self.short_period).unwrap();
        let mut long_ma = SMA::new(self.long_period).unwrap();
        let mut last_short_value = 0f64;
        let mut last_long_value = 0f64;
        Box::new(move |di_kline| {
            let c = di_kline.di.c()[di_kline.i] as f64;
            let short_value = short_ma.next(c);
            let long_value = long_ma.next(c);
            match last_norm_hold {
                NormHold::No if di_kline.i != 0 => {
                    if last_short_value < last_long_value && short_value >= long_value {
                        last_norm_hold = NormHold::Lo(1.);
                    }
                }
                NormHold::Lo(_) if short_value < long_value => {
                    last_norm_hold = NormHold::No;
                }
                _ => {}
            }
            last_short_value = short_value;
            last_long_value = long_value;
            last_norm_hold.clone()
        })
    }
}

#[ta_derive2]
pub struct TwoMaStraOpen(pub usize, pub usize); 


#[typetag::serde]
impl Cond for TwoMaStraOpen {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let short_ma = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
        let long_ma = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
        Box::new(move |e, _| {
            e > 0 && short_ma[e - 1] < long_ma[e - 1] && short_ma[e] >= long_ma[e]
        })
    }
}

#[ta_derive2]
pub struct TwoMaStraExit(pub usize, pub usize); 


#[typetag::serde]
impl Cond for TwoMaStraExit {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let short_ma = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
        let long_ma = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
        Box::new(move |e, _| {
            short_ma[e] < long_ma[e]
        })
    }
}

#[ta_derive2]
pub struct MaTa(pub usize, pub usize);

#[typetag::serde]
impl Ta for MaTa {
    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        let short_ma = da[0].roll(RollFunc::Mean, RollOps::N(self.0));
        let long_ma = da[0].roll(RollFunc::Mean, RollOps::N(self.1));
        vec![short_ma, long_ma]
    }
}

#[ta_derive2]
pub struct TwoMaStraOpen2(pub usize, pub usize);

#[typetag::serde]
impl Cond for TwoMaStraOpen2 {
    fn calc_da<'a>(&self,_data:avv32,di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(MaTa(self.0, self.1));
        let short_ma = data[0].clone();
        let long_ma = data[1].clone();
        Box::new(move |e, _| {
            e > 0 && short_ma[e - 1] < long_ma[e - 1] && short_ma[e] >= long_ma[e]
        })
    }
}

#[ta_derive2]
pub struct TwoMaStraExit2(pub usize, pub usize);

#[typetag::serde]
impl Cond for TwoMaStraExit2 {
    fn calc_da<'a>(&self,_data:avv32,di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(MaTa(self.0, self.1));
        let short_ma = data[0].clone();
        let long_ma = data[1].clone();
        Box::new(move |e, _| {
            short_ma[e] < long_ma[e]
        })
    }
}

#[typetag::serde]
impl CondType6 for TwoMaStraOpen2 {
    fn cond_type6(&self,_di: &Di) -> RetFnCondType6 {
        let ma_ta = MaTa(self.0, self.1);
        Box::new(move |di_kline_o| {
            let data = di_kline_o.di_kline.di.calc(&ma_ta);
            let e = di_kline_o.di_kline.i;
            e > 0 && data[0][e - 1] < data[1][e - 1] && data[0][e] >= data[1][e]

        })
    }
}

#[typetag::serde]
impl CondType6 for TwoMaStraExit2 {
    fn cond_type6(&self,_di: &Di) -> RetFnCondType6 {
        let ma_ta = MaTa(self.0, self.1);
        Box::new(move |di_kline_o| {
            let data = di_kline_o.di_kline.di.calc(&ma_ta);
            let e = di_kline_o.di_kline.i;
            data[0][e] < data[1][e]
        })
    }
}

#[ta_derive2]
pub struct TwoMaTick;

// #[typetag::serde]
impl CondType7 for TwoMaTick {
    fn cond_type7(&self) -> RetFnCondType7 {
        let mut last_norm_hold = NormHold::No;
        let mut short_ma = SMA::new(1200).unwrap();
        let mut long_ma = SMA::new(2400).unwrap();
        let mut last_short_value = 0f64;
        let mut last_long_value = 0f64;
        Box::new(move |tick_data| {
            let c = tick_data.c as f64;
            let short_value = short_ma.next(c);
            let long_value = long_ma.next(c);
            match last_norm_hold {
                NormHold::No if last_short_value != 0. => {
                    if last_short_value < last_long_value && short_value >= long_value {
                        last_norm_hold = NormHold::Lo(1.);
                    }
                }
                NormHold::Lo(_) if short_value < long_value => {
                    last_norm_hold = NormHold::No;
                }
                _ => {}
            }
            last_short_value = short_value;
            last_long_value = long_value;
            // last_norm_hold.clone()
            todo!();
        })
    }
}

#[ta_derive2]
pub struct TwoMaTickOrderAction;

impl ApiType for TwoMaTickOrderAction {
    fn api_type(&self) -> RetFnApi {
        let mut short_ma = SMA::new(1200).unwrap();
        let mut long_ma = SMA::new(2400).unwrap();
        let mut last_short_value = 0f64;
        let mut last_long_value = 0f64;
        Box::new(move |stream_api| {
            let c = stream_api.tick_data.c as f64;
            let short_value = short_ma.next(c);
            let long_value = long_ma.next(c);
            let hold = stream_api.hold.sum();
            let mut res = OrderAction::No;
            if hold == 0 {
                match last_short_value != 0. && last_short_value < last_long_value && short_value >= long_value {
                    true => {
                        res = OrderAction::LoOpen(1, stream_api.tick_data.bid1);
                    }
                    false => (),
                }
            } else if hold > 0 && short_value < long_value {
                res = OrderAction::ShClose(hold, stream_api.tick_data.ask1);
            }
            last_short_value = short_value;
            last_long_value = long_value;
            res
        })
    }
}

#[typetag::serde]
impl CondType4 for TwoMaTickOrderAction {
    fn cond_type4(&self, _di: &Di) -> RetFnCondType4 {
        let ta_ops = MaTa(10, 20);
        Box::new(move |stream_cond_type1| {
            let ma_ta_res = stream_cond_type1.di_kline_state.di_kline.di.calc(&ta_ops);
            let tick_data = stream_cond_type1.stream_api.tick_data;
            let i = stream_cond_type1.di_kline_state.di_kline.i;
            let hold = stream_cond_type1.stream_api.hold.sum();
            let mut res = OrderAction::No;
            if hold == 0 {
                if tick_data.c > ma_ta_res[0][i] && ma_ta_res[0][i-1] < ma_ta_res[1][i-1] {
                    res = OrderAction::LoOpen(1, tick_data.bid1);
                }
            } else if hold > 0 && tick_data.c < ma_ta_res[0][i] {
                res = OrderAction::ShClose(hold, tick_data.ask1);
            }
            res
        })
    }
}
