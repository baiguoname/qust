use std::cell::RefCell;
use serde::{ Serialize, Deserialize };
use qust::prelude::*;
use qust_derive::*;


/* #region ThreType */
#[ta_derive]
pub enum ThreType {
    Num(f32),
    Percent(f32),
}

impl ThreType {
    pub fn get_thre_vec(&self, di: &Di) -> v32 {
        match self {
            ThreType::Num(i) => vec![*i; di.size()],
            ThreType::Percent(i) => {
                di.calc(vol_pms.clone())[0]
                    .iter()
                    .map(|x| x * i)
                    .collect_vec()
            }
        }
    }
}

impl From<f32> for ThreType {
    fn from(value: f32) -> Self {
        ThreType::Num(value)
    }
}
/* #endregion */

#[ta_derive]
pub struct cond_out(pub Dire, pub f32);

#[typetag::serde(name = "cond_out")]
impl Cond for cond_out {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let o = di.o();
        let h = di.h();
        let l = di.l();
        let c = di.c();
        let day_range = izip!(o.iter(), c.iter())
            .map(|(x, y)| (x + y) / 2f32)
            .collect_vec();
        match self.0 {
            Sh => Box::new({
                let stopl = RefCell::new(
                    l
                        .iter()
                        .map(|x| x * (1f32 - self.1 / 1000f32))
                        .collect_vec()
                );
                move |e, _o| {
                    if e > _o && c[e] > c[e - 1] && day_range[e] > day_range[e - 1] {
                        stopl.borrow_mut()[_o] += (c[e] - c[e - 1]) / 2f32;
                    }
                    // println!("{}, {} {} {} {} {}", e, _o, l[e], stopl.borrow()[_o], c[e], o[e]);
                    l[e] < stopl.borrow()[_o] && c[e] > o[e]
                }
            }),
            Lo => Box::new({
                let stops = RefCell::new(
                    h
                        .iter()
                        .map(|x| x * (1f32 + self.1 / 1000f32))
                        .collect_vec()
                );
                move |e, _o| {
                    if e > _o && c[e] < c[e - 1] && day_range[e] < day_range[e - 1] {
                        stops.borrow_mut()[_o] += (c[e] - c[e - 1]) / 2f32;
                    }
                    h[e] > stops.borrow()[_o] && c[e] < o[e]
                }
            }),
        }
    }
}

///stop profit, sh: stop long position profit, price go up </br>
/// stop loss, lo: stop long position loss, price go down
#[ta_derive]
pub struct stop_cond(pub Dire, pub ThreType);

#[typetag::serde(name = "stop_cond")]
impl Cond for stop_cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let thre = self.1.get_thre_vec(di);
        match self.0 {
            Sh => Box::new(move |e, o| c[e] / c[o] - 1.0 > thre[e]),
            Lo => Box::new(move |e, o| c[e] / c[o] - 1.0 < -thre[e]),
        }
    }
}

#[ta_derive]
pub struct exit_by_k_num(pub usize);

#[typetag::serde(name = "exit_by_k_num")]
impl Cond for exit_by_k_num {
    fn cond<'a>(&self, _di: &'a Di) -> LoopSig<'a> {
        let thre = self.0;
        Box::new(move |e, o| e - o >= thre)
    }
}

/* #region now - highest */
#[ta_derive]
pub struct cond_out3(pub Dire, pub ThreType);

#[typetag::serde(name = "cond_out3")]
impl Cond for cond_out3 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let thre_vec = self.1.get_thre_vec(di);
        match self.0 {
            Sh => Box::new({
                let h = di.h();
                let last_h = RefCell::new(h.to_vec());
                move |e, o| {
                    if e > o && h[e] > h[e - 1] {
                        last_h.borrow_mut()[o] = h[e];
                    }
                    thre_vec[o].is_nan() || c[e] / last_h.borrow()[o] < 1f32 - thre_vec[o]
                }
            }),
            Lo => Box::new({
                let l = di.l();
                let last_l = RefCell::new(l.to_vec());
                move |e, o| {
                    if e > o && l[e] < l[e - 1] {
                        last_l.borrow_mut()[o] = l[e];
                    }
                    thre_vec[o].is_nan() || c[e] / last_l.borrow()[o] > 1f32 + thre_vec[o]
                }
            }),
        }
    }
}
/* #endregion */

///Sh: a = 开仓以来最高价 - 当前开盘价 * 0.01, 当前最低价小于a </br>
///Lo: a = 开仓以来最低价 + 当前开盘价 * 0.01，当前最高价大于a
#[ta_derive]
pub struct price_sby(pub Dire, pub f32);

// #[cfg(not(feature = "live"))]
// #[typetag::serde(name = "price_sy")]
// impl Cond for price_sby {
//     fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
//         let (ovec, h, l, c) = (di.o(), di.h(), di.l(), di.c());
//         let mut last_c = c.to_vec();
//         let multi = self.1;
//         match self.0 {
//             Lo => Box::new(move |e, o| {
//                 if e > o && h[e] > h[e - 1] { 
//                     last_c[o] = h[e];
//                 }
//                 l[e] <= last_c[o] - ovec[e] * multi
//             }),
//             Sh => Box::new(move |e, o| {
//                 if e > o && l[e] < l[e - 1] {
//                     last_c[o] = l[e];
//                 }
//                 h[e] > last_c[o] + ovec[e] * multi
//             }),
//         }
//     }
// }

// #[cfg(feature = "live")]
#[typetag::serde(name = "price_sy")]
impl Cond for price_sby {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let (ovec, h, l, c) = (di.o(), di.h(), di.l(), di.c());
        let multi = self.1;
        match self.0 {
            Lo => Box::new(move |e, o| {
                let mut last_c = c[o];
                if e > o {
                    for i in o+1..=e {
                        if h[i] > h[i - 1] {
                            last_c = h[i];
                        }
                    }
                }
                l[e] < last_c - ovec[e] * multi
            }),
            Sh => Box::new(move |e, o| {
                let mut last_c = c[o];
                if e > o {
                    for i in o+1..=e {
                        if l[i] < l[i - 1] {
                            last_c = l[i];
                        }
                    }
                }
                h[e] > last_c + ovec[e] * multi
            }),
        }
    }
}

///5m, is stop profit </br>
///30m, is stop loss
pub const price_sby_for_5min: (price_sby, price_sby) = (price_sby(Lo, 0.008), price_sby(Sh, 0.008));
pub const price_sby_for_30min: (price_sby, price_sby) = (price_sby(Sh, 0.015), price_sby(Lo, 0.015));


#[ta_derive]
pub struct exit_by_price(pub Dire, pub f32);


#[typetag::serde(name = "exit_by_price")]
impl Cond for exit_by_price {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let thre = self.1;
        match self.0 {
            Sh => Box::new(move |e, o| {
                c[e] - c[o] <= -thre
            }),
            Lo => Box::new(move |e, o| {
                c[e] - c[o] >= thre
            }),
        }
    }
}

#[ta_derive]
pub struct exit_by_tick_size(pub Dire, pub f32);

#[typetag::serde(name = "exit_by_tick_size")]
impl Cond for exit_by_tick_size {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let tz = di.pcon.ticker.info().tz;
        let thre = self.1 * tz;
        match self.0 {
            Sh => Box::new(move |e, o| {
                c[e] - c[o] <= -thre
            }),
            Lo => Box::new(move |e, o| {
                c[e] - c[o] >= thre
            }),
        }
    }
}