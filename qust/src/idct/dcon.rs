use crate::std_prelude::*;
use crate::trade::di::{Di, PriceArc, PriceOri, ToArc};
use crate::trade::inter::{KlineData, KlineState, Pri};
use qust_derive::AsRef;
use qust_ds::prelude::*;

// #[ta_derive]
#[derive(Clone, Serialize, Deserialize, AsRef)]
// #[serde(from = "Ttt")]
pub enum Convert {
    Tf(usize, usize),
    Ha(usize),
    Event(Box<dyn Pri>),
    PreNow(Box<Convert>, Box<Convert>),
    VolFilter(usize, usize),
    Log,
    FlatTick,
}
impl PartialEq for Convert {
    fn eq(&self, other: &Self) -> bool {
        self.debug_string() == other.debug_string()
    }
}
impl Debug for Convert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Convert::*;
        let show_str = match self {
            Tf(_, _) => "ori".into(),
            Ha(u) => format!("Ha({})", u),
            PreNow(pre, now) => format!("{:?} : {:?}", pre, now),
            Event(t) => format!("{:?}", t),
            VolFilter(a, b) => format!("VolFilter({}, {})", a, b),
            Log => "Log".into(),
            FlatTick => "FlatTick".into(),
        };
        f.write_str(&show_str)
    }
}

use Convert::*;

impl Convert {
    pub fn get_pre(&self, di: &Di) -> PriceArc {
        match self {
            PreNow(pre, _now) => di.calc(&**pre),
            _ => {
                let mut price_res = di.pcon.price.clone().to_arc();
                price_res.finished = None;
                price_res
            }
        }
    }

    pub fn convert(&self, price: PriceArc, di: &Di) -> PriceArc {
        match self {
            Tf(_start, _end) => price,
            Ha(w) => {
                let close_price = izip!(
                    price.o.iter(),
                    price.h.iter(),
                    price.l.iter(),
                    price.c.iter(),
                )
                .map(|(o, h, l, c)| (o + h + l + c) / 4.0);
                let open_price = price.c.ema(*w);
                let mut open_price = open_price.lag(1);
                open_price[0] = open_price[1];
                let high_price: Vec<f32> =
                    izip!(price.h.iter(), open_price.iter(), close_price.clone())
                        .map(|(a, b, c)| a.max(*b).max(c))
                        .collect();
                let low_price: Vec<f32> =
                    izip!(price.l.iter(), open_price.iter(), close_price.clone())
                        .map(|(a, b, c)| a.min(*b).min(c))
                        .collect();
                PriceArc {
                    t: price.t.clone(),
                    o: Arc::new(open_price),
                    h: Arc::new(high_price),
                    l: Arc::new(low_price),
                    c: Arc::new(close_price.collect()),
                    v: price.v.clone(),
                    ki: price.ki.clone(),
                    finished: None,
                    immut_info: price.immut_info.clone(),
                }
            }
            PreNow(_pre, now) => now.convert(price, di),
            Event(tri) => {
                let mut price_res = PriceOri::with_capacity(price.o.len());
                let mut finished_vec = Vec::with_capacity(price_res.o.capacity());
                let mut f = tri.update_kline_func(di, &price);
                for (i, (&t, &o, &h, &l, &c, &v, ki)) in izip!(
                    price.t.iter(),
                    price.o.iter(),
                    price.h.iter(),
                    price.l.iter(),
                    price.c.iter(),
                    price.v.iter(),
                    price.ki.iter(),
                )
                .enumerate()
                {
                    let kline_data = KlineData { t, o, h, l, c, v, ki: ki.clone() };
                    let finished = f(&kline_data, &mut price_res, i);
                    finished_vec.push(finished);
                }
                (price_res, Some(finished_vec)).to_arc()
            }
            VolFilter(window, percent) => {
                let (finished_vec, mask_len) = price.v.rolling(*window).fold(
                    (Vec::with_capacity(price.v.len()), 0usize),
                    |mut accu, x| {
                        let m = x.last().unwrap() >= &x.quantile(*percent as f32 / 100f32);
                        if m {
                            accu.1 += 1;
                        }
                        let state: KlineState = m.into();
                        accu.0.push(state);
                        accu
                    },
                );
                let mut price_res = PriceOri::with_capacity(mask_len);
                izip!(
                    finished_vec.iter(),
                    price.t.iter(),
                    price.o.iter(),
                    price.h.iter(),
                    price.l.iter(),
                    price.c.iter(),
                    price.v.iter()
                )
                .for_each(|(i, t, o, h, l, c, v)| {
                    if let KlineState::Finished = i {
                        price_res.t.push(*t);
                        price_res.o.push(*o);
                        price_res.h.push(*h);
                        price_res.l.push(*l);
                        price_res.c.push(*c);
                        price_res.v.push(*v);
                    }
                });
                (price_res, Some(finished_vec)).to_arc()
            }
            Log => {
                let numerator = price.l.min();
                PriceArc {
                    t: price.t.clone(),
                    o: price.o.map(|x| x / numerator).to_arc(),
                    h: price.h.map(|x| x / numerator).to_arc(),
                    l: price.l.map(|x| x / numerator).to_arc(),
                    c: price.c.map(|x| x / numerator).to_arc(),
                    v: price.v.clone(),
                    ki: price.ki.clone(),
                    finished: None,
                    immut_info: price.immut_info.clone(),
                }
            }
            FlatTick => {
                let mut res = Vec::with_capacity(price.l.len());
                let c_vec_ori = &price.c;
                res.push(c_vec_ori[0]);
                for i in 2..c_vec_ori.len() {
                    if c_vec_ori[i - 2] == c_vec_ori[i] && c_vec_ori[i - 1] != c_vec_ori[i] {
                        res.push(c_vec_ori[i - 2]);
                    } else {
                        res.push(c_vec_ori[i - 1]);
                    }
                }
                res.push(*c_vec_ori.last().unwrap());
                let res = Arc::new(res);
                PriceArc {
                    t: price.t.clone(),
                    o: res.clone(),
                    h: res.clone(),
                    l: res.clone(),
                    c: res,
                    v: price.v.clone(),
                    ki: price.ki.clone(),
                    finished: None,
                    immut_info: price.immut_info.clone(),
                }
            }
        }
    }
}

pub trait VertBack {
    fn vert_back(&self, di: &Di, res: Vec<&[f32]>) -> Option<vv32>;
}

impl VertBack for Convert {
    fn vert_back(&self, di: &Di, res: Vec<&[f32]>) -> Option<vv32> {
        match self {
            PreNow(pre, now) => {
                let price_now = di.calc(self);
                let res_pre = match &price_now.finished {
                    None => res.map(|x| x.to_vec()),
                    Some(finished_vec) => res
                        .iter()
                        .map(|x| {
                            let mut res_pre = vec![f32::NAN; finished_vec.len()];
                            let mut v_iter = x.iter();
                            finished_vec.iter().enumerate().for_each(|(i, x)| {
                                if x.into() {
                                    res_pre[i] = *v_iter.next().unwrap();
                                }
                            });
                            res_pre
                        })
                        .collect_vec(),
                };
                match (*pre.clone(), *now.clone()) {
                    (Tf(_, _), _) => Some(res_pre),
                    (pre_n, _) => pre_n.vert_back(di, res_pre.iter().map(|x| &x[..]).collect_vec()),
                }
            }
            _ => {
                let price_now = di.calc(self);
                match &price_now.finished {
                    None => res.map(|x| x.to_vec()).into(),
                    Some(finished_vec) => res
                        .iter()
                        .map(|x| {
                            let mut res_pre = vec![f32::NAN; finished_vec.len()];
                            let mut v_iter = x.iter();
                            finished_vec.iter().enumerate().for_each(|(i, x)| {
                                if x.into() {
                                    res_pre[i] = *v_iter.next().unwrap();
                                }
                            });
                            res_pre
                        })
                        .collect_vec()
                        .into(),
                }
            }
        }
    }
}

// impl VertBack for (Convert, Convert) {
//     fn vert_back(&self, di: &Di, res: Vec<&[f32]>) -> Option<vv32> {
//         match self {
//             (Convert::PreNow(pre, _), Tf(_, _)) => {
//                 let data_pre = self.0.vert_back(di, res)?;
//                 (*pre.clone(), self.1.clone()).vert_back(di, data_pre.iter().map(|x| &x[..]).collect_vec())
//             }
//             (pre, Tf(_, _)) => {
//                 (self.1.clone() + pre.clone()).vert_back(di, res)
//             }
//         }
//     }
// }
