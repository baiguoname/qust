use std::collections::HashSet;

use burn::backend::ndarray::NdArrayDevice;
use burn::backend::{Autodiff, NdArray};
use burn::prelude::{Module, Tensor};
use chrono::Timelike;
use qust_ds::prelude::*;
use qust::prelude::*;
use qust_io::prelude::GenDi;
use super::utils::*;


use super::run::get_model;

pub const NUM_FACTORS: usize = 8;
pub const NUM_TICKERS: usize = 6;
pub const NUM_FEATURES: usize = NUM_FACTORS * NUM_TICKERS;
pub const Y_EXIT: usize = 100;

pub enum GetTrainData {
    Binary,
    Contin,
}

impl GetTrainData {
    pub fn get_train_data(&self, price_tick: &hm<Ticker, PriceTick>, tickers: &[Ticker]) -> (vv32, v32, vdt) {
        let v_t = tickers
            .iter()
            .map(|x| price_tick[x].t.modify())
            .collect_vec()
            .union_vecs();
        let p0 = &price_tick[&tickers[0]];
        let (x, y) = self.get_xy(p0, tickers[0]);
        let ri = Reindex::new(&p0.t.modify(), &v_t);
        let mut x_data = ri.reindex(&x).ffill(vec![0.; x[0].len()]);
        let y = ri.reindex(&y).ffill(0.);
        tickers.iter().skip(1).for_each(|x| {
            let p0 = &price_tick[x];
            let (xx, _) = self.get_xy(p0, *x);
            let ri_part = Reindex::new(&p0.t.modify(), &v_t);
            let mut xx = ri_part.reindex(&xx).ffill(vec![0.; xx[0].len()]);
            x_data.iter_mut().zip(xx.iter_mut())
                .for_each(|(z, zz)| {
                    z.append(zz);
                });
        });
        (x_data, y, v_t)
    }

    pub fn get_y_part(&self, data: &[f32], tz: f32) -> f32 {
        match self {
            GetTrainData::Binary => {
                let d0 = data[0];
                for d in data {
                    if (d - d0) / tz <= -2. {
                        return 0.
                    } else if (d - d0) / tz >= 2. {
                        return 2.
                    }
                }
                1.
            }
            GetTrainData::Contin => {
                let mut res = 0f32;
                let d0 = data[0];
                for d in data {
                    let k = (d - d0) / tz;
                    if k.abs() > res.abs() {
                        res = k;
                    }
                }
                res
            }
        }
    }
    pub fn get_xy(&self, price_tick: &PriceTick, ticker: Ticker) -> (vv32, v32) {
        let c = &price_tick.c;
        let tz = ticker.info().tz;
        let c0 = c.iter().zip(c.lag(1.).iter()).map(|(x, y)| (x - y) / tz).collect_vec();
        let s = price_tick.ask1.iter().zip(price_tick.bid1.iter())
            .map(|(x, y)| {
                (x - y) / tz
            })
            .collect_vec();
        fn dddd(data: &[f32], n: usize) -> v32 {
            let mut x1 = data.roll(RollFunc::Sum, RollOps::N(n));
            x1.fillna(0.);
            x1
        }
        let x1 = dddd(&c0, 30);
        let x2 = dddd(&c0, 60);
        let x3 = dddd(&c0, 100);
        let x4 = dddd(&c0, 200);

        let k1 = dddd(&s, 30);
        let k2 = dddd(&s, 60);
        let k3 = dddd(&s, 100);
        let k4 = dddd(&s, 200);
        
        let k_len = x1.len();
        let x = vcat(vec![x1, x2, x3, x4, k1, k2, k3, k4]);
        // let x = vcat(vec![x3, k3]);
        let mut y = c
            .windows(Y_EXIT)
            .map(|x| self.get_y_part(x, tz))
            .collect_vec();
        y.resize(k_len, 0.);
        // let x =x.into_iter().zip(y.iter()).map(|(x, y)| {
        //     let mut x = x;
        //     x.push(*y);
        //     x
        // })
        // .collect_vec();
        // let x = y.iter().map(|x| vec![*x]).collect_vec();
        (x, y)
    }

    pub fn get_xy_data(&self, tickers: Vec<Ticker>, range: ForCompare<dt>) -> (vv32, v32, vdt) {
        let gen_di = GenDi("/root/qust/data");
        let price_tick_hm = gen_di.getl_tick(&tickers, range);
        let (x, y, time_vec) = self.get_train_data(&price_tick_hm, &tickers);
        let time_range1 = 90100.to_tt() .. 112900.to_tt();
        let time_range2 = 133100.to_tt() .. 145900.to_tt();
        let time_range3 = 210100.to_tt() .. 225900.to_tt();
        let z_cut = time_vec
            .clone()
            .into_iter()
            .filter(|x| {
                let x_t = x.time();
                time_range1.contains(&x_t) || time_range2.contains(&x_t) || time_range3.contains(&x_t)
            })
            .collect_vec();
        let ri = Reindex::new(&time_vec, &z_cut);
        let x = ri.reindex(&x).into_iter().flatten().collect_vec();
        let y = ri.reindex(&y).into_iter().flatten().collect_vec();

        (x, y, z_cut)
    }
}

pub struct cond(pub hm<dt, f32>);

impl ApiType for cond {
    fn api_type(&self) -> RetFnApi {
        let mut order_hm = self.0.clone();
        let mut open_counts = 0;
        let mut exit_target = 0.;
        let mut last_tick_time = dt::default();
        let mut last_target = 0;
        let mut c = 0.;
        Box::new(move |stream| {
            let tick_data = stream.tick_data;
            let hold = stream.hold;
            let hold_sum = hold.sum();
            if tick_data.t > last_tick_time {
                last_tick_time = tick_data.t;
                if hold_sum == 0. {
                    match order_hm.remove(&tick_data.t.modify()) {
                        Some(exit_thre) => {
                            if exit_thre > 0. {
                                open_counts = 0;
                                last_target = 1;
                                exit_target = exit_thre;
                                c = tick_data.c;
                                OrderAction::LoOpen(1., tick_data.ask1)
                            } else if exit_thre < 0. {
                                open_counts = 0;
                                last_target = -1;
                                exit_target = exit_thre;
                                c = tick_data.c;
                                OrderAction::ShOpen(1., tick_data.bid1)
                            } else {
                                OrderAction::No
                            }
                        }
                        None => {
                            open_counts += 1;
                            if last_target == 0 {
                                OrderAction::No
                            } else if last_target > 0 {
                                OrderAction::LoOpen(1., tick_data.ask1)
                            } else {
                                OrderAction::ShOpen(1., tick_data.bid1)
                            }
                        }
                    }
                } else if hold_sum > 0. {
                    open_counts += 1;
                    if tick_data.c - c >= exit_target || open_counts >= Y_EXIT {
                        last_target = 0;
                        OrderAction::ShClose(hold_sum, tick_data.bid1)
                    } else {
                        OrderAction::No
                    }
                } else {
                    open_counts += 1;
                    if tick_data.c - c <= exit_target || open_counts >= Y_EXIT {
                        last_target = 0;
                        OrderAction::LoClose(-hold_sum, tick_data.ask1)
                    } else {
                        OrderAction::No
                    }
                }
            } else {
                if hold_sum == 0. {
                    if last_target == 0 {
                        OrderAction::No
                    } else if last_target > 0 {
                        OrderAction::LoOpen(1., tick_data.ask1)
                    } else {
                        OrderAction::ShOpen(1., tick_data.bid1)
                    }
                } else if hold_sum > 0. {
                    if last_target == 0 {
                        OrderAction::ShClose(1., tick_data.bid1)
                    } else if last_target > 0 {
                        OrderAction::No
                    } else {
                        OrderAction::ShClose(1., tick_data.bid1)
                    }
                } else {
                    if last_target == 0 {
                        OrderAction::LoClose(1., tick_data.ask1)
                    } else if last_target > 0 {
                        OrderAction::LoClose(1., tick_data.ask1)
                    } else {
                        OrderAction::No
                    }
                }
            }
        })
    }
}


struct DataHis {
    tzv: v32,
    last_c: v32,
    his: vv32,
    device: NdArrayDevice,
}

impl DataHis {
    fn from_tickers(tickers: &[Ticker]) -> Self {
        Self {
            tzv: tickers.map(|x| x.info().tz),
            last_c: vec![0.; tickers.len()],
            his: vec![],
            device: NdArrayDevice::Cpu,
        }
        
    }
    fn update_then_get(&mut self, data: &[&TickData]) -> Option<Tensor<Autodiff<NdArray>, 2>> {
        let last_c = data.map(|x| x.c);
        if self.his.is_empty() {
            self.last_c = last_c.clone();
        }
        let his_part = data
            .iter()
            .zip(self.last_c.iter())
            .zip(self.tzv.iter())
            .map(|((x, y), z)| (x.c - y) / z)
            .collect_vec();
        self.last_c = last_c;
        self.his.push(his_part);
        if self.his.len() < 50 {
            return None;
        }
        while self.his.len() > 50 {
            self.his.remove(0);
        }
        let input_data: [f32; NUM_FEATURES] = self
            .his
            .iter()
            .fold(repeat_to_vec(|| Vec::with_capacity(50), data.len()), |mut accu, x| {
                accu.iter_mut().zip(x.iter())
                    .for_each(|(x, y)| {
                        x.push(*y);
                    });
                accu
            })
            .concat()
            .try_into()
            .unwrap();
        let input_tensor = Tensor::<Autodiff<NdArray>, 1>::from_floats(input_data, &self.device);
        let inputs = vec![input_tensor.unsqueeze()];
        Some(Tensor::cat(inputs, 0))
    }
}

pub struct cond2(pub Vec<Ticker>); 

impl GetTickerVec for cond2 {
    fn get_ticker_vec(&self) -> Vec<Ticker> {
        self.0.clone()
    }
}

impl CondCrossTarget for cond2 {

    fn cond_cross_target(&self) -> RetFnCrossTarget {
        let mut data_his = DataHis::from_tickers(&self.0);
        let model = get_model(&data_his.device);
        let mut counts = 0;
        let mut last_target = vec![OrderTarget::No; 3];
        let mut last_c = 0.;
        let mut exit_thre = 0.;
        let tz = self.0[0].info().tz;
        let mut debug_res = vec![];
        Box::new(move |stream| {
            let Some(input) = data_his.update_then_get(&stream) else {
                return last_target.clone();
            };
            let predicted_res = model.forward(input.clone());
            let posi = predicted_res
                .inner()
                .into_primitive()
                .tensor()
                .array
                .into_iter()
                .collect_vec()[0];
            debug_res.push((
                input.inner().into_primitive().tensor().array.into_iter().collect_vec(),
                posi,
                stream[0].t,
            ));
            // if debug_res.len() % 1000 == 0 {
            //     debug_res.sof("live_debug", ".");
            // }
            loge!("ctp", "{:?} {:?}", posi, stream);
            counts += 1;
            if posi >= 3. {
                counts = 0;
                last_target[0] = OrderTarget::Lo(1.);
                exit_thre = posi;
                last_c = stream[0].c;
            } else if posi <= -3. {
                counts = 0;
                last_target[0] = OrderTarget::Sh(1.);
                exit_thre = posi;
                last_c = stream[0].c;
            }
            match &last_target[0] {
                OrderTarget::No => {}
                OrderTarget::Lo(_) => {
                    if counts > Y_EXIT || (stream[0].c - last_c) / tz >= exit_thre {
                        last_target[0] = OrderTarget::No;
                    }
                }
                OrderTarget::Sh(_) => {
                    if counts > Y_EXIT || (stream[0].c - last_c) / tz <= exit_thre {
                        last_target[0] = OrderTarget::No;
                    }
                }
            }
            last_target.clone()
        })
    }
}


pub struct MillisAlignment(pub usize);

impl UpdatedPool for MillisAlignment {
    fn updated_pool(&self) -> FnMutBox<UpdatedTickDataIndex, Option<Vec<TickData>>> {
        let mut tick_time_last = dt::default().modify();
        let mut tick_cache = repeat_to_vec(|| Option::<TickData>::None, self.0);
        let mut cached_set: HashSet<usize> = HashSet::with_capacity(self.0); 
        let mut tick_vec_last = repeat_to_vec(TickData::default, self.0);
        Box::new(move |updated_tick_data_index| {
            let index = updated_tick_data_index.index;
            let tick_data = updated_tick_data_index.data;
            let tick_time_now = tick_data.t.modify();
            let res_ffill = if tick_time_now > tick_time_last && !tick_cache.check_all(|x| x.is_none()) {
                cached_set.clear();
                tick_cache
                    .iter_mut()
                    .zip(tick_vec_last.iter())
                    .map(|(x, y)| x.take().unwrap_or_else(|| y.clone()))
                    .collect_vec()
                    .pip(Some)
            } else {
                None
            };
            tick_vec_last[index] = tick_data.clone();
            tick_cache[index] = Some(tick_data);
            cached_set.insert(index);
            tick_time_last = tick_time_now;
            match res_ffill {
                Some(data) => Some(data),
                None if cached_set.len() == self.0 => {
                    cached_set.clear();
                    tick_cache
                        .iter_mut()
                        .map(|x| x.take().unwrap())
                        .collect_vec()
                        .pip(Some)
                }
                _ =>  None,
            }
        })
    }
}

