#![allow(unused_imports, clippy::collapsible_else_if, unused_assignments)]
use itertools::izip;
use qust::prelude::*;
use qust_derive::*;

use crate::prelude::{exit_by_tick_size, rl5mall, AttachCond, AttachConds};

#[ta_derive2]
pub struct ta(pub f32, pub PriBox);

#[typetag::serde(name = "c71_ta_inter")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![
            di.calc(ShiftInter { inter: self.1.clone(), n: 0, kline: KlineType::Open, index_spec: IndexSpec::First})[0].clone(),
            di.calc(ShiftInter { inter: self.1.clone(), n: 1, kline: KlineType::High, index_spec: IndexSpec::Max})[0].clone(),
            di.calc(ShiftInter { inter: self.1.clone(), n: 1, kline: KlineType::Low, index_spec: IndexSpec::Min})[0].clone(),
            di.c(),
        ]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let h_l_pre = izip!(da[1].iter(), da[2].iter())
            .map(|(&x, &y)| self.0 * (x - y))
            .collect::<v32>();
        let dnband = izip!(da[0].iter(), h_l_pre.iter())
            .map(|(&x, y)| x - y)
            .collect();
        let upband = izip!(da[0].iter(), h_l_pre.iter())
            .map(|(&x, y)| x + y)
            .collect();
        vec![dnband, da[3].to_vec(), upband]
    }
}

pub trait ChangeFrame<T> {
    fn change_frame(self, input: T) -> Self;
}

impl ChangeFrame<Ptm> for Stral {
    fn change_frame(self, input: Ptm) -> Self {
        self.0.into_iter()
            .map(|x| x.change_frame(input.clone()))
            .collect_vec()
            .pip(Stral)
    }
}

impl ChangeFrame<TriBox> for Stral {
    fn change_frame(self, input: TriBox) -> Self {
        self.0.into_iter()
            .map(|x| Stra{
                ident: PconIdent { inter: input.clone(), ticker: x.ident.ticker},
                name: x.name,
                ptm: x.ptm,
            })
            .collect_vec()
            .pip(Stral)
    }
}

impl ChangeFrame<Ptm> for Stra {
    fn change_frame(self, input: Ptm) -> Self {
        Stra {
            ident: self.ident,
            name: self.name,
            ptm: self.ptm.change_frame(input)
        }
    }
}

impl ChangeFrame<Ptm> for Ptm {
    fn change_frame(self, input: Ptm) -> Self {
        match self {
            Ptm::Ptm2(f, stp1, stp2) => {
                if let Ptm::Ptm2(_, stp3, stp4) = input {
                    Ptm::Ptm2(f, stp1.change_frame(stp3), stp2.change_frame(stp4))
                } else {
                    panic!("")
                }
            }
            _ => panic!(),
        }
    }
}

impl ChangeFrame<Stp> for Stp {
    fn change_frame(self, input: Stp) -> Self {
        match self {
            Stp::StpWeight(stp_box, cond_weight) => {
                let res = (*stp_box).change_frame(input);
                Stp::StpWeight(Box::new(res), cond_weight)
            }
            Stp::Stp(Tsig::TsigFilter(_tsig, io_cond)) => {
                if let Stp::Stp(Tsig::Tsig(a, b, c, d)) = input {
                    Stp::Stp(Tsig::TsigFilter(Box::new(Tsig::Tsig(a, b, c, d)), io_cond))
                }  else {
                    panic!("")
                }
            }
            _ => panic!("")
        }
    }
}

pub fn get_ptm() -> Vec<Ptm> {
    use super::para::*;
    let ptm_stra = ptm.clone();
    let pre = ori + Event(rl30mday.pri_box()) + oos;
    (ta_box_vec.clone(), [range_vec.clone(), range_vec2.clone()].concat())
        .product(|(ta_box, rangei)| {
            let pms = pre.clone() + ta_box + rank_day + vori;
            let io_conds = (
                Iocond { pms: pms.clone(), range:rangei.0 },
                Iocond { pms, range: rangei.1 }
            );
            ptm_stra.attach_conds(LogicOps::And, &io_conds, Lo)
        })
}


lazy_static! {
    pub static ref PMS1: Pms = ori + oos + ta(0.2, rl30mday.pri_box());
    pub static ref PMS2: Pms = ori + Event(rl30mday.pri_box()) + oos + EffRatio(2, 10) + vori;
    pub static ref COND_L1: BandCond<Pms> = BandCond(Lo, BandState::Action, PMS1.clone());
    pub static ref COND_S1: BandCond<Pms> = BandCond(Sh, BandState::Action, PMS1.clone());
    pub static ref COND_L2: BandCond<Pms> = BandCond(Lo, BandState::Lieing, PMS2.clone());
    pub static ref COND_S2: BandCond<Pms> = BandCond(Sh, BandState::Lieing, PMS2.clone());
    pub static ref ptm: Ptm = {
        let msig_oc_l = qust::msig!(and, COND_L1.clone(), COND_L2.clone());
        let msig_ec_s = qust::msig!(or, COND_S1.clone(), Filterday);
        let msig_oc_s = qust::msig!(and, COND_S1.clone(), COND_S2.clone());
        let msig_ec_l = qust::msig!(or, COND_L1.clone(), Filterday);

        let tsig_l = Tsig::new(Sh, &msig_oc_l, &msig_ec_s);
        let tsig_s = Tsig::new(Lo, &msig_oc_s, &msig_ec_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M2(10000.)), stp_l, stp_s)
        // Ptm::Ptm2(Box::new(M2(10000.)), stp_s, stp_l)
    };
}


// #[ta_derive2]
// pub struct cond1 {
//     sh_out: CondBox,
//     lo_out: CondBox,
// }
// fn get_exceed_percent(c: f32, data: &[av32], i: usize) -> f32 {
//     let sh_price = data[0][i];
//     let lo_price = data[2][i];
//     let gap = lo_price - sh_price;
//     let exceed = c - sh_price;
//     exceed / gap
// }


// #[typetag::serde(name = "c71inter_cond1")]
// impl CondType0 for cond1 {
//     fn cond(&self) -> CondOpsRetFnType {
//         let mut last_ret = LiveTarget::No;
//         Box::new(move |&StreamCondType0 { tick_data, di_index, di, hold, .. }| {
//             // let ticker = di.pcon.ticker;
//             // let c = tick_data.c;
//             // let ta_res = di.calc(PMS1.clone());
//             // let sh_price = ta_res[0][i];
//             // let lo_price = ta_res[2][i];
//             // let exceed_percent = get_exceed_percent(c, &ta_res, i);
//             // let sh_price_round = sh_price.round_ticker_price(ticker);
//             // let lo_price_round = lo_price.round_ticker_price(ticker);
//             // let ticker_gap = ticker.info().tz * 4.;
//             // last_ret = if hold == 0. {
//             //     if exceed_percent > 0.6 && cexceed_percent < 1. {
//             //         LiveTarget::OrderAction(OrderAction::ShOpen(1, lo_price_round))
//             //     }
//             // }
//             todo!();
//             // let ticker = di.pcon.ticker;
//             // let c = tick_data.c;
//             // let ta_res = di.calc(PMS1.clone());
//             // let sh_price = ta_res[0][i];
//             // let lo_price = ta_res[2][i];
//             // let exceed_percent = get_exceed_percent(c, &ta_res, i);
//             // let sh_price_round = sh_price.round_ticker_price(ticker);
//             // let lo_price_round = lo_price.round_ticker_price(ticker);
//             // let ticker_gap = ticker.info().tz * 4.;
//             // last_ret = if hold == 0. {
//             //     if exceed_percent > 0.6 && exceed_percent < 1. {
//             //         TargetAndPrice { n: -1., price: lo_price_round }.into()
//             //     } else if exceed_percent < 0.4 && exceed_percent > 0. {
//             //         TargetAndPrice { n: 1., price: sh_price_round }.into()
//             //     } else {
//             //         LiveTarget::No
//             //     }
//             // } else if hold > 0. {
//             //     if c <= sh_price_round - ticker_gap {
//             //         TargetAndPrice { n: 0., price: lo_price_round }.into()
//             //     } else {
//             //         TargetAndPrice { n: 0., price: tick_data.ask1 }.into()
//             //     }
//             // }  else {
//             //     if c >= lo_price_round + ticker_gap {
//             //         TargetAndPrice { n: 0., price: tick_data.ask1 }.into()
//             //     } else {
//             //         TargetAndPrice { n: 0., price: sh_price_round }.into()
//             //     }
//             // };
//             // last_ret.clone()
//         })
//     }
// }



// pub trait RoundTickerPrice {
//     fn round_ticker_price(self, ticker: Ticker) -> Self;
// }

// impl RoundTickerPrice for f32 {
//     fn round_ticker_price(self, ticker: Ticker) -> Self {
//         let tz = ticker.info().tz as i32;
//         let tz = tz.max(1);
//         ((self as i32 / tz) * tz) as f32
//     }
// }

// #[ta_derive2]
// pub struct cond2;

// #[typetag::serde(name = "c71inter_cond2")]
// impl CondType0 for cond2 {
//     fn cond(&self) -> CondOpsRetFnType {
//         // let mut last_ret = LiveTarget::TargetAndPrice(TargetAndPrice::QuickOut);
//         let mut last_ret = LiveTarget::OrderAction(OrderAction::No);
//         Box::new(move |&StreamCondType0{ tick_data, di_index, di, hold, .. }| {
//             // todo!();
//             let i = di_index;
//             let ticker = di.pcon.ticker;
//             let c = tick_data.c;
//             let ta_res = di.calc(PMS1.clone());
//             let sh_price = ta_res[0][i];
//             let lo_price = ta_res[2][i];
//             let exceed_percent = get_exceed_percent(c, &ta_res, i);
//             let sh_price_round = sh_price.round_ticker_price(ticker);
//             let lo_price_round = lo_price.round_ticker_price(ticker);
//             let ticker_gap = ticker.info().tz * 4.;
//             let hold = hold.sum() as f32;
//             last_ret = if hold == 0. {
//                 if exceed_percent > 0.6 && exceed_percent < 1. {
//                     LiveTarget::OrderAction(OrderAction::ShOpen(1, lo_price_round))
//                 } else if exceed_percent < 0.4 && exceed_percent > 0. {
//                     LiveTarget::OrderAction(OrderAction::LoOpen(1, sh_price_round))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::No)
//                 }
//             } else if hold > 0. {
//                 if c <= sh_price_round - ticker_gap {
//                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, tick_data.bid1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, lo_price_round))
//                 }
//             }  else {
//                 if c >= lo_price_round + ticker_gap {
//                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, tick_data.ask1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, sh_price_round))
//                 }
//             };
//             loge!(ticker, "--ta-- last ret: {:?}", last_ret);
//             last_ret.clone()
//         })
//     }
// }


// #[ta_derive2]
// pub struct cond3(pub Cond2Box, pub Cond2Box);

// #[typetag::serde(name = "c71inter_cond3")]
// impl CondType0 for cond3 {
//     fn cond(&self) -> CondOpsRetFnType {
//         // let mut last_ret = LiveTarget::TargetAndPrice(TargetAndPrice::QuickOut);
//         let mut last_ret = LiveTarget::OrderAction(OrderAction::No);
//         let mut lo_cond = self.0.cond();
//         let mut sh_cond = self.1.cond();
//         Box::new(move |&StreamCondType0 { tick_data, di_index, di, hold,  .. }| {
//             let i = di_index;
//             let ticker = di.pcon.ticker;
//             let c = tick_data.c;
//             let ta_res = di.calc(PMS1.clone());
//             let sh_price = ta_res[0][i];
//             let lo_price = ta_res[2][i];
//             let exceed_percent = get_exceed_percent(c, &ta_res, i);
//             let sh_price_round = sh_price.round_ticker_price(ticker);
//             let lo_price_round = lo_price.round_ticker_price(ticker);
//             let ticker_gap = ticker.info().tz * 4.;
//             let hold = hold.sum() as f32;
//             last_ret = if hold == 0. {
//                 if exceed_percent > 0.6 && exceed_percent < 1. && lo_cond(i, i,  di) {
//                     LiveTarget::OrderAction(OrderAction::ShOpen(1, lo_price_round))
//                 } else if exceed_percent < 0.4 && exceed_percent > 0. && sh_cond(i, i, di) {
//                     LiveTarget::OrderAction(OrderAction::LoOpen(1, sh_price_round))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::No)
//                 }
//             } else if hold > 0. {
//                 if c <= sh_price_round - ticker_gap {
//                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, tick_data.bid1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, lo_price_round))
//                 }
//             }  else {
//                 if c >= lo_price_round + ticker_gap {
//                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, tick_data.ask1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, sh_price_round))
//                 }
//             };
//             loge!(ticker, "--ta-- last ret: {:?}", last_ret);
//             last_ret.clone()
//         })
//     }
// }

// #[ta_derive2]
// pub struct cond4;

// #[typetag::serde(name = "c71inter_cond4")]
// impl CondType0 for cond4 {
//     fn cond(&self) -> CondOpsRetFnType {
//         // let mut last_ret = LiveTarget::TargetAndPrice(TargetAndPrice::QuickOut);
//         let mut last_ret = LiveTarget::OrderAction(OrderAction::No);
//         let pms = ori + ono + ta(0.1, rl5mall.pri_box());
//         Box::new(move |&StreamCondType0 { tick_data, di_index, di, hold,  .. }| {
//             let i = di_index;
//             let ticker = di.pcon.ticker;
//             let c = tick_data.c;
//             let ta_res = di.calc(&pms);
//             let sh_price = ta_res[0][i];
//             let lo_price = ta_res[2][i];
//             let sh_price_round = sh_price.round_ticker_price(ticker);
//             let lo_price_round = lo_price.round_ticker_price(ticker);
//             let ticker_gap = ticker.info().tz * 4.;
//             let ticker_gap2 = ticker.info().tz * 1.;
//             let hold = hold.sum() as f32;
//             last_ret = if hold == 0. {
//                 if tick_data.c == lo_price_round - ticker_gap2 {
//                     LiveTarget::OrderAction(OrderAction::LoOpen(1, tick_data.bid1))
//                 } else if tick_data.c == sh_price_round + ticker_gap2 {
//                     LiveTarget::OrderAction(OrderAction::ShOpen(1, tick_data.ask1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::No)
//                 }
//             } else if hold > 0. {
//                 if c <= sh_price_round {
//                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, tick_data.ask1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, lo_price_round + ticker_gap))
//                 }
//             }  else {
//                 if c >= lo_price_round {
//                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, tick_data.bid1))
//                 } else {
//                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, sh_price_round - ticker_gap))
//                 }
//             };
//             loge!(ticker, "--ta-- last ret: {:?}", last_ret);
//             last_ret.clone()
//         })
//     }
// }

#[ta_derive2]
pub struct cond5(pub CondType6Box, pub CondType6Box);

#[typetag::serde(name = "c71inter_cond5")]
impl CondType2 for cond5 {
    fn cond(&self) -> RetFnCondType2 {
        todo!();
        // let mut last_ret = LiveTarget::OrderAction(OrderAction::No);
        // let pms = ori + ono + ta(0.1, rl5mall.pri_box());
        // let mut cond1_fn = self.0.cond_type6_lazy();
        // let mut cond2_fn = self.1.cond_type6_lazy();
        // Box::new(move |&StreamCondType2 { stream_api, di_kline }| {
        //     let i = di_index;
        //     let ticker = di.pcon.ticker;
        //     let c = tick_data.c;
        //     let ta_res = di.calc(&pms);
        //     let sh_price = ta_res[0][i];
        //     let lo_price = ta_res[2][i];
        //     let sh_price_round = sh_price.round_ticker_price(ticker);
        //     let lo_price_round = lo_price.round_ticker_price(ticker);
        //     let ticker_gap = ticker.info().tz * 4.;
        //     let ticker_gap2 = ticker.info().tz * 1.;
        //     let hold = hold.sum() as f32;
        //     last_ret = if hold == 0. {
        //         if tick_data.c == lo_price_round - ticker_gap2 && cond1_fn(i, i, di) {
        //             LiveTarget::OrderAction(OrderAction::LoOpen(1, tick_data.bid1))
        //         } else if tick_data.c == sh_price_round + ticker_gap2 && cond2_fn(i, i, di) {
        //             LiveTarget::OrderAction(OrderAction::ShOpen(1, tick_data.ask1))
        //         } else {
        //             LiveTarget::OrderAction(OrderAction::No)
        //         }
        //     } else if hold > 0. {
        //         if c <= sh_price_round {
        //             LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, tick_data.ask1))
        //         } else {
        //             LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, lo_price_round + ticker_gap))
        //         }
        //     }  else {
        //         if c >= lo_price_round {
        //             LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, tick_data.bid1))
        //         } else {
        //             LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, sh_price_round - ticker_gap))
        //         }
        //     };
        //     loge!(ticker, "--ta-- last ret: {:?}", last_ret);
        //     last_ret.clone()
        // })
    }
}

// // #[ta_derive2]
// // pub struct cond6(pub Cond2Box, pub Cond2Box);

// // #[typetag::serde(name = "c71inter_cond3")]
// // impl CondType0 for cond5 {
// //     fn cond(&self) -> CondOpsRetFnType {
// //         // let mut last_ret = LiveTarget::TargetAndPrice(TargetAndPrice::QuickOut);
// //         let mut last_ret = LiveTarget::OrderAction(OrderAction::No);
// //         let pms = ori + ono + ta(0.1, rl5mall.pri_box());
// //         let mut cond1_fn = self.0.cond();
// //         let mut cond2_fn = self.1.cond();
// //         Box::new(move |&StreamCondType0 { tick_data, di_index, di, hold,  .. }| {
// //             let i = di_index;
// //             let ticker = di.pcon.ticker;
// //             let c = tick_data.c;
// //             let ta_res = di.calc(&pms);
// //             let sh_price = ta_res[0][i];
// //             let lo_price = ta_res[2][i];
// //             let sh_price_round = sh_price.round_ticker_price(ticker);
// //             let lo_price_round = lo_price.round_ticker_price(ticker);
// //             let ticker_gap = ticker.info().tz * 4.;
// //             let ticker_gap2 = ticker.info().tz * 1.;
// //             let hold = hold.sum() as f32;
// //             last_ret = if hold == 0. {
// //                 if tick_data.c == lo_price_round - ticker_gap2 && cond1_fn(i, i, di) {
// //                     LiveTarget::OrderAction(OrderAction::LoOpen(1, tick_data.bid1))
// //                 } else if tick_data.c == sh_price_round + ticker_gap2 && cond2_fn(i, i, di) {
// //                     LiveTarget::OrderAction(OrderAction::ShOpen(1, tick_data.ask1))
// //                 } else {
// //                     LiveTarget::OrderAction(OrderAction::No)
// //                 }
// //             } else if hold > 0. {
// //                 if c <= sh_price_round {
// //                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, tick_data.ask1))
// //                 } else {
// //                     LiveTarget::OrderAction(OrderAction::ShClose(hold as i32, lo_price_round + ticker_gap))
// //                 }
// //             }  else {
// //                 if c >= lo_price_round {
// //                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, tick_data.bid1))
// //                 } else {
// //                     LiveTarget::OrderAction(OrderAction::LoClose(-hold as i32, sh_price_round - ticker_gap))
// //                 }
// //             };
// //             loge!(ticker, "--ta-- last ret: {:?}", last_ret);
// //             last_ret.clone()
// //         })
// //     }
// // }

// lazy_static! {
//     pub static ref PMS1: Pms = ori + ono + ta(0.1, rl5mall.pri_box());
//     pub static ref PMS2: Pms = ori + oos + EffRatio(2, 10);
//     pub static ref COND_L1: BandCond<Pms> = BandCond(Lo, BandState::Action, PMS1.clone());
//     pub static ref COND_S1: BandCond<Pms> = BandCond(Sh, BandState::Action, PMS1.clone());
//     pub static ref COND_L2: BandCond<Pms> = BandCond(Lo, BandState::Lieing, PMS2.clone());
//     pub static ref COND_S2: BandCond<Pms> = BandCond(Sh, BandState::Lieing, PMS2.clone());
//     pub static ref ptm: Ptm = {
//         let msig_oc_l = qust::msig!(and, COND_L1.clone(), COND_L2.clone());
//         let msig_ec_s = qust::msig!(or, COND_S1.clone(), Filterday);
//         let msig_oc_s = qust::msig!(and, COND_S1.clone(), COND_S2.clone());
//         let msig_ec_l = qust::msig!(or, COND_L1.clone(), Filterday);

//         let tsig_l = Tsig::new(Lo, &msig_oc_l, &msig_ec_s);
//         let tsig_s = Tsig::new(Sh, &msig_oc_s, &msig_ec_l);
//         let stp_l = Stp::Stp(tsig_l);
//         let stp_s = Stp::Stp(tsig_s);
//         Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
//     };

//     pub static ref ptm2: Ptm = {
//         let lo_out_msig = qust::msig!(or, exit_by_tick_size(Lo, 4.), Filterday);
//         let sh_out_msig = qust::msig!(or, exit_by_tick_size(Sh, 4.), Filterday);
//         let cond = cond1 { sh_out: sh_out_msig.cond_box(), lo_out: lo_out_msig.cond_box() };
//         Ptm::Ptm6(cond.condops_box())
//     };
//     pub static ref ptm3: Ptm = Ptm::Ptm6(cond2.condops_box());
// }

