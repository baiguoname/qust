use qust::prelude::*;
use qust_derive::*;

#[ta_derive2]
pub struct ta(pub usize, pub f32);

#[typetag::serde(name = "c25_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![di.h(), di.l(), di.c()]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let n = self.0;
        let f = self.1;
        let mut sar_s = vec![f32::NAN; da[1].len()];
        let mut sar_l = vec![f32::NAN; da[1].len()];
        let mut state = 1;
        let mut factor = f;
        if da[1].len() < n {
            return vec![sar_s];
        }
        sar_s[n - 1] = da[1][..n].min();
        for i in n..da[1].len() {
            if state == 1 {
                if da[2][i] < sar_s[i - 1] {
                    state = -1;
                    factor = f;
                    sar_l[i] = da[0][i - n + 1..i + 1].max();
                } else if da[0][i] <= da[0][i - 1] {
                    sar_s[i] = sar_s[i - 1];
                } else if da[0][i] > da[0][i - 1] {
                    if factor <= 0.2 {
                        factor += f;
                    }
                    sar_s[i] = sar_s[i - 1] + factor * (da[0][i] - sar_s[i - 1]);
                }
            } else if state == -1 {
                if da[2][i] > sar_l[i - 1] {
                    state = 1;
                    factor = f;
                    sar_s[i] = da[0][i - n + 1..i + 1].min();
                } else if da[1][i] >= da[1][i - 1] {
                    sar_l[i] = sar_l[i - 1];
                } else if da[1][i] < da[1][i - 1] {
                    if factor <= 0.2 {
                        factor += f;
                    }
                    sar_l[i] = sar_l[i - 1] + factor * (da[1][i] - sar_l[i - 1]);
                }
            }
        }
        // vec![sar_l, sar_s]
        let sarsum = vec![&sar_l, &sar_s].nanmean2d();
        let res = S(&da[2]) - &sarsum;
        vec![res]
    }
}

#[ta_derive2]
pub struct cond1(pub Pms);
#[ta_derive2]
pub struct cond2(pub Pms);

#[typetag::serde(name = "c25_cond1")]
impl Cond for cond1 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0)[0].clone();
        Box::new(move |e: usize, _o: usize| { data[e] > 0.0 })
    }
}

#[typetag::serde(name = "c25_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0)[0].clone();
        Box::new(move |e: usize, _o: usize| { data[e] < 0.0 })
    }
}

use crate::filter::AttachConds;
use crate::econd;

#[ta_derive2]
pub struct cond3(pub Pms);
#[ta_derive2]
pub struct cond4(pub Pms);

#[typetag::serde(name = "c25_cond3")]
impl Cond for cond3 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data1 = di.calc(&self.0)[0].clone();
        let data2 = di.h();
        Box::new(move |e, _o| data2[e] > data1[e])
    }
}

#[typetag::serde(name = "c25_cond4")]
impl Cond for cond4 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data1 = di.calc(&self.0)[0].clone();
        let data2 = di.l();
        Box::new(move |e, _o| data2[e] < data1[e])
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let pms1 = ori + ha + oos + ta(10, 0.005);
        let pms2_l = ori + oos + maxta(KlineType::Close, 40);
        let pms2_s = ori + oos + minta(KlineType::Close, 40);
        let cond1_ = cond1(pms1.clone());
        let cond2_ = cond2(pms1);
        let cond3_ = cond3(pms2_l);
        let cond4_ = cond4(pms2_s);
        let o_sig_l = qust::msig!(and, cond1_, cond3_);
        let e_sig_s = qust::msig!(or, cond2_, cond4_, Filterday);
        let o_sig_s = qust::msig!(and, cond2_, cond4_);
        let e_sig_l = qust::msig!(or, cond1_, cond3_, Filterday);
        let tsig_l = Tsig::new(Dire::Lo, &o_sig_l, &e_sig_s);
        let tsig_s = Tsig::new(Dire::Sh, &o_sig_s, &e_sig_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.0)), stp_l, stp_s)
    };

    pub static ref ptm2: Ptm = {
        let pms1 = ori + VolFilter(10, 30) + ha + oos + ta(10, 0.005) + vori;
        let pms2_l = ori + oos + maxta(KlineType::Close, 40);
        let pms2_s = ori + oos + minta(KlineType::Close, 40);
        let cond1_ = cond1(pms1.clone());
        let cond2_ = cond2(pms1);
        let cond3_ = cond3(pms2_l);
        let cond4_ = cond4(pms2_s);
        let o_sig_l = qust::msig!(and, cond1_, cond3_);
        let e_sig_s = qust::msig!(or, cond2_, cond4_, Filterday);
        let o_sig_s = qust::msig!(and, cond2_, cond4_);
        let e_sig_l = qust::msig!(or, cond1_, cond3_, Filterday);
        let tsig_l = Tsig::new(Dire::Lo, &o_sig_l, &e_sig_s);
        let tsig_s = Tsig::new(Dire::Sh, &o_sig_s, &e_sig_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.0)), stp_l, stp_s)
    };
    pub static ref ptm3: Ptm = ptm
        .change_money(M3(1.))
        .attach_conds(or, &(econd::price_sby(Lo, 0.008), econd::price_sby(Sh, 0.008)), Dire::Sh);
}


// use crate::sig::cond::{Msig, Filterday};
// use crate::sig::livesig::Tsig;
// use crate::sig::posi::Dire;
// use crate::idct::pms::const_pms::*;
// use crate::idct::ta::{Max, Min};
// use crate::trade::inter::KlineType::*;
// use crate::sig::cond::LogicOps::{And as and, Or as or};
/* 

use crate::sig::cond::CondLoop;

#[ta_derive2]
pub struct C25loop1<T>(pub Pms<T, ta>, pub Pms<T, Max>, pub Pms<T, Min>);


impl<T: Convert + Clone> CondLoop for C25loop1<T> {
    fn calc_init(&self, di: &mut Di) {
        di.calc_init(&self.0);
        di.calc_init(&self.1);
        di.calc_init(&self.2);
    }

    fn cond<'a>(&self, di: &'a Di) -> Box<dyn FnMut(usize) -> f32 + 'a> {
        let data1 = &di.calc(&self.0)[0];
        let data2 = &di.calc(&self.1)[0];
        let data3 = &di.calc(&self.2)[0];
        let h = di.h();
        let l = di.l();
        let mut posi = 0f32;
        let mut _open_i = 0usize;
        Box::new(
            move |i: usize| {
                if posi == 0f32 {
                    if data1[i] > 0.0 && h[i] > data2[i] {
                        posi = 1f32;
                        _open_i = i;
                    } else if data1[i] < 0.0 && l[i] < data3[i] {
                        posi = -1f32;
                        _open_i = i;
                    }
                } else if (posi > 0f32 && (data1[i] < 0.0 || l[i] < data3[i])) ||
                          (posi < 0f32 && (data1[i] > 0.0 || h[i] > data2[i])) {
                    posi = 0f32;
                } 
                // else if posi == -1f32 && (data1[i] > 0.0 || h[i] > data2[i]) {
                //     posi = 0f32;
                // }
                posi
            }
        )
    }
}
*/

// #[ta_derive2]
// pub struct two_ma_lo(pub usize, pub usize);

// #[typetag::serde(name = "two_ma_sh")]
// impl Cond for two_ma_lo {
//     fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
//         let mean_short = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
//         let mean_long = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
//         Box::new(move |e, _o| {
//             e > 0 &&
//                 mean_short[e - 1] < mean_long[e - 1] &&
//                 mean_short[e] > mean_long[e]
//         })
//     }
// }

// #[ta_derive2]
// pub struct two_ma_sh(pub usize, pub usize);

// #[typetag::serde(name = "two_ma_lo")]
// impl Cond for two_ma_sh {
//     fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
//         let mean_short = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
//         let mean_long = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
//         Box::new(move |e, _o| {
//             mean_short[e] < mean_long[e]
//         })
//     }
// }

fn get_ooo() -> i32 {
    println!("aaa");
    10
}

lazy_static! {
    pub static ref olc: i32 = get_ooo();
}