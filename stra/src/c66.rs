#![allow(non_camel_case_types, clippy::type_complexity)] 
use serde::{Serialize, Deserialize};
use qust::prelude::*;
use qust_derive::*;
use crate::c25;


#[ta_derive]
pub struct ta(pub usize, pub usize);

impl ta {
    fn get_usize(data: &[bool]) -> v32 {
        let mut res = vec![f32::NAN; data.len()];
        let mut last_v = f32::NAN;
        for i in 1..data.len() {
            if data[i - 1] {
                last_v = i as f32;
            }
            res[i] = last_v;
        }
        res
    }
}

#[typetag::serde(name = "c66_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![di.h(), di.l(), di.c()]
    }
    
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let hhv = da[0].roll(RollFunc::Max, RollOps::N(self.0));
        let llv = da[1].roll(RollFunc::Min, RollOps::N(self.0));
        let hlv = izip!(hhv.iter(), llv.iter())
            .map(|(x, y)| x - y)
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(self.1));
        let clv = izip!(da[2].iter(), llv.iter())
            .map(|(x, y)| x - y)
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(self.1));
        let k_value = izip!(clv.iter(), hlv.iter())
            .map(|(x, y)| 100. * (if y == &0. { 0. } else { x / y }))
            .collect_vec();
        let d_value = k_value.roll(RollFunc::Mean, RollOps::N(self.1));
        let line_cross = izip!(k_value.iter(), d_value.iter())
            .map(|(x, y)| x - y)
            .collect_vec();
        let is_cross_do = line_cross.rolling(2)
            .map(|x| x.len() > 1 && x[0] <= 0. && x[1] > 0.)
            .collect::<Vec<bool>>();
        let is_cross_up = line_cross.rolling(2)
            .map(|x| x.len() > 1 && x[0] >= 0. && x[1] < 0.)
            .collect::<Vec<bool>>();
        let l_bar = ta::get_usize(&is_cross_do);
        let h_bar = ta::get_usize(&is_cross_up);
        let lhs = izip!(l_bar.iter(), h_bar.iter())
            .map(|(&x, &y)| { if (x - y).is_nan() { f32::NAN} else { 20f32.max(x - y) } })
            .collect_vec();
        let hls = izip!(l_bar.iter(), h_bar.iter())
            .map(|(&x, &y)| { if (y - x).is_nan() { f32::NAN} else { 20f32.max(y - x) } })
            .collect_vec();
        let mut low_down = vec![f32::NAN; lhs.len()];
        let mut high_up = vec![f32::NAN; lhs.len()];
        for i in 1..low_down.len() {
            if !lhs[i].is_nan() {
                let i_begin = 0f32.max(i as f32 - lhs[i]) as usize;
                low_down[i] = da[1][i_begin .. i].agg(RollFunc::Min);
            }
            if !hls[i].is_nan() {
                let i_begin = 0f32.max(i as f32 - hls[i]) as usize;
                high_up[i] = da[0][i_begin .. i].agg(RollFunc::Max);
            }
        }
        let mut ld = izip!(is_cross_do.lag(1).iter(), low_down.iter())
            .map(|(&x, &y)| if x { y } else { f32::NAN })
            .collect_vec();
        ld.ffill();
        let mut hd = izip!(is_cross_up.lag(1).iter(), high_up.iter())
            .map(|(&x, &y)| if x { y } else { f32::NAN })
            .collect_vec();
        hd.ffill();
        let mut ks = izip!(is_cross_do.lag(2).iter(), is_cross_up.lag(2).iter())
            .map(|(&x, &y)| {
                if x { -1. } else if y { 1. } else { f32::NAN }
            })
            .collect_vec();
        ks.ffill();
        let ma1 = da[2].lag((1usize, da[2][0])).roll(RollFunc::Mean, RollOps::N(self.1));
        vec![da[0].to_vec(), da[1].to_vec(), da[2].to_vec(), ks, ld, hd, ma1]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond1(pub Pms);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond2(pub Pms);

#[typetag::serde(name = "c66_cond1")]
impl Cond for cond1 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        Box::new(
            move |e: usize, _o: usize| { {
                e > 1 && data[3][e] > 0. &&
                    data[0][e] >= data[5][e - 1] &&
                    data[2][e - 1] > data[6][e]
            }}
        )
    }
}

#[typetag::serde(name = "c66_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        Box::new(
            move |e: usize, _o: usize| { {
                e > 0 && data[3][e] < 0. &&
                    data[1][e] <= data[4][e - 1] &&
                    data[2][e - 1] < data[6][e]
            }}
        )
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let pms = qust::pms!(ori, oos, ta(10, 20));
        let cond1_ = cond1(pms.clone());
        let cond2_ = cond2(pms);
        let (cond3_, cond4_) = if let Ptm::Ptm2(
            _,
            Stp::Stp(Tsig::Tsig(_, _, _, esig1)), 
            Stp::Stp(Tsig::Tsig(_, _, _, esig2))
        ) = c25::ptm.clone() {
                (esig1, esig2)
            } else { panic!()};
        let tsig_l = Tsig::Tsig(Lo, Sh, Box::new(cond1_), cond3_);
        let tsig_s = Tsig::Tsig(Sh, Lo, Box::new(cond2_), cond4_);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
}