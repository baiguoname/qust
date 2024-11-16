#![allow(non_camel_case_types)]
use serde::{Serialize, Deserialize};
use qust::prelude::*;
use qust_derive::*;

#[derive(Debug, Clone, PartialEq, Copy, Serialize, Deserialize)]
pub struct ta(pub usize);

#[typetag::serde(name = "cqqq_ta")]
impl Ta for ta {
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let data_lag = izip!(da[0].iter(), da[0].lag((self.0, f32::NAN)))
            .map(|(x, y)| x - y)
            .collect_vec();
        let data_lag_1 = data_lag.lag((1, f32::NAN));
        let mark_ind0 = izip!(data_lag.iter(), data_lag_1.iter())
            .map(|(x, y)|  if x <= y { 1f32 } else { f32::NAN })
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(3));
        let mark_ind1 = izip!(data_lag.iter(), data_lag_1.iter())
            .map(|(x, y)|  if x >= y { 1f32 } else { f32::NAN })
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(3));
        vec![mark_ind0, mark_ind1]
    }
}

#[ta_derive]
pub struct cond1(pub Pms);
#[ta_derive]
pub struct cond2(pub Pms);

#[typetag::serde(name = "cqqq_cond1")]
impl Cond for cond1 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0)[0].clone();
        Box::new(
            move |e: usize, _o: usize| { data[e] == 3f32 }
        )
    }
}
#[typetag::serde(name = "cqqq_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0)[1].clone();
        Box::new(
            move |e: usize, _o: usize| { data[e] == 3f32 }
        )
    }
}


lazy_static! {
    pub static ref ptm: Ptm = {
        let pms = ori + oos + ta(10);
        let cond_l1 = cond1(pms.clone());
        let cond_s1 = cond2(pms.clone());
        let cond_l2 = Iocond { pms: ori + oos + Rsi(3), range: 0f32 .. 15f32 };
        let cond_s2 = Iocond { pms: ori + oos + Rsi(3), range: 85f32 .. 101f32 };
        let cond_s_e = Iocond { pms: ori + oos + Rsi(2), range: 75f32 .. 101f32 };
        let cond_l_e = Iocond { pms: ori + oos + Rsi(2), range: 0f32 .. 25f32 };
        let cond_l_o = qust::msig!(and, cond_l1, cond_l2);
        let cond_s_o = qust::msig!(and, cond_s1, cond_s2);
        let tsig_l = Tsig::new(Lo, &cond_l_o, &cond_s_e);
        let tsig_s  = Tsig::new(Sh, &cond_s_o, &cond_l_e);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
}



