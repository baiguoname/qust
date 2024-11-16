use serde::{Serialize, Deserialize};
use qust::prelude::*;
use crate::econd::{self, ThreType};


#[ta_derive]
pub struct ta {
    pub len: usize,
    pub sig_len: usize,
    pub n: usize,
}

impl ta {
    fn calc_da(&self, data: &[&[f32]], n: usize) -> v32 {
        data[1]
            .roll(RollFunc::Sum, RollOps::N(n))
            .iter()
            .zip(data[1].iter())
            .zip(data[0].iter())
            .map(|((x, y), z)| {
                (y / x) * z
            })
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(n))
    }
}

#[typetag::serde(name = "s20_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![di.c(), di.v()]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        self.calc_da(&da);
        let wi_sum = da[1].roll(RollFunc::Sum, RollOps::N(self.sig_len));
        let wi_sum_n = da[1].roll(RollFunc::Sum, RollOps::N(self.sig_len * self.n));
        let v_wap = da[1].iter().zip(wi_sum.iter()).map(|(x, y)| x / y);
        let v_wap_n = da[1].iter().zip(wi_sum_n.iter()).map(|(x, y)| x / y);
        let v_wap_price = v_wap.zip(da[0].iter()).map(|(x, y)| x * y).collect_vec();
        let v_wap_price_n = v_wap_n.zip(da[0].iter()).map(|(x, y)| x * y).collect_vec();
        let v_wap_a = v_wap_price.roll(RollFunc::Sum, RollOps::N(self.len));
        let v_wap_b = v_wap_price_n.roll(RollFunc::Sum, RollOps::N(self.len * self.n));
        vec![v_wap_a, v_wap_b]
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let pms = ori + ono + ta { len: 10, sig_len: 5, n: 3 };
        let cond1 = CrossCond(Lo, pms.clone());
        let cond2 = CrossCond(Sh, pms);
        let cond3 = econd::stop_cond(Lo, ThreType::Percent(0.02));
        let cond4 = econd::stop_cond(Sh, ThreType::Percent(0.02));
        let tsig_l = Tsig::new(Lo, &cond1, &cond3);
        let tsig_s = Tsig::new(Sh, &cond2, &cond4);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
}