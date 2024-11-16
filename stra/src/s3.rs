use serde::{Serialize, Deserialize};
use qust::prelude::*;
use crate::econd::{self, ThreType};
use qust_derive::*;


#[ta_derive]
pub struct ta(pub usize, pub usize);

#[typetag::serde(name = "s3_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![di.h(), di.l(), di.c()]
    }

    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let ma1 = da[2].lag(1).ema(self.1);
        let ma2 = ma1.ema(self.0);
        let h_l = izip!(da[0].iter(), da[1].iter()).map(|(x, y)| x + y).collect_vec();
        let h_m = izip!(da[0].iter(), da[0].lag(1)).map(|(x, y)| (x - y).abs());
        let l_m = izip!(da[1].iter(), da[1].lag(1)).map(|(x, y)| (x - y).abs());
        let change = izip!(h_l.iter(), h_l.lag(1).iter(), h_m, l_m)
            .map(|(x1, x2, x3, x4)| {
                let (dbf, kbf) = if *x1 < *x2 {
                        (x3.max(x4), 0.)
                    } else if *x1 > *x2 {
                        (0., x3.max(x4))
                    } else {
                        (0., 0.)
                    };
                let dbl = (dbf + 100.) / (dbf + 100. + kbf + 100.);
                let kbl = (kbf + 100.) / (dbf + 100. + kbf + 100.);
                dbl - kbl
             })
            .collect_vec();
        let ma_change =  change.roll(RollFunc::Mean, RollOps::N(self.0));
        let ma_change2 = ma_change.roll(RollFunc::Mean, RollOps::N(self.0));
        vec![ma1, ma2, change, ma_change, ma_change2]
    }
}

#[ta_derive]
pub struct cond(pub Dire, pub Pms);

#[typetag::serde(name = "s3_cond")]
impl Cond for cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data = di.calc(&self.1);
        let ma1 = data[0].clone();
        let ma2 = data[1].clone();
        let change = data[2].clone();
        let ma_change = data[3].clone();
        let ma_change2 = data[4].clone();
        match self.0 {
            Dire::Lo => Box::new(move |e, _o| {
                e > 0 &&
                    c[e - 1] > ma1[e] &&
                    ma1[e] > ma2[e] &&
                    change[e] > 0. &&
                    ma_change[e] > ma_change[2]
            }),
            Dire::Sh => Box::new(move |e, _o| {
                e > 0 &&
                    c[e - 1] < ma1[e] &&
                    ma1[e] < ma2[e] &&
                    change[e] < 0. &&
                    ma_change[e] < ma_change2[e]
            }),
        }
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let pms = ori + ono + ta(100, 1200);
        let cond1 = cond(Lo, pms.clone());
        let cond2 = cond(Sh, pms);
        let cond3 = econd::stop_cond(Lo, ThreType::Percent(0.02));
        let cond4 = econd::stop_cond(Sh, ThreType::Percent(0.02));
        let ptm1 = Ptm::Ptm3(Box::new(M1(1.)), Lo, cond1.to_box(), cond3.to_box());
        let ptm2 = Ptm::Ptm3(Box::new(M1(1.)), Sh, cond2.to_box(), cond4.to_box());
        Ptm::Ptm4(Box::new(ptm1), Box::new(ptm2))
    };
}