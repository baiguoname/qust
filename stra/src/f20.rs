use serde::{Serialize, Deserialize};
use qust::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Serialize, Deserialize)]
pub struct ta(pub usize, pub usize);
#[typetag::serde(name = "f20_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        let pms = di.last_dcon() + ono + ShiftDays(0, KlineType::Open, IndexSpec::First);
        let o_today = di.calc(&pms)[0].clone();
        vec![o_today, di.h(), di.l(), di.c()]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let up_avg = izip!(da[1].iter(), da[3].iter())
            .map(|(&x, &y)|  x - y)
            .collect_vec()
            .roll(RollFunc::Max, RollOps::N(self.0))
            .into_iter()
            .zip(da[0].iter())
            .map(|(x, y)| x + y)
            .collect_vec();
        let do_avg = izip!(da[2].iter(), da[3].iter())
            .map(|(&x, &y)| x - y)
            .collect_vec()
            .roll(RollFunc::Min, RollOps::N(self.0))
            .into_iter()
            .zip(da[0].iter())
            .map(|(x, y)| x + y)
            .collect_vec();
        let median = izip!(da[1].iter(), da[2].iter())
            .map(|(x, y)| (x + y) / 2.)
            .collect_vec();
        let range = izip!(da[1].iter(), da[2].iter())
            .map(|(x, y)| x - y);
        let ma = da[3]
            .roll(RollFunc::Mean, RollOps::N(self.1));
        vec![
            do_avg,
            median, 
            up_avg, 
            range.collect_vec(), 
            ma, 
            da[1].to_vec(), 
            da[2].to_vec(), 
            da[3].to_vec(),
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct open_long(pub Pms);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct open_short(pub Pms);

#[typetag::serde(name = "f20_open_long")]
impl Cond for open_long {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        Box::new(
            move |e, _o| e >= 2 && {
                let k = e - 1;
                data[1][k] > data[5][k - 1] &&
                data[3][k] > data[3][k - 1] &&
                data[7][e] > data[4][e] &&
                data[7][e - 1] > data[2][e]
            }
        )
    }
}

#[typetag::serde(name = "f20_open_short")]
impl Cond for open_short {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        Box::new(
            move |e, _o| e >= 2 && {
                let k = e - 1;
                data[1][k] < data[6][k - 1] &&
                data[3][k] > data[3][k - 1] && 
                data[7][e] < data[4][e] &&
                data[7][e - 1] < data[0][e - 1]
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct exit_short(pub Pms);
#[derive(Debug, Clone,  Serialize, Deserialize)]
pub struct exit_long(pub Pms);

#[typetag::serde(name = "f20_exit_short")]
impl Cond for exit_short {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        let data1 = data[7].clone();
        let data2 = data[0].clone();
        Box::new(
            move |e, _o| {
                e >= 2 && data1[e] < data2[e]
            }
        )
    }
}

#[typetag::serde(name = "f20_exit_long")]
impl Cond for exit_long {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        let data1 = data[7].clone();
        let data2 = data[2].clone();
        Box::new(
            move |e, _o| {
                e >= 2 && data1[e] < data2[e]
            }
        )
    }
}

use super::c71;

lazy_static! {
    pub static ref ptm: Ptm = {
        let pms       = ori + oos + ta(3, 5);
        let cond1     = open_long(pms.clone());
        let cond2     = open_short(pms.clone());
        let msig_ec_s = c71::COND_S1.to_box() | Filterday.to_box();
        let msig_ec_l = c71::COND_L1.to_box() | Filterday.to_box();
        let tsig_l    = Tsig::Tsig(Lo, Sh, cond1.to_box(), msig_ec_s);
        let tsig_s    = Tsig::Tsig(Sh, Lo, cond2.to_box(), msig_ec_l);
        let stp_l     = Stp::Stp(tsig_l);
        let stp_s     = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
}