use serde::{Serialize, Deserialize};
use qust::prelude::*;
use BandState::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond {
    dire: Dire,
    state: BandState,
    pre: Pre,
    macd: Macd,
    n: usize,
    m: usize,
}

#[typetag::serde(name = "macd_cond")]
impl Cond for cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let ema_s = di.calc(&(self.pre.clone() + RollTa(KlineType::Close, RollFunc::Mean, RollOps::N(self.n))))[0].clone();
        let ema_l = di.calc(&(self.pre.clone() + RollTa(KlineType::Close, RollFunc::Mean, RollOps::N(self.m))))[0].clone();
        let diff = di.calc(&(self.pre.clone() + self.macd.clone()))[0].clone();
        let macd = di.calc(&(self.pre.clone() + Diff(self.macd.0, self.macd.1)))[0].clone();
        match (&self.dire, &self.state) {
            (Lo, BandState::Action) => Box::new(
                move |e, _o| {
                    e > 1 &&
                        ema_s[e] > ema_l[e] &&
                        c[e] > ema_s[e] &&
                        c[e - 1] > ema_s[e - 1] &&
                        diff[e] > 0f32 &&
                        macd[e] > 0f32
                }
            ),
            (Sh, BandState::Action) => Box::new(
                move |e, _o| {
                    e > 1 &&
                        ema_s[e] < ema_l[e] &&
                        c[e] < ema_s[e] &&
                        c[e - 1] < ema_s[e - 1] &&
                        diff[e] < 0f32 &&
                        macd[e] < 0f32
                }
            ),
            (Lo, BandState::Lieing) => Box::new(
                move |e, _o| {
                    macd[e] > 0f32 || ema_s[e] > ema_l[e]
                }
            ),
            (Sh, BandState::Lieing) => Box::new(
                move |e, _o| {
                    macd[e] < 0f32 || ema_s[e] < ema_l[e]
                }
            ),
        }
    }
}

lazy_static!(
    pub static ref ptm: Ptm = {
        let pre = ori + ono;
        let macd = Macd(30, 50, 40);
        let cond_o_l = cond { dire: Lo, state: Action, pre: pre.clone(), macd: macd.clone(), n: 20, m: 40 };
        let cond_o_s = cond { dire: Sh, state: Action, pre: pre.clone(), macd: macd.clone(), n: 20, m: 40 };
        let cond_e_s = cond { dire: Sh, state: Lieing, pre: pre.clone(), macd: macd.clone(), n: 20, m: 40 };
        let cond_e_l = cond { dire: Lo, state: Lieing, pre, macd, n: 20, m: 40 };
        let tsig_l = Tsig::new(Lo, &cond_o_l, &cond_e_s);
        let tsig_s = Tsig::new(Sh, &cond_o_s, &cond_e_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
);



#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ta {
    pub n0: usize,
    pub n_atr_d: usize,
}

#[typetag::serde(name = "ll_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![
            di.c(),
            di.calc(Atr(self.n_atr_d))[0].clone(),
            di.calc(ShiftDays(1, KlineType::High, IndexSpec::Max))[0].clone(),
            di.calc(ShiftDays(1, KlineType::Low, IndexSpec::Min))[0].clone(),
            di.calc(ShiftDays(0, KlineType::Open, IndexSpec::First))[0].clone(),
            di.calc(ShiftDays(1, KlineType::Close, IndexSpec::Last))[0].clone(),
        ]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let c= da[0];
        vec![
            c.roll(RollFunc::Mean, RollOps::N(4)),//m0, 0
            c.roll(RollFunc::Mean, RollOps::N(self.n0)),//m1, 1
            c.roll(RollFunc::Mean, RollOps::N(4 * self.n0)),//m2, 2
            c.roll(RollFunc::Mean, RollOps::N(10 * self.n0)),//m3, 3
            da[1].to_vec(), // taAtr, 4
            da[1].roll(RollFunc::Mean, RollOps::N(self.n_atr_d)), // AvgtaAtr, 5
            izip!(da[2].iter(), da[3].iter()).map(|(x, y)| x - y).collect_vec(), //day_range, 6
            da[4].to_vec(), //open today, 7
            da[5].to_vec(), // close last, 8

        ]
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond2(pub Dire, pub Pms, pub usize);

#[typetag::serde(name = "macd_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let da = di.calc(&self.1);
        let o = di.o();
        let h = di.h();
        let l = di.l();
        let c = di.c();
        let w_mean = di.calc(
                &(ori + dayk.clone() + ono + RollTa(KlineType::Close, RollFunc::Mean, RollOps::N(self.2)) + vori))[0].clone();
        match self.0 {
            Lo => Box::new(
                move |e, _o| {
                    e > 4 &&
                        da[1][e] > da[2][e] &&
                        da[1][e] > da[1][e - 1] && 
                        da[0][e] * (2f32 / 3f32) + da[1][e] * (1f32 / 3f32) > da[1][e]  &&
                        da[1][e] > da[3][e] &&
                        da[3][e] > da[3][e - 4] &&
                        da[4][e] > da[5][e] &&
                        da[0][e] * (2f32 / 3f32) + da[1][e] * (1f32 / 3f32) >= (da[7][e] + da[8][e]) / 2f32 + 0.1 * da[6][e] + 1.5 * da[4][e] &&
                        c[e] > w_mean[e] &&
                        c[e] < o[e - 1] &&
                        (c[e] - o[e]).abs() * 2f32 > h[e] - l[e]
                }
            ),
            Sh => Box::new(
                move |e, _o| {
                    e > 4 &&
                        da[1][e] < da[2][e] &&
                        da[1][e] < da[1][e - 1] && 
                        da[0][e] * (2f32 / 3f32) + da[1][e] * (1f32 / 3f32) < da[1][e]  &&
                        da[1][e] < da[3][e] &&
                        da[3][e] < da[3][e - 4] &&
                        da[4][e] > da[5][e] &&
                        da[0][e] * (2f32 / 3f32) + da[1][e] * (1f32 / 3f32) <= (da[7][e] + da[8][e]) / 2f32 - 0.1 * da[6][e] - 1.5 * da[4][e] &&
                        c[e] < w_mean[e] &&
                        c[e] > o[e - 1] &&
                        (c[e] - o[e]).abs() * 2f32 > h[e] - l[e]
                }
            ),
        }   
    }
}

lazy_static!(
    pub static ref ptm2: Ptm = {
        use crate::econd;
        let ta = ta { n0: (24f32 / 0.8f32) as usize, n_atr_d: (15f32 / 0.8f32) as usize };
        let pms = ori + ha + ono + ta;
        let cond_o_l = cond2(Lo, pms.clone(), 10);
        let cond_o_s = cond2(Sh, pms.clone(), 10);
        let cond_e_s = econd::cond_out(Sh, 0.8);
        let cond_e_l = econd::cond_out(Lo, 0.8);

        let tsig_l = Tsig::new(Lo, &cond_o_l, &cond_e_s);
        let tsig_s = Tsig::new(Sh, &cond_o_s, &cond_e_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
);
