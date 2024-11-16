use ds::prelude::*;
use qust::prelude::*;

#[ta_derive]
pub struct ta(pub usize, pub f32);

#[typetag::serde]
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

#[ta_derive]
pub struct cond1(pub Pms);
#[ta_derive]
pub struct cond2(pub Pms);

#[typetag::serde]
impl Cond for cond1 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0)[0].clone();
        Box::new(move |e: usize, _o: usize| { data[e] > 0.0 })
    }
}

#[typetag::serde]
impl Cond for cond2 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0)[0].clone();
        Box::new(move |e: usize, _o: usize| { data[e] < 0.0 })
    }
}

#[ta_derive]
pub struct cond3(pub Pms);
#[ta_derive]
pub struct cond4(pub Pms);

#[typetag::serde]
impl Cond for cond3 {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data1 = di.calc(&self.0)[0].clone();
        let data2 = di.h();
        Box::new(move |e, _o| data2[e] > data1[e])
    }
}

#[typetag::serde]
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
        let o_sig_l = msig!(and, cond1_, cond3_);
        let e_sig_s = msig!(or, cond2_, cond4_, Filterday);
        let o_sig_s = msig!(and, cond2_, cond4_);
        let e_sig_l = msig!(or, cond1_, cond3_, Filterday);
        let tsig_l = Tsig::new(Dire::Lo, &o_sig_l, &e_sig_s);
        let tsig_s = Tsig::new(Dire::Sh, &o_sig_s, &e_sig_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.0)), stp_l, stp_s)
    };
}

/*aaaa
let dil = gen_di.get((rl5m.clone(), tickers3.clone()));
let pnl_vec = ptm.clone().dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec
    .sum()
    .groupby(y2021_split.clone(), |x| x.sum())
    .with_stats()
    .aplot(2);
// let plot_res = pnl_vec.sum().aplot(8);
let csv_res = pnl_vec.sum();
aaaa*/

