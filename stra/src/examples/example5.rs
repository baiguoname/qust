use serde::{ Serialize, Deserialize };
use qust::prelude::*;

#[ta_derive]
pub struct ta(pub usize, pub usize);

#[typetag::serde]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![di.c()]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let mean_short = da[0].roll(RollFunc::Mean, RollOps::N(self.0));
        let mean_long = da[0].roll(RollFunc::Mean, RollOps::N(self.1));
        vec![mean_short, mean_long]
    }
}

#[ta_derive]
pub struct cond(pub Dire, pub BandState, pub ta);

#[typetag::serde]
impl Cond for cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.2);
        let mean_short = data[0].clone();
        let mean_long = data[1].clone();
        match (self.0, self.1.clone()) {
            (Lo, BandState::Action) => {
                Box::new(move |e: usize, _o: usize| {
                    e > 0 &&
                        mean_short[e - 1] < mean_long[e - 1] &&
                        mean_short[e] > mean_long[e]
                })
            }
            (Sh, BandState::Action) => {
                Box::new(move |e: usize, _o: usize| {
                    e > 0 &&
                        mean_short[e - 1] > mean_long[e - 1] &&
                        mean_short[e] < mean_long[e]
                })
            }
            (Sh, BandState::Lieing) => {
                Box::new(move |e: usize, _o: usize| {
                    e > 0 &&
                        mean_short[e] < mean_long[e]
                })
            }
            (Lo, BandState::Lieing) => {
                Box::new(move |e: usize, _o: usize| {
                    e > 0 &&
                        mean_short[e] > mean_long[e]
                })
            }
        }

    }
}

pub fn get_two_ma_ptm(n: usize, m: usize) -> Ptm {
    use BandState::*;
    let cond1 = cond(Lo, Action, ta(n, m));
    let cond2 = cond(Sh, Action, ta(n, m));
    let cond3 = cond(Sh, Lieing, ta(n, m));
    let cond4 = cond(Lo, Lieing, ta(n, m));
    let ptm1 = Ptm::Ptm3(Box::new(M1(1.)), Lo, cond1.to_box(), cond3.to_box());
    let ptm2 = Ptm::Ptm3(Box::new(M1(1.)), Sh, cond2.to_box(), cond4.to_box());
    Ptm::Ptm4(Box::new(ptm1), Box::new(ptm2))
}

/*aaaa
let dil = gen_di.get(rl5m.clone());
let ptm = get_two_ma_ptm(10, 20);
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec.sum().plot();
let csv_res = pnl_vec.sum();
aaaa*/

