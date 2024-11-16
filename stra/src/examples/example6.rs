use serde::{ Serialize, Deserialize };
use qust::prelude::*;

#[ta_derive]
pub enum ThreType {
    Num(f32),
    Percent(f32),
}

impl ThreType {
    pub fn get_thre_vec(&self, di: &Di) -> v32 {
        match self {
            ThreType::Num(i) => vec![*i; di.size()],
            ThreType::Percent(i) => {
                di.calc(vol_pms.clone())[0]
                    .iter()
                    .map(|x| x * i)
                    .collect_vec()
            }
        }
    }
}

#[ta_derive2]
pub struct stop_cond(pub Dire, pub ThreType);

#[typetag::serde(name = "example_stop_cond")]
impl Cond for stop_cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let thre = self.1.get_thre_vec(di);
        match self.0 {
            Lo => Box::new(move |e, o| c[e] / c[o] - 1.0 > thre[e]),
            Sh => Box::new(move |e, o| c[e] / c[o] - 1.0 < -thre[e]),
        }
    }
}

#[ta_derive]
pub struct exit_by_k_num(pub usize);

#[typetag::serde]
impl Cond for exit_by_k_num {
    fn cond<'a>(&self, _di: &'a Di) -> LoopSig<'a> {
        let thre = self.0;
        Box::new(move |e, o| e - o >= thre)
    }
}

/*aaaa
use super::example5::*;
use BandState::*;
let cond_open = cond(Lo, Action, ta(10, 20));
let cond_exit1 = cond(Sh, Lieing, ta(10, 20));
let cond_exit2 = stop_cond(Sh, ThreType::Num(0.005));
let cond_exit = qust::msig!(or, cond_exit1, cond_exit2);
let tsig = (Dire::Lo, cond_open, cond_exit).to_ptm();
let dil = gen_di.get(rl5m.clone());
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec.sum().plot();
let csv_res = pnl_vec.sum();
aaaa*/