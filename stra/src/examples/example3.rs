use serde::{ Serialize, Deserialize };
use qust::prelude::*;

use super::example2::*;

#[ta_derive]
pub struct cond_short_open(pub usize, pub usize);

#[typetag::serde]
impl Cond for cond_short_open {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let mean_short = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
        let mean_long = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
        Box::new(move |e: usize, o: usize| {
            e > 0 &&
                mean_short[e - 1] > mean_long[e - 1] &&
                mean_short[e] < mean_long[e]
        })
    }
}

#[ta_derive]
pub struct cond_long_exit(pub usize, pub usize);

#[typetag::serde]
impl Cond for cond_long_exit {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let mean_short = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
        let mean_long = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
        Box::new(move |e: usize, o: usize| {
            mean_short[e] > mean_long[e]
        })
    }
}

/*aaaa
let ptm = (Dire::Sh, cond_short_open(10, 20), cond_long_exit(10, 20)).to_ptm();
let dil = gen_di.get(rl5m.clone());
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec.aplot(4);
let csv_res = pnl_vec.sum();
aaaa*/