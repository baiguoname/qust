use serde::{ Serialize, Deserialize };
use qust::prelude::*;

use crate::{
    examples::{example8, example9},
    filter::AttachConds, 
    prelude::merge,
    inters::*,
};

///Sh: a = 开仓以来最高价 - 当前开盘价 * 0.01, 当前最低价小于a </br>
///Lo: a = 开仓以来最低价 + 当前开盘价 * 0.01，当前最高价大于a
#[ta_derive]
pub struct price_sby(pub Dire, pub f32);

#[typetag::serde]
impl Cond for price_sby {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let (ovec, h, l, c) = (di.o(), di.h(), di.l(), di.c());
        let last_c = c.to_vec();
        let multi = self.1;
        match self.0 {
            Lo => Box::new(move |e, o| {
                if e > o && h[e] > h[e - 1] { 
                    // last_c[o] = h[e];
                }
                l[e] <= last_c[o] - ovec[e] * multi
            }),
            Sh => Box::new(move |e, o| {
                if e > o && l[e] < l[e - 1] {
                    // last_c[o] = l[e];
                }
                h[e] > last_c[o] + ovec[e] * multi
            }),
        }
    }
}

//创建5min的平仓条件
pub const exit_cond_5min: (price_sby, price_sby) = (price_sby(Lo, 0.008), price_sby(Sh, 0.008));
//创建30min的平仓条件
pub const exit_cond_30min: (price_sby, price_sby) = (price_sby(Lo, 0.010), price_sby(Sh, 0.010));


lazy_static! {
    pub static ref ptm1: Stral = example9::ptm.clone().to_stral(&(tickers3.clone(), crate::prelude::rl5m.tri_box()));
    pub static ref ptm2: Stral = merge::ptm_bare.clone().to_stral(&(merge::tickers.clone(), crate::prelude::rl30mday.tri_box()));
}

/*aaaa
let dil = gen_di.get(vec![rl5m.clone(), rl30mday.clone()]);
let pnl1 = ptm1.dil(&dil).calc(cs2).sum();
let pnl1_after_exit = ptm1.attach_conds(or, &exit_cond_5min, Sh).dil(&dil).calc(cs2).sum();

let pnl2 = ptm2.dil(&dil).calc(cs2).sum();
let pnl2_after_exit = ptm2.attach_conds(or, &exit_cond_30min, Sh).dil(&dil).calc(cs2).sum();
let plot_res = [
    pnl1.get_part(y2021.clone()),
    pnl1_after_exit.get_part(y2021.clone()),
    pnl1,
    pnl1_after_exit,
    pnl2.get_part(y2021.clone()),
    pnl2_after_exit.get_part(y2021.clone()),
    pnl2,
    pnl2_after_exit,
]
    .with_stats()
    .aplot(2);
aaaa*/


