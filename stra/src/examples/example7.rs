use serde::{ Serialize, Deserialize };
use qust::prelude::*;

use crate::filter::AttachConds;
use super::{ example5, example6::* };

/*aaaa
let ptm_ori = example5::get_two_ma_ptm(10, 20);
let stop_cond_sh = stop_cond(Sh, ThreType::Num(0.005));
let stop_cond_lo = stop_cond(Lo, ThreType::Num(0.005));
let ptm = ptm_ori.attach_conds(or, (&stop_cond_sh, &stop_cond_lo), Sh);
let dil = gen_di.get(rl5m.clone());
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec.sum().plot();
let csv_res = pnl_vec.sum();
aaaa*/
