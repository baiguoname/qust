use serde::{ Serialize, Deserialize };
use qust::prelude::*;

use super::{example2::*, example3::*};

/*aaaa
let ptm1 = (Dire::Lo, cond_long_open(10, 20), cond_short_exit(10, 20)).to_ptm();
let ptm2 = (Dire::Sh, cond_short_open(10, 20), cond_long_exit(10, 20)).to_ptm();
let ptm = (ptm1, ptm2).to_ptm();
let dil = gen_di.get(rl5m.clone());
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec.aplot(4);
let csv_res = pnl_vec.sum();
aaaa*/