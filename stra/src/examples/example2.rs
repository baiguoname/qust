use serde::{ Serialize, Deserialize };
use qust::prelude::*;

//创建一个条件
#[ta_derive]
pub struct cond_long_open(pub usize, pub usize);

//具体的条件功能
#[typetag::serde]
impl Cond for cond_long_open {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let mean_short = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
        let mean_long = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
        //先计算数据，然后依据这些数据做判断。di.c()表示取数据的收盘价，di.h()、di.t()表示取最高价、时间等。
        //下面就是每次循环的判断，e表示当前循环的点，o表示这个条件有持仓的时候的开仓时间点
        Box::new(move |e: usize, o: usize| {
            e > 0 &&
                mean_short[e - 1] < mean_long[e - 1] &&
                mean_short[e] > mean_long[e]
        })
    }
}

#[ta_derive]
pub struct cond_short_exit(pub usize, pub usize);

#[typetag::serde]
impl Cond for cond_short_exit {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let mean_short = di.c().roll(RollFunc::Mean, RollOps::N(self.0));
        let mean_long = di.c().roll(RollFunc::Mean, RollOps::N(self.1));
        Box::new(move |e: usize, o: usize| {
            mean_short[e] < mean_long[e]
        })
    }
}

/*aaaa
//生成tsig，Dire::Lo表示进场条件是做多，Dire::Sh表示进场条件是做空。第二个参数表示进场条件，第三个表示出场条件
let ptm = (Dire::Lo, cond_long_open(10, 20), cond_short_exit(10, 20)).to_ptm();
let dil = gen_di.get(rl5m.clone());
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec.aplot(4);
let csv_res = pnl_vec.sum();
aaaa*/

