use serde::{ Serialize, Deserialize };
use qust::prelude::*;


#[ta_derive]
pub struct cond(pub Dire, pub usize, pub usize);

#[typetag::serde]
impl Cond for cond {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let (h, l, c) = (di.h(), di.l(), di.c());
        let ma = c.roll(RollFunc::Mean, RollOps::N(self.1));
        let range = izip!(h.iter(), l.iter()) 
            .map(|(x, y)| x - y)
            .collect_vec();
        let open_d = di.calc(ShiftDays(0, KlineType::Open, IndexSpec::First))[0].clone();
        let up_avg = izip!(h.iter(), c.iter(), open_d.iter())
            .map(|(h, c, open_d)| h - c + open_d)
            .collect_vec()
            .roll(RollFunc::Max, RollOps::N(self.2));
        let low_avg = izip!(c.iter(), l.iter(), open_d.iter())
            .map(|(c, l, open_d)| c - l + open_d)
            .collect_vec()
            .roll(RollFunc::Min, RollOps::N(self.2));
        let median_price = izip!(h.iter(), l.iter())
            .map(|(h, l)| (h + l) / 2.)
            .collect_vec();
        match self.0 {
            Dire::Lo => Box::new(move |e, o| {
                e > 0 &&
                    median_price[e] > h[e - 1] &&
                    range[e] > range[e - 1] &&
                    c[e] > ma[e]
            }),
            Dire::Sh => Box::new(move |e, o| {
                e > 0 &&
                    median_price[e] < l[e - 1] &&
                    range[e] > range[e - 1] &&
                    c[e] < ma[e]
            })
        }
    }
}

pub fn get_ptm(n: usize, m: usize) -> Ptm {
    use crate::econd::*;
    use Dire::*;
    let ptm1 = (Lo, cond(Lo, n, m), cond_out3(Sh, ThreType::Num(0.006))).to_ptm();
    let ptm2 = (Sh, cond(Sh, n, m), cond_out3(Lo, ThreType::Num(0.006))).to_ptm();
    (ptm1, ptm2).to_ptm()
}

/*aaaa
let ptm = get_ptm(300, 300);
let dil = gen_di.get(rl5m.clone());
let pnl_vec = ptm.dil(&dil).calc(cs2.clone());
let string_res = dil;
let plot_res = pnl_vec
    .sum()
    .groupby(vec![2020.to_year().before(), 2020.to_year().after()], |x| x.sum())
    .with_stats()
    .aplot(2);
// let plot_res = pnl_vec.sum().aplot(8);
let csv_res = pnl_vec.sum();
aaaa*/




