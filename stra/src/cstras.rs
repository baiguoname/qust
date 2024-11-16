pub mod prelude {
    pub use serde::{ Serialize, Deserialize };
    pub use qust::prelude::*;
    pub use RollOps::*;
    pub use RollFunc::{ Mean, Min, Max, Std };
    pub use KlineType::{ Open as Op, Close as Cp, High as Hp, Low as Lp };
    pub use IndexSpec::*;
    pub use crate::{ filter::AttachConds, econd, inters::* };
}

pub mod c11 {
    use super::prelude::*;
    use qust_derive::*;

    #[ta_derive2]
    pub struct ta(pub usize, pub usize);

    #[typetag::serde(name = "c11_ta")]
    impl Ta for ta {
        fn calc_di(&self, di: &Di) -> avv32 {
            vec![
                izip!(di.o().iter(), di.h().iter(), di.l().iter(), di.c().iter())
                    .map(|(o, h, l, c)| (o + h + l + c) / 4.0)
                    .collect_vec()
                    .to_arc(),
                di.calc(Tr)[0].clone(),
            ]
        }

        fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
            let avg_value = da[0].roll(RollFunc::Mean, RollOps::N(self.0));
            let shift_value = da[1].roll(RollFunc::Mean, RollOps::N(self.1));
            let dband = izip!(avg_value.iter(), shift_value.iter())
                .map(|(x, y)| x - y)
                .collect_vec();
            let uband = izip!(avg_value.iter(), shift_value.iter())
                .map(|(x, y)| x + y)
                .collect_vec();
            vec![dband, uband]
        }
    }

    #[ta_derive2]
    pub struct cond(pub Dire, pub Pms);

    #[typetag::serde(name = "c11_cond")]
    impl Cond for cond {
        fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
            let (h, l) = (di.h(), di.l());
            let data = di.calc(&self.1);
            let dband = data[0].clone();
            let uband = data[1].clone();
            match self.0 {
                Dire::Lo => Box::new(move |e, _o| h[e] > uband[e]),
                Dire::Sh => Box::new(move |e, _o| l[e] < dband[e]),
            }
        }
    }

    #[ta_derive2]
    pub struct ta_std_change(pub usize, pub usize);

    #[typetag::serde(name = "ta_std_change")]
    impl Ta for ta_std_change {
        fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
            vec![
                da[0].roll(RollFunc::Std, RollOps::N(self.0)),
                da[0].roll(RollFunc::Std, RollOps::N(self.1)),
            ]
        }
    }

    #[ta_derive2]
    pub struct cond_std_change(pub Pms);

    #[typetag::serde(name = "cond_std_change")]
    impl Cond for cond_std_change {
        fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
            let data = di.calc(&self.0);
            let data1 = data[0].clone();
            let data2 = data[1].clone();
            Box::new(move |e, _o| data1[e] > data2[e])
        }
    }

    lazy_static! {
        pub static ref ptm: Ptm = {
            let pms1 = ori + ono + ta(20, 14);
            let cond1 = cond(Dire::Lo, pms1.clone());
            let cond2 = cond(Dire::Sh, pms1);
            let cond3 = cond_std_change(ori + ono + ta_std_change(10, 60));
            (
                (Dire::Lo, msig!(and, cond1, cond3), cond2.clone()).to_ptm(),
                (Dire::Sh, msig!(and, cond2, cond3), cond1.clone()).to_ptm(),
            )
                .to_ptm()
                .attach_conds(or, &econd::price_sby_for_30min, Dire::Sh)
        };
        pub static ref stral1: Stral = {
            let ident_vec = vec![
                eger, MAer, TAer, eber, SAer, hcer, ier, jer, rber, SFer, sser, APer, cer, nier,
            ]
            .map(|x| PconIdent::new(qust::prelude::rl30mday.tri_box(), *x));
            ptm.clone().change_money(M3(1.)).to_stral(&ident_vec)
        };
        pub static ref stral2: Stral = {
            let ident_vec = vec![
                buer, ver, eber, SAer, jmer, rber, SFer, sser, pger, ager, mer, APer, cer, aler,
            ]
            .map(|x| PconIdent::new(crate::prelude::rl30m_all_bare.tri_box(), *x));
            ptm.clone().change_money(M3(1.)).to_stral(&ident_vec)
        };
    }
}

pub mod c12 {
    use super::prelude::*;
    use qust_derive::*;

    #[ta_derive2]
    pub struct ta_lo(pub usize);

    #[ta_derive2]
    pub struct ta_sh(pub usize);

    #[typetag::serde(name = "c12_ta_lo")]
    impl Ta for ta_lo {
        fn calc_di(&self,di: &Di) -> avv32 {
            vec![di.h()]
        }
        
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            let res = izip!(da[0].iter(), da[0].lag(1.).roll(RollFunc::Max, RollOps::InitMiss(self.0)))
                .map(|(x, y)| x - y)
                .collect_vec();
            vec![res]
        }
    }

    #[typetag::serde(name = "c12_ta_sh")]
    impl Ta for ta_sh {
        fn calc_di(&self,di: &Di) -> avv32 {
            vec![di.l()]
        }
        
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            let res = izip!(da[0].iter(), da[0].lag(1.).roll(RollFunc::Min, RollOps::InitMiss(self.0)))
                .map(|(x, y)| x - y)
                .collect_vec();
            vec![res]
        }
    }


    lazy_static! {
        pub static ref ptm: Ptm = {
            let n = 200;
            let cond1 = BandCond(Lo, BandState::Action, ori + ha + ono + ta_lo(n));
            let cond2 = BandCond(Sh, BandState::Action, ori + ha + ono + ta_sh(n));
            let cond3 = BandCond(Sh, BandState::Lieing, ori + ha + ono + ta_sh(n / 2));
            let cond4 = BandCond(Lo, BandState::Lieing, ori + ha + ono + ta_lo(n / 2));
            let ptm1 = (Lo, cond1, cond3).to_ptm();
            let ptm2 = (Sh, cond2, cond4).to_ptm();
            (ptm1, ptm2).to_ptm()
                .attach_conds(or, &(econd::price_sby(Lo, 0.015), econd::price_sby(Sh, 0.015)), Sh)
        };

        pub static ref stral: Stral = {
            let ident_vec = vec![TAer,ver,eber,SAer,hcer,ier,jer,
                rber,scer,mer,sper,aler,cuer,sner]
                .map(|x| PconIdent::new(qust::prelude::rl30mday.tri_box(), *x));
            ptm.clone()
                .change_money(M3(1.))
                .to_stral(&ident_vec)
        };
    }

}

pub mod c18 {
    use super::prelude::*;
    use qust_derive::*;
    
    #[ta_derive2]
    pub struct ta;

    #[typetag::serde(name = "c18_ta")]
    impl Ta for ta {
        fn calc_di(&self,di: &Di) -> avv32 {
            vec![di.c(), di.v()]
        }

        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            let ma = da[0].roll(Mean, N(16));
            let std = da[0].roll(Std, N(16));
            let upline = izip!(ma.iter(), std.iter())
                .map(|(x, y)| x + y)
                .collect_vec();
            let downline = izip!(ma.iter(), std.iter())
                .map(|(x, y)| x - y)
                .collect_vec();
            let rrange = izip!(ma.iter(), std.iter())
                .map(|(x, y)| y / x * 2.)
                .collect_vec();
            let average = rrange.roll(Mean, N(16));
            let avgvol = da[1].roll(Mean, N(16));
            vec![downline, upline, rrange, average, avgvol]
        }
    }
}

pub mod c20 {
    use super::prelude::*;
    use qust_derive::*;

    /// must be with ori + ono
    #[ta_derive2]
    pub struct ta(pub f32);

    #[typetag::serde(name = "c20_ta")]
    impl Ta for ta {
        fn calc_da(&self,  _da:Vec< &[f32]> , di: &Di) -> vv32 {
            let jo = &di.calc(DayKlineWrapper(KlineType::Open))[0];
            // let jh = di.calc(DayKlineWrapper(KlineType::High));
            // let jl = di.calc(DayKlineWrapper(KlineType::Close));
            let zc = &di.calc(ShiftDays(1, KlineType::Close, IndexSpec::Last))[0];
            let zh = &di.calc(ShiftDays(1, KlineType::High, IndexSpec::Max))[0];
            let zl = &di.calc(ShiftDays(1, KlineType::Low, IndexSpec::Min))[0];
            let (tmp1, tmp2) = izip!(zc.iter(), zl.iter(), zh.iter(), jo.iter())
                .fold((vec![], vec![]), |mut accu, (zc_, zl_, zh_, jo_)| {
                    let band = (zc_ - zl_).max(zh_ - zc_).max(jo_ * 0.01) * self.0;
                    accu.0.push(jo_ - band);
                    accu.1.push(jo_ + band);
                    accu
                });
            vec![tmp1, tmp2]
        }
    }

    #[ta_derive2]
    pub struct cond(pub Dire, pub ta);

    #[typetag::serde(name = "c20_cond")]
    impl Cond for cond {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let data = di.calc(&self.1);
            let c = di.c();
            match self.0 {
                Dire::Sh => Box::new(move |e, _o| c[e] < data[1][e]),
                Dire::Lo => Box::new(move |e, _o| c[e] > data[0][e]) ,
            }
        }
    }
}

pub mod cold {

    use super::prelude::*;
    use qust_derive::*;


    #[ta_derive2]
    pub struct ta;

    #[typetag::serde(name = "cold_ta")]
    impl Ta for ta {
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            let std = da[0].roll(Std, N(20));
            let std_change = izip!(std.iter(), std.lag(1.).iter())
                .map(|(x, y)| {
                    (((y / x + 1.) * 20.) as usize).clamp(20, 60)
                })
                .collect_vec();
            let win = Vary(Box::new(std_change));
            let res_mean = da[0].roll(Mean, &win);
            let res_std = da[0].roll(Std, &win);
            let res_low_min = da[0].roll(Min, &win);
            let res_high_max = da[0].roll(Max, &win);
            let res_bottom_bound = izip!(res_mean.iter(), res_std.iter())
                .map(|(x, y)| x - 2. * y)
                .collect_vec();
            let res_top_bound = izip!(res_mean.iter(), res_std.iter())
                .map(|(x, y)| x + 2. * y)
                .collect_vec();
            vec![res_mean, res_std, res_low_min, res_high_max, res_bottom_bound, res_top_bound]
        }
    }

    #[ta_derive2]
    pub struct cond(pub Dire, pub Trading);

    #[typetag::serde(name = "cond_cold")]
    impl Cond for cond {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let (h, l, c) = (di.h(), di.l(), di.c());
            let data = di.calc(ta);
            match (self.0, self.1) {
                (Lo, Trading::Open) => Box::new(move |e, _o| {
                    h[e] >= data[3][e] &&
                    h[e] > data[5][e]
                }),
                (Sh, Trading::Open) => Box::new(move |e, _o| {
                    l[e] <= data[2][e] &&
                    l[e] < data[4][e]
                }),
                (Sh, Trading::Exit) => Box::new(move |e, _o| {
                    c[e] < data[0][e]
                }),
                (Lo, Trading::Exit) => Box::new(move |e, _o| {
                    c[e] > data[0][e]
                }),
            }
        }
    }

    pub const cond1: cond = cond(Lo, Trading::Open);
    pub const cond2: cond = cond(Sh, Trading::Open);
    pub const cond3: cond = cond(Sh, Trading::Exit);
    pub const cond4: cond = cond(Lo, Trading::Exit);

}

pub mod c52 {
    use super::prelude::*;
    use qust_derive::*;

    #[ta_derive2]
    pub struct ta;

    #[typetag::serde(name = "ta_c52")]
    impl Ta for ta {
        fn calc_da(&self,_da:Vec< &[f32]>, di: &Di) -> vv32 {
            let oo = &di.calc(ShiftDays(0, Op, First))[0];
            let ma1 = di.c().lag(1.).ema(50);
            let rangeb = izip!(di.h().iter(), di.l().iter())
                .map(|(x, y)| x - y)
                .collect_vec();
            let upavg = izip!(di.h().iter(), di.c().iter(), oo.iter())
                .map(|(h, c, o)| h - c + o)
                .collect_vec()
                .roll(Max, N(24));
            let lowavg = izip!(di.l().iter(), di.c().iter(), oo.iter())
                .map(|(l, c, o)| l - c + o)
                .collect_vec()
                .roll(Min, N(24));
            let median_price = izip!(di.h().iter(), di.l().iter())
                .map(|(x, y)| (x + y) / 2.)
                .collect_vec();
            let exavg = median_price.lag(5.).roll(Mean, N(24));
            vec![rangeb, upavg, lowavg, median_price, exavg, ma1]
        }
    }
}

pub mod c33 {
    use super::prelude::*;
    use qust_derive::*;

    #[ta_derive2]
    pub struct ta;

    #[typetag::serde(name = "ta_c33")]
    impl Ta for ta {
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            let pubu1 = da[0].ema(15);
            let pubu2 = da[0].ema(25);
            let pubu3 = da[0].ema(45);
            vec![pubu1, pubu2, pubu3]
        }
    }
}

pub mod c53 {
    use super::prelude::*;
    use qust_derive::*;

    #[ta_derive2]
    pub struct ta;

    #[typetag::serde(name = "cond_c53")]
    impl Ta for ta {
        fn calc_di(&self,di: &Di) -> avv32 {
            vec![di.h(), di.l(), di.c()]
        }
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            let mas = da[2].ema(100);
            let upperc = da[0].roll(Max, N(50));
            let lowerc = da[1].roll(Min, N(50));
            vec![lowerc, mas, upperc]
        }
    }
}

pub mod merge {
    use super::prelude::*;
    use super::*;

    lazy_static! {

        pub static ref ptm_bare: Ptm = {
            let pms   = ori + ono + c11::ta(20, 14);
            let cond1 = c11::cond(Lo, pms.clone());
            let cond2 = c11::cond(Sh, pms);

            let cond3 = BandCond(Lo, BandState::Action, ori + ha + ono + c12::ta_lo(200));
            let cond4 = BandCond(Sh, BandState::Action, ori + ha + ono + c12::ta_sh(200));

            let cond5 = BandCond(Lo, BandState::Lieing, ori + ha + ono + c12::ta_lo(200 / 2));
            let cond6 = BandCond(Sh, BandState::Lieing, ori + ha + ono + c12::ta_sh(200 / 2));

            let cond7 = cold::cond1;
            let cond8 = cold::cond2;

            let cond_open_lo = msig!(and, cond1, cond3, cond5, cond7);
            let cond_open_sh = msig!(and, cond2, cond4, cond6, cond8);

            let cond_exit_sh = cond6;
            let cond_exit_lo = cond5;
            
            let ptm1 = (Lo, cond_open_lo, cond_exit_sh).to_ptm();
            let ptm2 = (Sh, cond_open_sh, cond_exit_lo).to_ptm();
            (ptm1, ptm2).to_ptm().change_money(M3(1.))
        };

        pub static ref tickers: Vec<Ticker> = vec![
            pper, cer, MAer, fuer, APer, sner, jmer, aler, sper, cuer, 
            mer, scer, rber, TAer, hcer, ier, ver, SAer, eber, jer];

        pub static ref ptm: Ptm = ptm_bare
            .attach_conds(or, &(econd::price_sby(Lo, 0.010), econd::price_sby(Sh, 0.010)), Sh);

        pub static ref stral: Stral = {
            let ident_vec = tickers.clone()
                .into_iter()
                .map(|x| PconIdent::new(qust::prelude::rl30mday.tri_box(), x))
                .collect_vec();
            ptm.clone().to_stral(&ident_vec)
        };
    }

    lazy_static! {
        pub static ref cond_open: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = vec![
            {
                let pms = ori + ono + c11::ta(20, 14);
                (c11::cond(Lo, pms.clone()).to_box(), c11::cond(Sh, pms).to_box())
            },
            {
                let x = c11::cond_std_change(ori + ono + c11::ta_std_change(10, 60)).to_box();
                (x.clone(), x)
            },
            {
                let n = 200;
                let cond1 = BandCond(Lo, BandState::Action, ori + ha + ono + c12::ta_lo(n));
                let cond2 = BandCond(Sh, BandState::Action, ori + ha + ono + c12::ta_sh(n));
                (cond1.to_box(), cond2.to_box())
            },
            {
                let n = 200;
                let cond4 = BandCond(Lo, BandState::Lieing, ori + ha + ono + c12::ta_lo(n / 2));
                let cond3 = BandCond(Sh, BandState::Lieing, ori + ha + ono + c12::ta_sh(n / 2));
                (cond4.to_box(), cond3.to_box())
            },
            {
                let cond1 = c20::cond(Lo, c20::ta(0.6));
                let cond2 = c20::cond(Sh, c20::ta(0.6));
                (cond1.to_box(), cond2.to_box())
            },
            {
                (cold::cond1.to_box(), cold::cond2.to_box())
            }
        ];
        pub static ref cond_exit: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = vec![
            {
                let n = 200;
                let cond4 = BandCond(Lo, BandState::Lieing, ori + ha + ono + c12::ta_lo(n / 2));
                let cond3 = BandCond(Sh, BandState::Lieing, ori + ha + ono + c12::ta_sh(n / 2));
                (cond4.to_box(), cond3.to_box())
            },
            {
                let cond1 = c20::cond(Lo, c20::ta(0.6));
                let cond2 = c20::cond(Sh, c20::ta(0.6));
                (cond1.to_box(), cond2.to_box())
            },
            {
                (cold::cond4.to_box(), cold::cond3.to_box())
            },
            {
                econd::price_sby_for_30min.pip(|x| (x.0.to_box(), x.1.to_box()))
            },
            {
                (econd::price_sby(Sh, 0.010).to_box(), econd::price_sby(Lo, 0.010).to_box())
            },
            {
                (econd::price_sby(Sh, 0.020).to_box(), econd::price_sby(Lo, 0.020).to_box())
            },
        ];
    }

}
