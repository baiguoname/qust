use qust::prelude::*;
use crate::prelude::*;

pub fn mut1(cond1: &[(Iocond, Iocond)], cond2: &[(Iocond, Iocond)], ptm: &Ptm) -> Vec<Ptm> {
    cond1.to_vec().into_tuple(cond2.to_vec())
        .product(|(x, y)| (
            msig!(and, x.0, y.0),
            msig!(and, x.1, y.1),
        ))
        .into_tuple(vec![ptm.clone()])
        .product(|(x, y)| {
            y.filter_conds(&(x.0, x.1))
        })
}

lazy_static! {
    pub static ref range_vec: Vec<(std::ops::Range<f32>, std::ops::Range<f32>)> = vec![
        (0f32..15f32, 0f32..15f32),
        (0f32..15f32, 85f32..101f32),
        (25f32..50f32, 50f32..75f32),
        (40f32..60f32, 40f32..60f32),
        (50f32..75f32, 25f32..50f32),
        (85f32..101f32, 0f32..15f32),
        (85f32..101f32, 85f32..101f32),
    ];

    pub static ref range_vec2: Vec<(std::ops::Range<f32>, std::ops::Range<f32>)> = vec![
        (000f32..020f32, 000f32..020f32),
        (000f32..020f32, 080f32..101f32),
        (000f32..050f32, 000f32..050f32),
        (000f32..050f32, 050f32..101f32),
        (000f32..080f32, 000f32..080f32),
        (020f32..101f32, 020f32..101f32),
        (040f32..060f32, 040f32..060f32),
        (050f32..101f32, 050f32..101f32),
        (050f32..101f32, 000f32..050f32),
        (080f32..101f32, 080f32..101f32),
        (080f32..101f32, 000f32..020f32),
    ];

    pub static ref convert_vec: Vec<Pre> = vec![
        ori + oos,
        ori + ha + oos,
    ];

    pub static ref convert_vec_day: Vec<Pre> = vec![
        ori + dayk.clone() + ono,
        ori + dayk.clone() + ha + ono,
    ];

    pub static ref atr_vec: Vec<Atr> = vec![
        Atr(10),
        Atr(25),
        Atr(50),
    ];

    pub static ref rsi_vec: Vec<Rsi> = vec![
        Rsi(10),
        Rsi(30),
        // Rsi(60),
    ];

    pub static ref kdayratio_vec: Vec<KDayRatio> = vec![
        KDayRatio(10),
        KDayRatio(30),
        // KDayRatio(60),
    ];

    pub static ref effratio_vec: Vec<EffRatio> = vec![
        EffRatio(2, 10),
        // EffRatio(2, 25),
        EffRatio(4, 20),
        // EffRatio(4, 50),
    ];

    pub static ref momentum_vec: Vec<RollTa<KlineType>> = vec![
        RollTa(KlineType::Close, RollFunc::Momentum, RollOps::N(10)),
        RollTa(KlineType::Close, RollFunc::Momentum, RollOps::N(30)),
        // RollTa(KlineType::Close, RollFunc::Momentum, RollOps::N(60)),
    ];

    pub static ref kta_vec: Vec<Kta> = vec![
        Kta(20, 40, 10),
    ];

    pub static ref dta_vec: Vec<Dta> = vec![
        Dta(20, 40, 10),
    ];

    pub static ref jta_vec: Vec<Jta> = vec![
        Jta(20, 40, 10),
    ];

    pub static ref macd_vec: Vec<Macd> = vec![
        Macd(10, 25, 10),
        Macd(25, 50, 25),
    ];

    pub static ref rank_vec: Vec<Rank> = vec![
        Rank(400, 100),
    ];

    pub static ref ta_box_vec: Vec<Box<dyn Ta>> = vec![
        Atr(10).ta_box(),
        Atr(25).ta_box(),
        Atr(50).ta_box(),
        Rsi(10).ta_box(),
        Rsi(30).ta_box(),
        KDayRatio(10).ta_box(),
        KDayRatio(30).ta_box(),
        EffRatio(2, 10).ta_box(),
        EffRatio(4, 20).ta_box(),
        RollTa(KlineType::Close, RollFunc::Momentum, RollOps::N(10)).ta_box(),
        RollTa(KlineType::Close, RollFunc::Momentum, RollOps::N(30)).ta_box(),
        Kta(20, 40, 10).ta_box(),
        Dta(20, 40, 10).ta_box(),
        Jta(20, 40, 10).ta_box(),
        Macd(10, 25, 10).ta_box(),
        Macd(25, 50, 25).ta_box(),
        Rankma(10, 20).ta_box(),
        Rankma(25, 50).ta_box(),
    ];

    pub static ref open_long_vec: Vec<Box<dyn Cond>> = vec![
        c25::cond1(ori + ha + oos + c25::ta(10, 0.005)).to_box(),
        c25::cond3(ori + oos + maxta(KlineType::Close, 40)).to_box(),
        c66::cond1(ori + oos + c66::ta(10, 20)).to_box(),
        BandCond(Lo, BandState::Action, ori + oos + c71::ta(0.2)).to_box(),
        BandCond(Lo, BandState::Lieing, ori + oos + EffRatio(2, 10)).to_box(),
        f20::open_long(ori + oos + f20::ta(3, 5)).to_box(),
        cqqq::cond1(ori + oos + cqqq::ta(10)).to_box(),
        macd::cond2(Lo, ori + ha + ono + macd::ta { n0: (24f32 / 0.8f32) as usize, n_atr_d: (15f32 / 0.8f32) as usize }, 10).to_box(),
    ];

    pub static ref open_short_vec: Vec<Box<dyn Cond>> = vec![
        c25::cond2(ori + ha + oos + c25::ta(10, 0.005)).to_box(),
        c25::cond4(ori + oos + maxta(KlineType::Close, 40)).to_box(),
        c66::cond2(ori + oos + c66::ta(10, 20)).to_box(),
        BandCond(Sh, BandState::Action, ori + oos + c71::ta(0.2)).to_box(),
        BandCond(Sh, BandState::Lieing, ori + oos + EffRatio(2, 10)).to_box(),
        f20::open_short(ori + oos + f20::ta(3, 5)).to_box(),
        cqqq::cond2(ori + oos + cqqq::ta(10)).to_box(),
        macd::cond2(Lo, ori + ha + ono + macd::ta { n0: (24f32 / 0.8f32) as usize, n_atr_d: (15f32 / 0.8f32) as usize }, 10).to_box(),
    ];

    pub static ref econd_lo_vec: Vec<Box<dyn Cond>> = vec![
        cond_out(Lo, 50f32).to_box(),
        stop_cond(Lo, ThreType::Num(0.01)).to_box(),
        stop_cond(Lo, ThreType::Percent(0.5)).to_box(),
        cond_out3(Lo, ThreType::Num(0.01)).to_box(),
        cond_out3(Lo, ThreType::Percent(0.5)).to_box(),
    ];

    pub static ref econd_sh_vec: Vec<Box<dyn Cond>> = vec![
        cond_out(Sh, 50f32).to_box(),
        stop_cond(Sh, ThreType::Num(0.01)).to_box(),
        stop_cond(Sh, ThreType::Percent(0.5)).to_box(),
        cond_out3(Lo, ThreType::Num(0.01)).to_box(),
        cond_out3(Lo, ThreType::Percent(0.5)).to_box(),
    ];
}


use std::ops::Range;
type PreAndRangeType = (Pre, Rank, (Range<f32>, Range<f32>));
pub fn get_iocond_vec<T: Ta + Clone>(pre_vec: &[PreAndRangeType], ta_vec: &[T]) -> Vec<(Iocond, Iocond)> {
    pre_vec.to_vec().into_tuple(ta_vec.to_vec())
        .product(|((x1, x2, x3), y)| (
            Iocond { pms: x1.clone() + y.clone() + x2 + vori, range: x3.0 },
            Iocond { pms: x1 + y + x2 + vori, range: x3.1 },
        ))
}

pub fn get_iocond_vec_vec(iocond_vec: &[Vec<(Iocond, Iocond)>], ptm: &Ptm) -> Vec<Ptm> {
    let mut res = vec![];
    for i in 0..iocond_vec.len() - 1 {
        for j in i + 1..iocond_vec.len() {
            let mut res_ = mut1(&iocond_vec[i], &iocond_vec[j], ptm);
            res.append(&mut res_);
        }
    }
    res
}

pub fn get_iocond_vec_stral(iocond_vec: &[Vec<(Iocond, Iocond)>], stral: &Stral) -> Vec<Stral> {
    let mut res = vec![];
    for i in 0..iocond_vec.len() - 1 {
        for j in i + 1..iocond_vec.len() {
            for (iocond_vec1, iocond_vec2) in izip!(iocond_vec[i].iter(), iocond_vec[j].iter()) {
                let stral_new = stral
                    .attach_conds(and, iocond_vec1, Lo)
                    .attach_conds(and, iocond_vec2, Lo);
                res.push(stral_new);
            }
        }
    }
    res
}

type VecBoxCond = [(Box<dyn Cond>, Box<dyn Cond>)];
pub fn merge_cond_vec(ops: LogicOps, cond_open: &VecBoxCond) -> (Box<dyn Cond>, Box<dyn Cond>) {
    cond_open
        .iter()
        .cloned()
        .reduce(|acc, e| {
            let l = MsigType(ops, acc.0, e.0).to_box();
            let s = MsigType(ops, acc.1, e.1).to_box();
            (l, s)
        })
        .unwrap()
}

pub fn _quik_merge_cond(cond_open: &VecBoxCond, cond_exit: &VecBoxCond) -> Ptm {
    let (open_l, open_s) = merge_cond_vec(and, cond_open);
    let (exit_l, exit_s) = merge_cond_vec(or, cond_exit);
    (
        Ptm::Ptm3(m1.clone(), Lo, open_l, exit_s),
        Ptm::Ptm3(m1.clone(), Sh, open_s, exit_l),
    )
        .to_ptm()
}