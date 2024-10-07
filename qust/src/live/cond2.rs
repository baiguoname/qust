#![allow(unused_imports, clippy::let_and_return)]
use crate::idct::pms::Pms;
use crate::loge;
use crate::prelude::{Iocond, LogicOps, MsigType, Ptm, ToPtm};
use crate::sig::posi::Dire;
use crate::trade::di::Di;
use chrono::Timelike;
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use std::ops::{BitAnd, BitOr, Not};
use super::cond_ops::*;
use crate::sig::posi::*;


#[typetag::serde]
impl CondType6 for CondType5Box {
    fn cond_type6(&self,di: &Di) -> RetFnCondType6 {
        let mut cond_fn = self.cond_type5(di);
        Box::new(move |di_kline_o| {
            cond_fn(&di_kline_o.di_kline)
        })
    }
}

pub type Msig2 = MsigType<CondType6Box>;

#[typetag::serde]
impl CondType6 for Msig2 {
    fn cond_type6(&self, di_init: &Di) -> RetFnCondType6 {
        let f = self.0;
        let mut cond1 = self.1.cond_type6(di_init);
        let mut cond2 = self.2.cond_type6(di_init);
        Box::new(move |di_kline_o| {
            match f {
                LogicOps::And => cond1(di_kline_o) && cond2(di_kline_o),
                LogicOps::Or => cond1(di_kline_o) || cond2(di_kline_o),
            }
        })
    }
}

#[typetag::serde]
impl CondType6 for Iocond {
    fn cond_type6(&self,_di: &Di) -> RetFnCondType6 {
        let range = self.range.clone();
        Box::new(move |di_kline_o| {
            let data = di_kline_o.di_kline.di.calc(&self.pms);
            range.contains(&data[0][di_kline_o.di_kline.i])
        })
    }
}


#[ta_derive2]
pub struct Posi1(pub f32);


#[typetag::serde]
impl Posi for Posi1 {
    fn posi(&self,_di: &Di) -> RetFnPosi {
        Box::new(move |stream_posi| {
            stream_posi.norm_hold * self.0
        })
    }
}

#[ta_derive2]
pub struct OpenExit {
    pub cond_open: CondType6Box,
    pub cond_exit: CondType6Box,
}

impl CondState for OpenExit {
    fn cond_state(&self, di: &Di) -> RetFnCondState {
        let mut o_fn = self.cond_open.cond_type6(di);
        let mut e_fn = self.cond_exit.cond_type6(di);
        let mut exit_i: Option<usize> = None;
        Box::new(move |di_kline| {
            let res = match exit_i {
                Some(o) => {
                    let di_kline_o = DiKlineO { di_kline: di_kline.clone(),  o };
                    if e_fn(&di_kline_o) {
                        exit_i = None;
                        CondStateVar::Exit
                    } else {
                        CondStateVar::No
                    }
                }
                None => {
                    let di_kline_o = DiKlineO { di_kline: di_kline.clone(), o: di_kline.i };
                    if o_fn(&di_kline_o) && !e_fn(&di_kline_o) {
                        exit_i = Some(di_kline.i);
                        CondStateVar::Open
                    } else {
                        CondStateVar::No
                    }
                }
            };
            // loge!(crate::prelude::aler, "i {} {res:?}", di_kline.i);
            res
        })
    }
}

#[ta_derive2]
pub struct DireOpenExit {
    pub dire: Dire,
    pub posi: PosiBox,
    pub open_exit: OpenExit,
}

#[typetag::serde]
impl Ktn for DireOpenExit {
    fn ktn(&self, di: &Di) -> RetFnKtn {
        let mut cond_state_fn = self.open_exit.cond_state(di);
        let mut posi_fn = self.posi.posi(di);
        let hold_unit = match self.dire {
            Dire::Lo => NormHold::Lo(1.),
            Dire::Sh => NormHold::Sh(1.),
        };
        let mut last_hold = NormHold::No;
        Box::new(move |di_kline| {
            match cond_state_fn(di_kline) {
                CondStateVar::Open => {
                    let stream_posi = StreamPosi { di_kline, norm_hold: &hold_unit };
                    last_hold = posi_fn(&stream_posi);
                }
                CondStateVar::Exit => {
                    last_hold = NormHold::No;
                }
                CondStateVar::No => {}
            }
            last_hold.clone()
        })
    }
}

#[ta_derive2]
pub struct TwoOpenExit {
    pub posi: PosiBox,
    pub open_exit_lo: OpenExit,
    pub open_exit_sh: OpenExit,
}

#[typetag::serde]
impl Ktn for TwoOpenExit {
    fn ktn(&self, di: &Di) -> RetFnKtn {
        let mut cond_state_fn_lo = self.open_exit_lo.cond_state(di);
        let mut cond_state_fn_sh = self.open_exit_sh.cond_state(di);
        let mut posi_fn = self.posi.posi(di);
        let hold_unit_lo = NormHold::Lo(1.);
        let hold_unit_sh = NormHold::Sh(1.);
        let mut last_hold = NormHold::No;
        Box::new(move |di_kline| {
            // loge!(crate::prelude::aler, "a {last_hold:?}");
            match last_hold {
                NormHold::Lo(_) => {
                    if let CondStateVar::Exit = cond_state_fn_lo(di_kline) {
                        last_hold = NormHold::No;
                    }
                }
                NormHold::Sh(_) => {
                    if let CondStateVar::Exit = cond_state_fn_sh(di_kline) {
                        last_hold = NormHold::No;
                    }
                }
                _ => {}
            }
            if let NormHold::No = last_hold {
                match cond_state_fn_lo(di_kline) {
                    CondStateVar::Open => {
                        let stream_posi = StreamPosi { di_kline, norm_hold: &hold_unit_lo };
                        last_hold = posi_fn(&stream_posi);
                    }
                    _ => {
                        if let CondStateVar::Open = cond_state_fn_sh(di_kline) {
                            let stream_posi = StreamPosi { di_kline, norm_hold: &hold_unit_sh };
                            last_hold = posi_fn(&stream_posi);
                        }
                    }
                }
            }
            last_hold.clone()
        })
    }
}

#[ta_derive]
pub enum KtnVar {
    One(DireOpenExit),
    Two(TwoOpenExit),
    Box(KtnBox),
}

impl ToPtm for KtnVar {
    fn to_ptm(self) -> Ptm {
        Ptm::Ptm7(self)
    }
}

pub trait ToKtnVar {
    fn to_ktn(self) -> KtnVar;
}

#[typetag::serde]
impl Ktn for KtnVar {
    fn ktn(&self, di: &Di) -> RetFnKtn {
        match self {
            KtnVar::One(one) => one.ktn(di),
            KtnVar::Two(two) => two.ktn(di),
            KtnVar::Box(ktn_box) => ktn_box.ktn(di),
        }
    }
}

impl ToKtnVar for KtnBox {
    fn to_ktn(self) -> KtnVar {
        KtnVar::Box(self)
    }
}

impl ToKtnVar for DireOpenExit {
    fn to_ktn(self) -> KtnVar {
        KtnVar::One(self)
    }
}

impl ToKtnVar for TwoOpenExit {
    fn to_ktn(self) -> KtnVar {
        KtnVar::Two(self)
    }
}

impl<T, N> ToKtnVar for (Dire, T, N)
where
    T: CondType6 + Clone,
    N: CondType6 + Clone,
{
    fn to_ktn(self) -> KtnVar {
        DireOpenExit {
            dire: self.0,
            posi: Posi1(1.).posi_box(), 
            open_exit: OpenExit { 
                cond_open: self.1.condtype6_box(), 
                cond_exit: self.2.condtype6_box()
            }
        }.to_ktn()
    }
}

impl<T, N> ToKtnVar for (T, N)
where
    T: CondType6 + Clone,
    N: CondType6 + Clone,
{
    fn to_ktn(self) -> KtnVar {
        (Dire::Lo, self.0, self.1).to_ktn()
    }
}

pub struct PtmResRecord {
    pub norm_hold: NormHold,
    pub norm_open: NormOpen,
    pub norm_exit: NormExit,
}

pub trait BtDiKline {
    fn bt_di_kline(&self, di: &Di) -> Vec<PtmResRecord>;
}

impl<T> BtDiKline for T
where
    T: Ktn,
{
    fn bt_di_kline(&self, di: &Di) -> Vec<PtmResRecord> {
        let mut ktn_fn = self.ktn(di);
        let mut last_hold = NormHold::No;
        let mut res = Vec::with_capacity(di.size());
        for i in 0..di.size() {
            let di_kline = DiKline { di, i };
            let norm_hold = ktn_fn(&di_kline);
            let (norm_open, norm_exit) = norm_hold.sub_norm_hold(&last_hold);
            last_hold = norm_hold.clone();
            let res_record = PtmResRecord { norm_hold, norm_open, norm_exit };
            res.push(res_record);
        }
        res
    }
}

impl BitAnd<Box<dyn CondType6>> for Box<dyn CondType6> {
    type Output = Box<dyn CondType6>;
    fn bitand(self, rhs: Self) -> Self::Output {
        MsigType(LogicOps::And, self, rhs).condtype6_box()
    }
}
impl BitOr<Box<dyn CondType6>> for Box<dyn CondType6> {
    type Output = Box<dyn CondType6>;
    fn bitor(self, rhs: Self) -> Self::Output {
        MsigType(LogicOps::Or, self, rhs).condtype6_box()
    }
}
impl<T: CondType6 + Clone> BitAnd<T> for Box<dyn CondType6> {
    type Output = CondType6Box;
    fn bitand(self, rhs: T) -> Self::Output {
        MsigType(LogicOps::And, self, rhs.condtype6_box()).condtype6_box()
    }
}
impl<T: CondType6 + Clone> BitOr<T> for Box<dyn CondType6> {
    type Output = CondType6Box;
    fn bitor(self, rhs: T) -> Self::Output {
        MsigType(LogicOps::Or, self, rhs.condtype6_box()).condtype6_box()
    }
}