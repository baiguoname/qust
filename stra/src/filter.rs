#![allow(clippy::map_entry, clippy::borrowed_box, unused_macros, non_snake_case)]

use qust::prelude::*;

/* #region FilterCond */
pub trait FilterCond {
    fn filter_cond(&self, cond: &Iocond) -> Self;
}

impl FilterCond for Tsig {
    fn filter_cond(&self, cond: &Iocond) -> Self {
        let tsig_box: Box<Tsig> = Box::new(self.clone());
        Tsig::TsigFilter(tsig_box, cond.clone())
    }
}

impl FilterCond for Stp {
    fn filter_cond(&self, cond: &Iocond) -> Self {
        match self {
            Stp::Stp(tsig) => Stp::Stp(tsig.filter_cond(cond)),
            Stp::StpWeight(tsig, cond_weight) => {
                let stp_new = (**tsig).filter_cond(cond);
                let stp_new = Box::new(stp_new);
                Stp::StpWeight(stp_new, cond_weight.clone())
            }
        }
    }
}

impl FilterCond for Ptm {
    fn filter_cond(&self, cond: &Iocond) -> Self {
        match self {
            Ptm::Ptm1(money, stp) => Ptm::Ptm1(money.clone(), stp.filter_cond(cond)),
            _ => panic!(),
        }
    }
}

/* #endregion */

/* #region WeightCond */
pub trait WeightCond {
    type R;
    fn weight_cond(&self, cond: &CondWeight) -> Self::R;
}

impl WeightCond for Stp {
    type R = Stp;
    fn weight_cond(&self, cond: &CondWeight) -> Self::R {
        match self {
            Stp::Stp(_) => Stp::StpWeight(Box::new(self.clone()), cond.clone()),
            _ => panic!(),
        }
    }
}

impl WeightCond for Ptm {
    type R = Ptm;
    fn weight_cond(&self, cond: &CondWeight) -> Self::R {
        match self {
            Ptm::Ptm1(money, stp) => Ptm::Ptm1(money.clone(), stp.weight_cond(cond)),
            _ => panic!("ddddd"),
        }
    }
}
/* #endregion */

/* #region AttachCond */
pub trait AttachBoxCond {
    fn attach_box_cond(&self, ops: LogicOps, cond: &Box<dyn Cond>, dire: Dire) -> Self;
}

impl AttachBoxCond for Tsig {
    fn attach_box_cond(&self, ops: LogicOps, cond: &Box<dyn Cond>, dire: Dire) -> Self {
        match self {
            Tsig::Tsig(dire1, dire2, cond1, cond2) => {
                let (cond1_new, cond2_new) = match dire {
                    Dire::Lo => {
                        let cond1_new: Box<dyn Cond> = Box::new(
                            MsigType(ops, cond1.clone(), cond.clone())
                        );
                        let cond2_new = cond2.clone();
                        (cond1_new, cond2_new)
                    }
                    Dire::Sh => {
                        let cond1_new = cond1.clone();
                        let cond2_new: Box<dyn Cond> = Box::new(
                            MsigType(ops, cond2.clone(), cond.clone())
                        );
                        (cond1_new, cond2_new)
                    }
                };
                Tsig::Tsig(*dire1, *dire2, cond1_new, cond2_new)
            }
            Tsig::TsigFilter(tsig, iocond) =>
                Tsig::TsigFilter(
                    Box::new((*tsig).attach_box_cond(ops, cond, dire)),
                    iocond.clone()
                ),
            _ => panic!("this is TsigQ"),
        }
    }
}

impl AttachBoxCond for Stp {
    fn attach_box_cond(&self, ops: LogicOps, cond: &Box<dyn Cond>, dire: Dire) -> Self {
        match self {
            Stp::Stp(tsig) => Stp::Stp(tsig.attach_box_cond(ops, cond, dire)),
            Stp::StpWeight(x, y) =>
                Stp::StpWeight(Box::new((**x).attach_box_cond(ops, cond, dire)), y.clone()),
        }
    }
}

impl AttachBoxCond for Ptm {
    fn attach_box_cond(&self, ops: LogicOps, cond: &Box<dyn Cond>, dire: Dire) -> Self {
        match self {
            Ptm::Ptm1(m, stp) => Ptm::Ptm1(m.clone(), stp.attach_box_cond(ops, cond, dire)),
            Ptm::Ptm2(m, stp1, stp2) => Ptm::Ptm2(
                m.clone(),
                stp1.attach_box_cond(ops, cond, dire),
                stp2.attach_box_cond(ops, cond, dire)
            ),
            Ptm::Ptm3(m, d, osig, esig) => {
                match dire {
                    Lo => Ptm::Ptm3(m.clone(), *d, MsigType(ops, osig.clone(), cond.clone()).to_box(), esig.clone()),
                    Sh => Ptm::Ptm3(m.clone(), *d, osig.clone(), MsigType(ops, esig.clone(), cond.clone()).to_box()),
                }
            }
            Ptm::Ptm4(ptm1, ptm2) => {
                Ptm::Ptm4(
                    Box::new((*ptm1).attach_box_cond(ops, cond, dire)),
                    Box::new((*ptm2).attach_box_cond(ops, cond, dire)),
                )
            }
            Ptm::Ptm5(ptm) => {
                Ptm::Ptm5(Box::new((*ptm).attach_box_cond(ops, cond, dire)))
            }
            _ => panic!(),
        }
    }
}

impl AttachBoxCond for Stra {
    fn attach_box_cond(&self, ops: LogicOps, cond: &Box<dyn Cond>, dire: Dire) -> Self {
        Stra::new(self.ident.clone(), self.name.clone(), self.ptm.attach_box_cond(ops, cond, dire))
    }
}
impl AttachBoxCond for Stral {
    fn attach_box_cond(&self, ops: LogicOps, cond: &Box<dyn Cond>, dire: Dire) -> Self {
        Stral(
            self.0
                .iter()
                .map(|x| x.attach_box_cond(ops, cond, dire))
                .collect_vec()
        )
    }
}

pub trait AttachCond: AttachBoxCond + Sized {
    fn attach_cond<T: Cond + Clone>(&self, ops: LogicOps, cond: &T, dire: Dire) -> Self {
        let c: Box<dyn Cond> = Box::new(cond.clone());
        self.attach_box_cond(ops, &c, dire)
    }
}
impl<T: AttachBoxCond> AttachCond for T {}
/* #endregion */

/* #region AttachConds */
pub trait AttachConds {
    fn attach_conds<T: Cond + Clone, N: Cond + Clone>(
        &self,
        ops: LogicOps,
        cond: &(T, N),
        dire: Dire
    ) -> Self;
    fn attach_conds_exit<T, N>(&self, cond: &(T, N)) -> Self
    where
        T: Cond + Clone,
        N: Cond + Clone,
        Self: Sized,
    {
        self.attach_conds(or, cond, Sh)
    }
}

impl AttachConds for Ptm {
    fn attach_conds<T: Cond + Clone, N: Cond + Clone>(
        &self,
        ops: LogicOps,
        cond: &(T, N),
        dire: Dire
    ) -> Self {
        match self {
            Ptm::Ptm2(m, stp1, stp2) => Ptm::Ptm2(
                m.clone(),
                stp1.attach_cond(ops, &cond.0, dire),
                stp2.attach_cond(ops, &cond.1, dire)
            ),
            Ptm::Ptm4(ptm1, ptm2) => Ptm::Ptm4(
                Box::new((*ptm1).attach_cond(ops, &cond.0, dire)),
                Box::new((*ptm2).attach_cond(ops, &cond.1, dire)),
            ),
            _ => panic!("attaching for this ptm and cond is not implemented"),
        }
    }
}
impl AttachConds for Stra {
    fn attach_conds<T: Cond + Clone, N: Cond + Clone>(
        &self,
        ops: LogicOps,
        cond: &(T, N),
        dire: Dire
    ) -> Self {
        Stra::new(self.ident.clone(), self.name.clone(), self.ptm.attach_conds(ops, cond, dire))
    }
}
impl AttachConds for Stral {
    fn attach_conds<T: Cond + Clone, N: Cond + Clone>(
        &self,
        ops: LogicOps,
        cond: &(T, N),
        dire: Dire
    ) -> Self {
        self.0
            .iter()
            .map(|x| x.attach_conds(ops, cond, dire))
            .collect_vec()
            .to_stral_bare()
    }
}
/* #endregion */

pub trait ChangePtm: AsRef<Ptm> {
    fn filter_conds<T: Cond + Clone, N: Cond + Clone>(&self, conds: &(T, N)) -> Ptm {
        self.as_ref().attach_conds(and, conds, Lo)
    }

    fn get_cond(&self, x: &Iocond, y: &CondWeight) -> Ptm {
        self.as_ref().filter_cond(x).weight_cond(y)
    }

    fn get_cond2(&self, x: (&Iocond, &Iocond), y: (&CondWeight, &CondWeight)) -> Ptm {
        match self.as_ref() {
            Ptm::Ptm2(money, stp1, stp2) => {
                Ptm::Ptm2(
                    money.clone(),
                    stp1.filter_cond(x.0).weight_cond(y.0),
                    stp2.filter_cond(x.1).weight_cond(y.1)
                )
            }
            _ => panic!("gggg"),
        }
    }

    fn get_cond3(&self, x: (&CondWeight, &CondWeight)) -> Ptm {
        match self.as_ref() {
            Ptm::Ptm2(money, stp1, stp2) => {
                Ptm::Ptm2(money.clone(), stp1.weight_cond(x.0), stp2.weight_cond(x.1))
            }
            _ => panic!("gggg"),
        }
    }
}
impl ChangePtm for Ptm {}

type Kkk = (Ticker, f32, TriBox, String, (Box<dyn Cond>, Box<dyn Cond>));
pub trait GetParams: AsRef<Stra> {
    fn get_params1(&self) -> Kkk {
        let stra = self.as_ref();
        let weight = stra.ptm.get_money_fn().get_init_weight();
        let ticker = stra.ident.ticker;
        let inter_box = stra.ident.inter.clone();
        let stra_name = "c71_3";
        let condweight = stra.get_condweight();
        (ticker, weight, inter_box, stra_name.to_string(), condweight)
    }
}
impl<T: AsRef<Stra>> GetParams for T {}

pub trait GetCondWeight {
    type Output;
    fn get_condweight(&self) -> Self::Output;
}
impl GetCondWeight for Stra {
    type Output = (Box<dyn Cond>, Box<dyn Cond>);
    fn get_condweight(&self) -> Self::Output {
        match &self.ptm {
            Ptm::Ptm1(_, _) => panic!(),
            Ptm::Ptm2(_, stp1, stp2) => (stp1.get_condweight(), stp2.get_condweight()),
            _ => panic!(),
        }
    }
}
impl GetCondWeight for Stp {
    type Output = Box<dyn Cond>;
    fn get_condweight(&self) -> Self::Output {
        match self {
            Stp::Stp(tsig) => match tsig {
                Tsig::Tsig(_, _, x, _) => x.clone(),
                _ => panic!(),
            },
            Stp::StpWeight(_, _) => panic!(),
        }
    }
}


pub struct LabelObj<T> {
    pub label: usize,
    pub obj: T,
}

