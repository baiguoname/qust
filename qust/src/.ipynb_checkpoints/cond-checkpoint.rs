use crate::di::Di;
use crate::types::*;
use chrono::Timelike;
use serde::{Deserialize, Serialize};

pub type LoopSig<'a> = Box<dyn Fn(usize, usize) -> bool + 'a>;

/* #region cond trait */
pub trait CondClone: Send + Sync {
    fn clone_box(&self) -> Box<dyn Cond>;
}

pub trait Cond: CondClone + 'static {
    fn calc_init(&self, _di: &mut Di) {}
    fn cond<'a>(&self, data: &'a Di) -> LoopSig<'a>;
}

impl<T> CondClone for T
where
    T: Cond + Clone + Send + Sync + 'static,
{
    fn clone_box(&self) -> Box<dyn Cond> {
        Box::new(self.clone())
    }
}
/* #endregion */

/* #region filter day */
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filterday;

impl Cond for Filterday {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data: Vec<tt> = di
            .t()
            .iter()
            .map(|x| tt::from_hms_opt(x.hour(), x.minute(), x.second()).unwrap())
            .collect();
        Box::new(move |e: usize, _o: usize| data[e] >= tt::from_hms_opt(14, 56, 50).unwrap())
    }
}

/* #endregion */

/* #region msig */
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicOps {
    And,
    Or,
}

impl LogicOps {
    fn evaluate(self, x: bool, y: bool) -> bool {
        match self {
            LogicOps::And => x && y,
            LogicOps::Or => x || y,
        }
    }
}

#[derive(Clone)]
pub struct Msig<T: Cond, N: Cond>(pub LogicOps, pub T, pub N);


impl<T: Cond + Clone, N: Cond + Clone> Cond for Msig<T, N> {
    fn calc_init(&self, di: &mut Di) {
        self.1.calc_init(di);
        self.2.calc_init(di);
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let f = self.0;
        let cond1 = self.1.cond(di);
        let cond2 = self.2.cond(di);
        Box::new(move |e, o| f.evaluate(cond1(e, o), cond2(e, o)))
    }
}
/* #endregion */


use crate::calc::Calc;

#[derive(Clone)]
pub struct Iocond<T> {
    pub pms: T,
    pub range: std::ops::Range<f32>,
}

impl<T: Calc<R = vv32> + Clone + 'static + Send + Sync> Cond for Iocond<T> {
    fn calc_init(&self, di: &mut Di) {
        di.calc_init(&self.pms);
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = &di.calc_save(&self.pms)[0];
        let range = &self.range;
        let bound_start = range.start;
        let bound_end = range.end;
        Box::new(move |e, _o| {
            let d = data[e];
            d >= bound_start && d < bound_end
        })
    }
}
