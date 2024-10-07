use crate::{
    idct::{
        dcon::Convert::{self, *},
        part::Part,
        ta::{ForeTa, Ta},
    },
    trade::di::Di,
};
use qust_ds::prelude::*;
use qust_derive::*;

// #[ta_derive]
#[derive(Clone, Serialize, Deserialize, AsRef, PartialEq, Eq, Hash)]
pub struct PmsType<T, N> {
    pub dcon: T,
    pub part: Part,
    pub fore: N,
}
pub type Pms = PmsType<Convert, Box<dyn Ta>>;
pub type PmsVert = PmsType<(Convert, Convert), Box<dyn Ta>>;
impl std::fmt::Debug for Pms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} + {:?} + {:?}", self.dcon, self.part, self.fore,)
    }
}
impl std::fmt::Debug for PmsVert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} -> {:?} + {:?} + {:?}",
            self.dcon.0, self.dcon.1, self.part, self.fore,
        )
    }
}

use std::ops::Add;

use super::fore::ForeTaCalc;

impl Add for Convert {
    type Output = Convert;
    fn add(self, rhs: Self) -> Self::Output {
        PreNow(Box::new(self), Box::new(rhs))
    }
}

impl Add<Part> for Convert {
    type Output = Pre;
    fn add(self, rhs: Part) -> Self::Output {
        Pre(self, rhs)
    }
}

#[ta_derive]
pub struct Pre(pub Convert, pub Part);

impl<T: Ta + Clone> Add<T> for Pre {
    type Output = Pms;
    fn add(self, rhs: T) -> Self::Output {
        PmsType {
            dcon: self.0,
            part: self.1,
            fore: Box::new(rhs),
        }
    }
}

impl<T: ForeTaCalc> Add<T> for Pms {
    type Output = Pms;
    fn add(self, rhs: T) -> Self::Output {
        let box_t: Box<dyn ForeTaCalc> = Box::new(rhs);
        let t = ForeTa(self.fore, box_t);
        PmsType {
            dcon: self.dcon,
            part: self.part,
            fore: Box::new(t),
        }
    }
}

/* #region GetPmsFromTa */
pub trait GetPmsFromTa: Send + Sync + std::fmt::Debug + Clone + 'static {
    fn get_pms_from_ta(&self, di: &Di) -> Pms;
}

impl<T: Ta + Clone> GetPmsFromTa for T {
    fn get_pms_from_ta(&self, di: &Di) -> Pms {
        let ta_box: Box<dyn Ta> = Box::new(self.clone());
        ta_box.get_pms_from_ta(di)
    }
}

impl GetPmsFromTa for Box<dyn Ta> {
    fn get_pms_from_ta(&self, di: &Di) -> Pms {
        PmsType {
            dcon: di.last_dcon(),
            part: di.last_part(),
            fore: self.clone(),
        }
    }
}

impl<T: Ta + Clone> GetPmsFromTa for (Part, T) {
    fn get_pms_from_ta(&self, di: &Di) -> Pms {
        let ta_box: Box<dyn Ta> = Box::new(self.1.clone());
        (self.0.clone(), ta_box).get_pms_from_ta(di)
    }
}

impl GetPmsFromTa for (Part, Box<dyn Ta>) {
    fn get_pms_from_ta(&self, di: &Di) -> Pms {
        PmsType {
            dcon: di.last_dcon(),
            part: self.0.clone(),
            fore: self.1.clone(),
        }
    }
}
/* #endregion */
