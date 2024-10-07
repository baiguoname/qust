#![allow(dead_code)]
use super::pms::*;
use crate::idct::dcon::Convert;
use crate::sig::livesig::LiveSig;
use crate::std_prelude::*;
use crate::trade::di::{Di, PriceArc};
use qust_derive::ta_derive;
use qust_derive::AsRef;
use qust_ds::prelude::*;
use dyn_clone::{clone_trait_object, DynClone};
use std::any::Any;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

/* #region Calc Type */
pub trait Calc<R>: DynClone + Send + Sync + Debug + 'static {
    fn calc(&self, di: &Di) -> R;
    fn to_box(&self) -> Box<dyn Calc<R>>
    where
        Self: Clone + 'static,
    {
        Box::new(self.clone())
    }
    fn id(&self) -> String {
        format!("{:?}", self)
    }
}
clone_trait_object!(<R> Calc<R>);

impl<R: 'static> Hash for dyn Calc<R> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id().hash(state)
    }
}

impl<R: 'static> PartialEq for dyn Calc<R> {
    fn eq(&self, other: &(dyn Calc<R>)) -> bool {
        self.id() == other.id()
    }
}

impl<R: 'static> Eq for dyn Calc<R> {}
/* #endregion */

impl Calc<PriceArc> for Convert {
    fn calc(&self, di: &Di) -> PriceArc {
        if di
            .data_save
            .save_dcon
            .read()
            .unwrap()
            .contains_key(&self.to_box())
        {
            di.data_save.save_dcon.read().unwrap()[&self.to_box()].clone()
        } else {
            let price_pre = self.get_pre(di);
            let res = self.convert(price_pre, di);
            di.data_save
                .save_dcon
                .write()
                .unwrap()
                .insert(self.to_box(), res.clone());
            res
        }
    }
}

impl Calc<avv32> for Pms {
    fn calc(&self, di: &Di) -> avv32 {
        if di
            .data_save
            .save_pms2d
            .read()
            .unwrap()
            .contains_key(&self.to_box())
        {
            di.data_save.save_pms2d.read().unwrap()[&self.to_box()].clone()
        } else {
            di.dcon.write().unwrap().push(self.dcon.clone());
            di.part.write().unwrap().push(self.part.clone());
            self.fore.start(di);
            let res = di
                .last_part()
                .calc_part(di, self.fore.clone())
                .into_iter()
                .map(Arc::new)
                .collect_vec();
            self.fore.end(di);
            di.dcon.write().unwrap().pop();
            di.part.write().unwrap().pop();
            di.data_save
                .save_pms2d
                .write()
                .unwrap()
                .insert(self.to_box(), res.clone());
            res
        }
    }
}

impl<T: GetPmsFromTa> Calc<avv32> for T {
    fn calc(&self, di: &Di) -> avv32 {
        self.get_pms_from_ta(di).calc(di)
    }
}

impl<T, R> Calc<Arc<BoxAny>> for T
where
    T: LiveSig<R = R> + Clone + Debug,
    R: Send + Sync + 'static,
{
    fn calc(&self, di: &Di) -> Arc<BoxAny> {
        if di
            .data_save
            .save_livesig
            .read()
            .unwrap()
            .contains_key(&self.to_box())
        {
            let data = di.data_save.save_livesig.read().unwrap()[&self.to_box()].clone();
            self.update(di, data.downcast_ref::<RwLock<R>>().unwrap());
            data
        } else {
            let res = self.get_data(di);
            self.update(di, &res);
            let data: Arc<BoxAny> = Arc::new(Box::new(res));
            di.data_save
                .save_livesig
                .write()
                .unwrap()
                .insert(self.to_box(), data.clone());
            data
        }
    }
}

// impl<T, R> Calc<Arc<BoxAny>> for T
// where
//     T: LiveSig<R = R> + Clone + Debug,
//     R: Send + Sync + 'static,
// {
//     fn calc(&self, di: &Di) -> Arc<BoxAny> {
//         if di.data_save.save_livesig.read().unwrap().contains_key(&self.to_box()) {
//             let data = di.data_save.save_livesig.read().unwrap()[&self.to_box()].clone();
//             {
//                 let mut f = self.update2(di, data.downcast_ref::<RwLock<R>>().unwrap());
//                 f(di);
//             }
//             data
//         } else {
//             let res = self.get_data(di);
//             {
//                 let mut f = self.update2(di, &res);
//                 f(di);
//             }
//             // self.update(di, &res);
//             let data: Arc<BoxAny> = Arc::new(Box::new(res));
//             di.data_save.save_livesig.write().unwrap().insert(self.to_box(), data.clone());
//             data
//         }
//     }
// }

pub trait CalcSave: Clone + Debug + Send + Sync + 'static {
    type Output;
    fn calc_save(&self, di: &Di) -> Self::Output;
}

#[ta_derive]
pub struct CalcSaveWrapper<T>(pub T);

impl<T, R> Calc<ABoxAny> for CalcSaveWrapper<T>
where
    T: CalcSave<Output = R>,
    R: Send + Sync + 'static,
{
    fn calc(&self, di: &Di) -> ABoxAny {
        if di
            .data_save
            .save_others
            .read()
            .unwrap()
            .contains_key(&self.to_box())
        {
            di.data_save.save_others.read().unwrap()[&self.to_box()].clone()
        } else {
            let res = self.0.calc_save(di);
            let data: Arc<BoxAny> = Arc::new(Box::new(res));
            di.data_save
                .save_others
                .write()
                .unwrap()
                .insert(self.to_box(), data.clone());
            data
        }
    }
}

/* #region DataSave */
pub type BoxAny = Box<dyn Any + Sync + Send>;
pub type ABoxAny = Arc<BoxAny>;
type Hmt<T> = hm<Box<dyn Calc<T>>, T>;
#[derive(Default)]
pub struct DataSave {
    pub save_dcon: RwLock<Hmt<PriceArc>>,
    pub save_pms2d: RwLock<Hmt<avv32>>,
    pub save_livesig: RwLock<Hmt<ABoxAny>>,
    pub save_others: RwLock<Hmt<ABoxAny>>,
    pub save_any: RwLock<hm<String, ABoxAny>>,
}

impl DataSave {
    pub fn len(&self) -> Vec<usize> {
        vec![
            self.save_dcon.read().unwrap().len(),
            self.save_pms2d.read().unwrap().len(),
            self.save_livesig.read().unwrap().len(),
            self.save_others.read().unwrap().len(),
        ]
    }
    pub fn len_sum(&self) -> usize {
        self.len().iter().sum::<usize>()
    }
    pub fn clear(&self) {
        self.save_dcon.write().unwrap().clear();
        self.save_pms2d.write().unwrap().clear();
        self.save_livesig.write().unwrap().clear();
        self.save_others.write().unwrap().clear();
    }

    pub fn print_keys(&self) {
        self.save_dcon.read().unwrap().keys().print();
        self.save_pms2d.read().unwrap().keys().print();
        self.save_livesig.read().unwrap().keys().print();
        self.save_others.read().unwrap().keys().print();
    }

    pub fn clear_with_condition(&self) {
        if self.save_dcon.read().unwrap().len() > 15 {
            self.save_dcon.write().unwrap().clear();
        }
        if self.save_pms2d.read().unwrap().len() > 150 {
            self.save_pms2d.write().unwrap().clear();
        }
        if self.save_livesig.read().unwrap().len() > 150 {
            self.save_livesig.write().unwrap().clear();
        }
        if self.save_others.read().unwrap().len() > 15 {
            self.save_others.write().unwrap().clear();
        }
    }
}
/* #endregion */
