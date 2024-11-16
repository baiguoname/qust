#![allow(unused_imports)]
use std::ops::Index;
use std::process::id;
use std::sync::{Arc, RwLock};
use crate::loge;
use crate::prelude::{Algo, ApiBridgeBox, Di, DiStral, Dire, KlineData, NormHold, OrderError, Stra, Stral, TickData, Ticker };
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use super::super::order_types::*;
use super::prelude::TradeOne;
use crate::sig::livesig::Ptm;
use crate::sig::posi::PtmResState;
use super::super::order_types::*;
use super::super::bt::*;
use super::super::live_ops::*;




#[derive(Debug, Clone)]
pub struct DiKlineO<'a> {
    pub di_kline: DiKline<'a>,
    pub o: usize,
}

#[derive(Debug, Clone)]
pub struct DiKlineState<'a> {
    pub di_kline: DiKline<'a>,
    pub state: bool,
}

pub struct StreamCondType1<'a> {
    pub stream_api: StreamApiType<'a>,
    pub di_kline_state: DiKlineState<'a>,
} 

pub struct StreamCondType2<'a> {
    pub stream_api: StreamApiType<'a>,
    pub di_kline: DiKline<'a>,
}

#[derive(Debug)]
pub struct StreamPosi<'a> {
    pub di_kline: &'a DiKline<'a>,
    pub norm_hold: &'a NormHold,
}

#[derive(Debug, Clone, Copy)]
pub enum CondStateVar {
    Open,
    Exit,
    No
}

pub type RetFnCondType1<'a> = Box<dyn FnMut(&StreamCondType1) -> OrderTarget + 'a>;
pub type RetFnCondType2 = Box<dyn FnMut(&StreamCondType2) -> OrderTarget + 'static>;
pub type RetFnCondType3<'a> = Box<dyn FnMut(&StreamApiType) -> OrderTarget + 'a>;
pub type RetFnCondType4<'a> = Box<dyn FnMut(&StreamCondType1) -> OrderAction + 'a>;
pub type RetFnCondType5<'a> = Box<dyn FnMut(&DiKline) -> bool + 'a>;
pub type RetFnCondType6<'a> = Box<dyn FnMut(&DiKlineO) -> bool + 'a>;
pub type RetFnCondType7<'a> = Box<dyn FnMut(&TickData) -> OrderTarget + 'a>;
pub type RetFnCondType8<'a> = Box<dyn FnMut(&TickData) -> Option<TradeInfo> + 'a>;
pub type RetFnCondType9<'a> = Box<dyn FnMut(&TickData) -> OrderAction + 'a>;

pub type RetFnPosi<'a> = Box<dyn FnMut(&StreamPosi) -> NormHold + 'a>;
pub type RetFnKtn<'a> = Box<dyn FnMut(&DiKline) -> NormHold + 'a>;
pub type RetFnCondState<'a> = Box<dyn FnMut(&DiKline) -> CondStateVar + 'a>;


#[clone_trait]
pub trait CondType2 {
    fn cond(&self) -> RetFnCondType2;
}

#[lazy_init(data.di_kline_state.di_kline.di)]
pub trait CondType1 {
    fn cond_type1(&self, di: &Di) -> RetFnCondType1;
}

impl CondType1 for Ptm {
    fn cond_type1(&self, _di: &Di) -> RetFnCondType1 {
        match self {
            Ptm::Ptm6(cond_ops) => {
                let mut res = cond_ops.cond();
                Box::new(move |stream_cond_type1| {
                    let stream = StreamCondType2 {
                        stream_api: stream_cond_type1.stream_api.clone(),
                        di_kline: stream_cond_type1.di_kline_state.di_kline.clone(),
                    };
                    res(&stream)

                })
            }
            Ptm::Ptm7(ktn) => {
                let mut ktn_fn = ktn.ktn_lazy();
                Box::new(move |stream_cond_type1| {
                    ktn_fn(&stream_cond_type1.di_kline_state.di_kline).into()
                })
            }
            other => {
                Box::new(move |stream_cond_type1| {
                    let b = stream_cond_type1.di_kline_state.di_kline.di.calc(other);
                    let ptm_res = &b
                        .downcast_ref::<RwLock<PtmResState>>()
                        .unwrap()
                        .read()
                        .unwrap()
                        .ptm_res;
                    ptm_res.0[stream_cond_type1.di_kline_state.di_kline.i].clone().into()
                }) 
            }
        }
        
    }
}

// #[typetag::serde]
impl CondType1 for Stra {
    fn cond_type1(&self, di: &Di) -> RetFnCondType1 {
        self.ptm.cond_type1(di)
    }
}

// #[typetag::serde(name = "dicondvec")]
impl CondType1 for Stral {
    fn cond_type1(&self, _di: &Di) -> RetFnCondType1 {
        let mut ptm_fn_vec = self
            .0
            .iter()
            .map(|x| (x.ident.clone(), x.name.clone(), x.cond_type1_lazy()))
            .collect_vec();
        Box::new(move |stream_cond_type1| {
            let mut live_target = OrderTarget::No;
            ptm_fn_vec
                .iter_mut()
                .for_each(|(ident, stra_name, ptm_fn)| {
                    let live_target_stra = ptm_fn(stream_cond_type1);
                    live_target = live_target.add_live_target(&live_target_stra);
                    loge!(ident.ticker, "{:?} -- {:?} calced ptm res: {:?}", ident.inter, stra_name, live_target);
                });
            live_target
        })

    }
}

pub trait CondType3 {
    fn cond_type3<'a>(&'a self, di: &'a RwLock<Di>) -> RetFnCondType3<'a>;
}

impl<T> CondType3 for T 
where
    T: CondType1,
{
    fn cond_type3<'a>(&'a self, di: &'a RwLock<Di>) -> RetFnCondType3<'a> {
        let mut di = di.write().unwrap();
        let pcon_ident = di.pcon.ident();
        let mut ptm_fn = self.cond_type1(&di);
        let mut update_tick_fn = pcon_ident.inter.update_tick_func(pcon_ident.ticker);
        let mut last_update_tick_time = Default::default();//maybe the update come from hold update
        Box::new(move |stream_api| {
            let is_finished = if stream_api.tick_data.t > last_update_tick_time {
                last_update_tick_time = stream_api.tick_data.t;
                update_tick_fn(stream_api.tick_data, &mut di.pcon.price).into()
            } else {
                false
            };
            if is_finished {
                di.clear2();
                loge!(pcon_ident.ticker, "{:?} pcon finished", pcon_ident.inter);
            }
            let stream_cond_type1 = StreamCondType1 {
                stream_api: stream_api.clone(),
                di_kline_state: DiKlineState { 
                    di_kline: DiKline { di: &di, i: di.size() - 1 }, 
                    state: is_finished 
                },
            };
            ptm_fn(&stream_cond_type1)
        })
    }

}

#[clone_trait]
pub trait CondType4 {
    fn cond_type4(&self, di: &Di) -> RetFnCondType4;
}


#[lazy_init(data.di)]
#[clone_trait]
pub trait CondType5 {
    fn cond_type5(&self, di: &Di) -> RetFnCondType5; 
}


#[lazy_init(data.di_kline.di)]
#[clone_trait]
pub trait CondType6 {
    fn cond_type6(&self, di: &Di) -> RetFnCondType6; 
}

// #[clone_trait]
pub trait CondType7: Send + Sync + std::fmt::Debug {
    fn cond_type7(&self) -> RetFnCondType7;
    fn cond_type7_box(&self) -> Box<dyn CondType7> 
    where
        Self: Clone + 'static,
    {
        Box::new(self.clone())
    }
}

#[lazy_init(data.di_kline.di)]
#[clone_trait]
pub trait Posi {
    fn posi(&self, di: &Di) -> RetFnPosi;
}

#[lazy_init(data.di)]
#[clone_trait]
pub trait Ktn {
    fn ktn(&self, di: &Di) -> RetFnKtn;
}

pub trait CondState {
    fn cond_state(&self, di: &Di) -> RetFnCondState;
}

pub trait CondType8 {
    fn cond_type8(&self) -> RetFnCondType8;
}

pub trait CondType9 {
    fn cond_type9(&self) -> RetFnCondType9;
}

pub trait CondTypeA {
    fn get_ticker(&self) -> Ticker {
        crate::prelude::aler
    }
    fn cond_type_a(&self) -> RetFnCondType3; 
}


impl<T: Algo + Clone> ToStraApi for WithInfo<WithInfo<DiStral<'_>, T>, &hm<Ticker, sstr>> {
    fn to_stra_api(self) -> StraApi {
        let mut res = hm::new();
        for (di, index_vec) in self.data.data.dil.dil.iter().zip(self.data.data.index_vec.into_iter()) {
            let ident = di.pcon.ident();
            let ticker = ident.ticker;
            res.entry(ticker).or_insert_with(Vec::new);
            let res_part = res.get_mut(&ticker).unwrap();
            let stra_part = index_vec
                .index_out(&self.data.data.stral.0)
                .pip(Stral)
                .with_info(RwLock::new(di.clone()));
            res_part.push(stra_part);
        }
        res
            .into_iter()
            .map(|(k, v)| {
                let res = v.with_info(k).with_info(self.data.info.algo_box());
                let trade_one = TradeOne::new(res, k, self.info);
                let trade_one_box: ApiBridgeBox = Box::new(trade_one);
                Arc::new(trade_one_box)
            })
            .collect_vec()
            .pip(|x| StraApi { pool: x })
    }
}
