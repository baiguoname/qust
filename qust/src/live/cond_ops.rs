#![allow(unused_imports)]
use std::sync::RwLock;
use crate::loge;
use crate::prelude::{Di, DiStral, GetCdt, KlineData, NormHold, OnlyOne, OrderError, PconIdent, Stra, Stral, TickData, Ticker};
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use super::order_types::LiveTarget;
use super::prelude::{AlgoBox, HoldLocal, OrderAction, OrderResult};
use crate::sig::livesig::Ptm;
use crate::sig::posi::PtmResState;

#[derive(Clone)]
pub struct DiKline<'a> {
    pub di: &'a Di,
    pub i: usize,
}

impl std::fmt::Debug for DiKline<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let price = &self.di.pcon.price;
        let kline = KlineData {
            t: price.t[self.i],
            o: price.o[self.i],
            h: price.h[self.i],
            l: price.l[self.i],
            c: price.c[self.i],
            v: price.v[self.i],
            ki: price.ki[self.i].clone(),
        };
        write!(f, "{:?}", kline)
    }
}

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

#[derive(Debug, Clone)]
pub struct StreamApiType<'a> {
    pub tick_data: &'a TickData,
    pub hold: &'a HoldLocal,
}

pub struct StreamCondType1<'a> {
    pub stream_api: StreamApiType<'a>,
    pub di_kline_state: DiKlineState<'a>,
} 

pub struct StreamCondType2<'a> {
    pub stream_api: StreamApiType<'a>,
    pub di_kline: DiKline<'a>,
}

pub struct StreamAlgo<'a> {
    pub stream_api: StreamApiType<'a>,
    pub live_target: LiveTarget,
}

#[derive(Debug, Clone)]
pub struct TradeInfo {
    pub time: dt,
    pub action: OrderAction,
}

#[derive(Debug)]
pub struct StreamBtMatch<'a> {
    pub tick_data: &'a TickData,
    pub hold: &'a mut HoldLocal,
    pub order_action: &'a OrderAction,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithMatchBox<T> {
    pub data: T,
    pub match_box: BtMatchBox,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithDiKline<T, N> {
    pub data: T,
    pub di: N,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithTicker<T> {
    pub ticker: Ticker,
    pub data: T,
}

#[derive(Default)]
pub struct LiveStraPool {
    pub data: Vec<WithTicker<Box<dyn ApiType>>>
}

pub type RetFnCondType1<'a> = Box<dyn FnMut(&StreamCondType1) -> LiveTarget + 'a>;
pub type RetFnCondType2 = Box<dyn FnMut(&StreamCondType2) -> LiveTarget + 'static>;
pub type RetFnCondType3<'a> = Box<dyn FnMut(&StreamApiType) -> LiveTarget + 'a>;
pub type RetFnCondType4<'a> = Box<dyn FnMut(&StreamCondType1) -> OrderAction + 'a>;
pub type RetFnCondType5<'a> = Box<dyn FnMut(&DiKline) -> bool + 'a>;
pub type RetFnCondType6<'a> = Box<dyn FnMut(&DiKlineO) -> bool + 'a>;
pub type RetFnCondType7<'a> = Box<dyn FnMut(&TickData) -> LiveTarget + 'a>;
pub type RetFnCondType8<'a> = Box<dyn FnMut(&TickData) -> Option<TradeInfo> + 'a>;
pub type RetFnCondType9<'a> = Box<dyn FnMut(&TickData) -> OrderAction + 'a>;

pub type RetFnApi<'a> = Box<dyn FnMut(StreamApiType) -> OrderAction + 'a>;
pub type RetFnAlgo = Box<dyn FnMut(&StreamAlgo) -> OrderAction +  'static>;
pub type RetFnBtMatch<'a> = Box<dyn FnMut(StreamBtMatch) -> Option<TradeInfo> + 'a>;
pub type RetFnPosi<'a> = Box<dyn FnMut(&StreamPosi) -> NormHold + 'a>;
pub type RetFnKtn<'a> = Box<dyn FnMut(&DiKline) -> NormHold + 'a>;
pub type RetFnCondState<'a> = Box<dyn FnMut(&DiKline) -> CondStateVar + 'a>;


#[clone_trait]
pub trait CondType2 {
    fn cond(&self) -> RetFnCondType2;
    fn to_box(&self) -> Box<dyn CondType2>
    where
        Self: Sized,
    {
        dyn_clone::clone_box(self)
    }
}

// #[clone_trait]
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
            let mut live_target = LiveTarget::No;
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

pub trait ApiType: Send + Sync {
    fn api_type(&self) -> RetFnApi;
    fn api_type_box(&self) -> Box<dyn ApiType>
    where
        Self: Clone + 'static,
    {
        Box::new(self.clone())
    }
}
pub type ApiTypeBox = Box<dyn ApiType>;

#[clone_trait]
pub trait CondType4 {
    fn cond_type4(&self, di: &Di) -> RetFnCondType4;
}


#[clone_trait]
pub trait BtMatch {
    fn bt_match(&self) -> RetFnBtMatch;
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
pub trait CondType7: Send + Sync {
    fn cond_type7(&self) -> RetFnCondType7;
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

// #[clone_trait]
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

pub trait ToLiveStraPool<T> {
    fn to_live_stra_pool(&self, input: T) -> LiveStraPool;
}

impl<T> ToLiveStraPool<Vec<Ticker>> for T
where
    T: ApiType + Clone + 'static,
{
    fn to_live_stra_pool(&self, input: Vec<Ticker>) -> LiveStraPool {
        input
            .into_iter()
            .map(|x| {
                WithTicker { ticker: x, data: self.api_type_box()}
            })
            .collect_vec()
            .pip(|data| LiveStraPool { data })
    }
}
