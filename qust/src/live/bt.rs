use qust_ds::prelude::*;
use crate::trade::prelude::*;
use crate::sig::posi::Dire;
use super::order_types::*;

pub type WithDi<'a, T> = WithInfo<T, &'a Di>; 
pub type WithTicker<T> = WithInfo<T, Ticker>;
pub type TickerTradeInfo = WithTicker<Vec<TradeInfo>>;
pub type WithDire<T> = WithInfo<T, Dire>;

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
pub struct StreamApiType<'a> {
    pub tick_data: &'a TickData,
    pub hold: &'a Hold,
}

pub type FnMutBox<'a, T, N> = Box<dyn FnMut(T) -> N + 'a>;
pub type RetFnApi<'a> = Box<dyn FnMut(StreamApiType) -> OrderAction + 'a>;

pub trait BtTick<Input> {
    type Output;
    fn bt_tick(&self, input: Input) -> Self::Output;
}


pub trait BtKline<Input> {
    type Output;
    fn bt_kline(&self, input: Input) -> Self::Output;
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


#[derive(Debug, Clone)]
pub struct TradeInfo {
    pub time: dt,
    pub action: OrderAction,
}
