use std::collections::BTreeMap;

use qust::prelude::*;
use serde::{Serialize, Deserialize};
use qust_derive::*;

#[ta_derive2]
pub struct HotContract {
    pub contract: String,
    pub start_date: da,
    pub end_date: da,
}

#[ta_derive2]
pub struct DiTd {
    pub di: Di,
    #[serde(
        serialize_with = "serialize_vec_da",
        deserialize_with = "deserialize_vec_da"
    )]
    pub td: vda,
    pub hot_contract: HotContract,
    pub valid_range: (usize, usize),
}


#[ta_derive2]
pub struct TickDataTd {
    pub tick_data: TickData,
    pub td: da,
}



#[ta_derive2]
pub struct KlineDataTd {
    pub kline_data: KlineData,
    pub td: da,
}



#[ta_derive2]
pub struct DiContracts {
    pub pool: BTreeMap<String, DiTd>,  
}

#[ta_derive2]
pub struct DiCs {
    pub pool: BTreeMap<Ticker, DiContracts>,
}
