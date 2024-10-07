// use super::{inter::Tri, ticker::Ticker as TickerV};
// use crate::prelude::{dayk, ha, ori, AsRef, Convert, InterBox, PconIdent as PconIdentV};
// use derive::ta_derive;
// use serde::{Deserialize, Serialize};

// #[ta_derive]
// pub struct PconIdent {
//     pub inter: InterBox,
//     pub ticker: TickerV,
// }

// impl From<PconIdent> for PconIdentV {
//     fn from(value: PconIdent) -> Self {
//         let tri_box = value.inter.tri_box();
//         PconIdentV {
//             inter: tri_box,
//             ticker: value.ticker,
//         }
//     }
// }

// #[ta_derive]
// pub struct Ttt(pub String);

// impl From<Ttt> for PconIdentV {
//     fn from(value: Ttt) -> Self {
//         let res: PconIdent = serde_json::from_str(&value.0).unwrap();
//         res.into()
//     }
// }

// impl From<Ttt> for Convert {
//     fn from(value: Ttt) -> Self {
//         match value.0.as_str() {
//             "ori : Ha(4)" => ori + ha,
//             "ori" => ori,
//             "ori : Rlast" => ori + dayk.clone(),
//             "ori : Rlast : Ha(4)" => ori + dayk.clone() + Convert::Ha(4),
//             "ori : Ha(4) : Rlast" => ori + Convert::Ha(4) + dayk.clone(),
//             "ori : Ha(10) : Rlast" => ori + Convert::Ha(10) + dayk.clone(),
//             "ori : Ha(6)" => ori + Convert::Ha(6),
//             "ori : Ha(10)" => ori + Convert::Ha(10),
//             "ori : Ha(6) : Rlast" => ori + Convert::Ha(6) + dayk.clone(),
//             other => panic!("-------{:?}", other),
//         }
//     }
// }
