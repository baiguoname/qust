#![allow(unused_imports)]
use std::fmt::Display;
use num_traits::Num;
use polars_io::SerWriter;
use qust::live::bt::WithDi;
use qust_ds::prelude::*;
use qust::prelude::*;
use std::sync::Arc;
use csv;
use crate::prelude::StatsRes;
use polars::prelude::{ CsvWriter, DataFrame, DataType, Expr };

/* #region To Index */
pub struct Index<T>(pub Vec<T>);
pub trait ToIndex<T> {
    fn to_index(&self) -> Index<T>;
}

impl<T: Num + Clone> ToIndex<T> for [T] {
    fn to_index(&self) -> Index<T> {
        Index(self.to_vec())
    }
}

impl ToIndex<String> for [dt] {
    fn to_index(&self) -> Index<String> {
        Index(self.map(|x| x.debug_string()))
    }
}
impl ToIndex<String> for [da] {
    fn to_index(&self) -> Index<String> {
        Index(self.map(|x| x.debug_string()))
    }
}

/* #endregion */

/* #region To Value */
pub struct Value<T>(pub Vec<Vec<T>>);
pub trait ToValue<N, K> {
    type T;
    fn to_value(&self) -> Value<Self::T>;
}

impl<T: Num + Clone> ToValue<u16, ()> for [T] {
    type T = T;
    fn to_value(&self) -> Value<Self::T> {
        Value(vec![self.to_vec()])
    }
}


impl<T: Clone> ToValue<u32, ()> for [Arc<Vec<T>>] {
    type T = T;
    fn to_value(&self) -> Value<Self::T> {
        Value(self.map(|x| x.to_vec()))
    }
}

impl<N: AsRef<[K]>, K: Clone> ToValue<u64, K> for [N] {
    type T = K;
    fn to_value(&self) -> Value<Self::T> {
        Value(
            self
                .iter()
                .map(|x| x.as_ref().to_vec())
                .collect_vec()
        )
    }
}

pub trait ToValueString {
    fn to_value_string(&self) -> Value<String>;
}
impl<T: std::fmt::Debug> ToValueString for [T] {
    fn to_value_string(&self) -> Value<String> {
        self.map(|x| x.debug_string())
            .pip(|x| Value(vec![x.to_vec()]))
    }
}
/* #endregion */

/* #region To Df */
#[derive(Debug)]
pub struct Df<T, N> {
    pub index: Vec<T>,
    pub value: Vec<N>,
    pub column: Vec<String>,
}

impl<T, N: Clone> Df<T, Vec<N>> {
    pub fn transpose_value(self) -> Self {
        let mut value = self.value.similar_init();
        for x in self.value.into_iter() {
            value
                .iter_mut()
                .zip(x.into_iter())
                .for_each(|(x, y)| {
                    x.push(y);
                });
        }
        Df { value, ..self }
    }
}


pub trait IntoDf {
    type Index;
    type Value;
    fn to_df(self) -> Df<Self::Index, Self::Value>;
}

impl<T, N, K> IntoDf for (Index<T>, Value<N>, Vec<K>)
where
    T: Clone,
    N: Clone,
    K: Display,
{
    type Index = T;
    type Value = Vec<N>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        Df {
            index: self.0.0,
            value: self.1.0,
            column: self.2
                .iter()
                .map(|x| x.to_string())
                .collect(),
        }
    }
}

impl<T, N> IntoDf for (Value<T>, Vec<N>)
where
    T: Clone,
    N: Display,
{
    type Index = usize;
    type Value = Vec<T>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        ((0..self.0.0[0].len()).collect_vec().to_index(), self.0, self.1).to_df()
    }
}

impl<T: Clone, N: Clone> IntoDf for (Index<T>, Value<N>) {
    type Index = T;
    type Value = Vec<N>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        let column: Vec<String> = (0..self.1.0.len()).map(|x| x.to_string()).collect();
        (self.0, self.1, column).to_df()
    }
}
impl<N: Clone> IntoDf for Value<N> {
    type Index = usize;
    type Value = Vec<N>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        ((0..self.0[0].len()).collect::<vuz>().to_index(), self).to_df()
    }
}

impl<T> IntoDf for PnlRes<T> where [T]: ToIndex<String>, T: std::clone::Clone {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        (
            self.0.to_index(),
            self.1.to_value(),
            vec![
                "pnl",
                "profit",
                "money_hold",
                "money_trade",
                "cost_all",
                "comm_all",
                "slip_all",
                "hold"
            ],
        ).to_df()
    }
}

impl IntoDf for PriceOri {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        (
            self.t.to_index(),
            [&self.o, &self.h, &self.l, &self.c, &self.v].to_value(),
            vec!["o", "h", "l", "c", "v"],
        ).to_df()
    }
}
impl IntoDf for PriceTick {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        (
            self.t.to_index(),
            [&self.c, &self.v, &self.ask1, &self.bid1, 
            &self.ask1_v, &self.bid1_v, &self.ct.map(|x| *x as f32)].to_value(),
            vec!["c", "v", "ask1", "bid1", "ask1_v", "bid1_v", "ct"],
        ).to_df()
    }
}
impl IntoDf for PriceArc {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        self.to_price_ori().to_df()
    }
}
impl IntoDf for Di {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        self.pcon.price.to_df()
    }
}
// impl IntoDf for Aee<PriceArc> {
//     type Index = String;
//     type Value = Vec<f32>;
//     fn to_df(self) -> Df<Self::Index, Self::Value> {
//         let open_vec = self.0.ot.map(|x| x.to_string());
//         let mut res = self.0.to_df();
//         res.index = res.index
//             .into_iter()
//             .zip(open_vec)
//             .map(|(x, y)| format!("{:<23} -- {:<23}", y, x))
//             .collect_vec();
//         res
//     }
// }
impl IntoDf for Aee<PriceOri> {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        let (open_vec, pass_last, pass_this) = self
            .0
            .ki
            .iter()
            .fold((vec![], vec![], vec![]), |mut accu, x| {
                accu.0.push(x.open_time);
                accu.1.push(x.pass_last as f32);
                accu.2.push(x.pass_this as f32);
                accu
            });
        let mut res = self.0.to_df();
        res.value.push(pass_last);
        res.value.push(pass_this);
        res.column.extend([String::from("pass_last"), String::from("pass_this")]);
        res.index = res.index
            .into_iter()
            .zip(open_vec)
            .map(|(x, y)| format!("{:<23} -- {:<23}", y.to_string(), x))
            .collect_vec();
        res
    }
}
impl IntoDf for Aee<Di> {
    type Index = String;
    type Value = Vec<f32>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        self.0.pcon.price.pip(Aee).to_df()
    }
}
impl IntoDf for Aee<PriceArc> {
    type Index = String;
    type Value = v32;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        self.0.to_price_ori().pip(Aee).to_df()
    }
}
/* #endregion */

/* #region To Excel */



/* #endregion */
use std::path::Path;
pub trait ToCsv: Sized {
    fn to_csv<P: AsRef<Path>>(self, path: P);
    fn aa(self) { self.to_csv("vision.csv"); }
}

impl<T, K, N> ToCsv for T
where
    T: IntoDf<Index = K, Value = N>,
    Df<K, N>: ToCsv,
{
    fn to_csv<P: AsRef<Path>>(self, path: P) {
        self.to_df().to_csv(path);
    }
}

impl<T: Display, N: Display> ToCsv for Df<T, Vec<N>> {
    fn to_csv<P: AsRef<Path>>(self, path: P) {
        if let Some(x) = path.as_ref().parent() {
            if x.to_str().unwrap() != "" && !x.exists() {
                x.build_an_empty_dir();
            }
        }
        let mut wtr = csv::Writer::from_path(path).unwrap();
        wtr.write_record([vec!["index".to_string()], self.column.clone()].concat()).unwrap();
        for i in 0..self.index.len() {
            let record = self.value
                .iter()
                .fold(vec![self.index[i].to_string()], |mut accu, x| {
                    accu.push(x[i].to_string());
                    accu
                });
            wtr.write_record(&record).unwrap();
        }
    }
}

impl ToCsv for DataFrame {
    fn to_csv<P: AsRef<Path>>(self, path: P) {
        let mut df = self;
        let mut file = std::fs::File::create(path.as_ref()).unwrap();
        CsvWriter::new(&mut file)
            .finish(&mut df)
            .unwrap();
    }
}

impl IntoDf for WithDi<'_, Pms> {
    type Index = String;
    type Value = v32;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        let pms_str = self.data.debug_string();
        let index = if pms_str.contains("FillCon") || pms_str.ends_with("ori"){
            self.info.t().to_index()
        } else {
            self.info.calc(&self.data.dcon).t.to_index()
        };
        (index, self.info.calc(self.data).to_value()).to_df()
    }
}

impl IntoDf for WithDi<'_, PnlRes<dt>> {
    type Index = String;
    type Value = v32;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        let mut df = self.data.to_df();
        let pv = self.info.pcon.ticker.info().pv;
        let num = izip!(df.value[2].iter(), self.info.c().iter())
            .map(|(x, y)| 1000. * x / y / pv)
            .collect_vec();
        df.value.push(num);
        df.column.push("num".into());
        df
    }
}

impl IntoDf for WithDi<'_, Ptm> {
    type Index = String;
    type Value = v32;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        WithInfo { data: self.info.pnl(&self.data, cs2), info: self.info }.to_df()
    }
}

impl IntoDf for StatsRes {
    type Index = String;
    type Value = v32;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        Df {
            index: vec![
                "ret", "sr", "cratio", "profit", "comm", "slip", "to_day", "to_sum", "hold", 
                "std", "mdd"].map(|x| String::from(*x)),
            value: vec![
                self.ret, self.sr, self.cratio, self.profit, self.comm, self.slip, 
                self.to_day, self.to_sum, self.hold, self.std, self.mdd,
            ].pip(|x| vec![x]),
            column: vec![String::from("stats_res")],
        }
    }
}

pub trait ConcatDf {
    type Output;
    fn concat_df(self) -> Self::Output;
}

impl<N: Clone> ConcatDf for Vec<Df<String, Vec<N>>> {
    type Output = Df<usize, Vec<N>>;
    fn concat_df(self) -> Self::Output {
        let l = self.len();
        let mut x = self.into_iter();
        let a = x.next().unwrap();
        let mut a = Df {
            index: (0..l).collect_vec(),
            value: a.value,
            column: a.index,
        };
        for data in x {
            data
                .value
                .into_iter()
                .for_each(|x| a.value.push(x));
        }
        a.transpose_value()
    }
}

impl<T, N> ConcatDf for Vec<(T, Df<String, Vec<N>>)>
where
    T: ToString,
    Vec<Df<String, Vec<N>>>: ConcatDf<Output = Df<usize, Vec<N>>>,
    N: Clone,
{
    type Output = Df<String, Vec<N>>;
    fn concat_df(self) -> Self::Output {
        let (v_index, v_df) = self
            .into_iter()
            .fold((vec![], vec![]), |mut accu, x| {
                accu.0.push(x.0.to_string());
                accu.1.push(x.1);
                accu
            });
        let df = v_df.concat_df();
        Df {
            index: v_index,
            value: df.value,
            column: df.column,
        }
    }
}



pub trait AddCol<T> {
    type Index;
    type Value;
    fn add_col(self, data: T) -> Df<Self::Index, Self::Value>;
}

impl<T, N> AddCol<(&str, T)> for Df<N, T>
where
    T: std::fmt::Debug,
{
    type Index = N;
    type Value = T;
    fn add_col(mut self, data: (&str, T)) -> Df<Self::Index, Self::Value> {
        self.value.push(data.1);
        self.column.push(data.0.to_string());
        self
    }
}

impl<T, N> AddCol<T> for Df<N, T>
where
    T: std::fmt::Debug,
{
    type Index = N;
    type Value = T;
    fn add_col(self, data: T) -> Df<Self::Index, Self::Value> {
        let append_col = self.column.len().debug_string();
        let g = append_col.as_str();
        self.add_col((g, data))
    }
}

impl IntoDf for Aee<Aee<PriceOri>> {
    type Index = String;
    type Value = Vec<String>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        let add_cols = self.0.0.immut_info
            .iter()
            .fold(init_a_matrix(self.0.0.t.len(), self.0.0.immut_info[0].len()), |mut accu, x| {
                let x_ = x.map(|x| x.debug_string());
                izip!(accu.iter_mut(), x_.into_iter())
                    .for_each(|(x, y)| {
                        x.push(y);
                    });
                accu
        });
        let df = self.0.to_df();
        let mut df = Df { index: df.index, value: df.value.into_map(|x| x.into_map(|x| x.debug_string())), column: df.column };
        for add_col in add_cols.into_iter() {
            df = df.add_col(add_col);
        }
        df
    }
}
impl IntoDf for Aee<Aee<Di>> {
    type Index = String;
    type Value = Vec<String>;
    fn to_df(self) -> Df<Self::Index, Self::Value> {
        self.0.0.pcon.price.pip(Aee).pip(Aee).to_df()
    }
}


pub trait EvcxrDisplay {
    fn evcxr_display(&self);
}

impl EvcxrDisplay for DataFrame {
    fn evcxr_display(&self) {
        let mut html = String::new();
        html.push_str("<table>");
        let data = self.debug_string();
        let datas = data.split_once('\n').unwrap();
        println!("{}", datas.0);
        datas
            .1
            .split_once('\n')
            .unwrap()
            .1
            .split('│')
            .for_each(|x| {
                if x.contains('┆') {
                    html.push_str("<tr>");
                    x.split('┆')
                        .for_each(|x| {
                            if !x.contains("---") {
                                html.push_str("<td>");
                                html.push_str(x);
                                html.push_str("</td>");
                            }
                        });
                    html.push_str("</tr>");
                } else if !x.contains("─────") && 
                    !x.contains("═════") &&
                    !x.contains("---") &&
                    x.len() > 2 {
                    html.push_str("<tr>");
                    html.push_str("<td>");
                    html.push_str(x);
                    html.push_str("</td>");
                    html.push_str("</tr>");
                }
            });
        html.push_str("</table>");
        println!("EVCXR_BEGIN_CONTENT text/html\n{}\nEVCXR_END_CONTENT", html)
    }
}
