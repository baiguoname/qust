use std::borrow::Cow;

use chrono::NaiveDateTime;
use polars::prelude::*;
use polars::error::PolarsResult;
use qust::prelude::{dt, *};

type cdt = chrono::NaiveDateTime;

pub fn vec_to_dataframe(data: &[v32], index: Option<Vec<cdt>>) -> PolarsResult<DataFrame> {
    let num_cols = data[0].len();

    let mut columns: Vec<Column> = Vec::with_capacity(num_cols);
    if let Some(data) = index {
        let inner_data = Series::new("index".into(), data);
        columns.push(Column::Series(inner_data.into()));
    }

    for col_idx in 0..num_cols {
        let column_data: Vec<f32> = data.iter().map(|row| row[col_idx]).collect();
        columns.push(Column::Series(Series::new((&format!("column_{}", col_idx)).into(), column_data).into()));
    }

    let df = DataFrame::new(columns)?;
    
    Ok(df)
}

fn aaaddd(df: DataFrame) {
    // df.lazy().with_column(col("y").round(decimals))
    todo!();
} 



#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForecastRes {
    pub predict: v32,
    pub y: v32,
    pub t: vdt,
}

impl ForecastRes {

    pub fn to_df(&self) -> PolarsResult<DataFrame> {
        df!(
            "t" => &self.t,
            "predict" => &self.predict,
            "y" => &self.y,
        )
    }
    pub fn cut_counts(&self, m: f32) -> PolarsResult<DataFrame> {
        df!(
            "predict" => self.predict.map(|x| (x * m).round() as i32),
            "y" => self.y.map(|x| *x as i32)
        )?
            .lazy()
            .group_by([col("predict")])
            .agg([
                col("y").mean().alias("mean"),
                col("y").len().alias("num"),
            ])
            .sort(["predict"], Default::default())
            .collect()
    } 

    pub fn to_b_hm<F: Fn(f32, f32) -> Option<f32>>(&self, f: F) -> hm<cdt, f32> {
        self.predict.iter()
            .zip(self.y.iter())
            .zip(self.t.iter())
            .fold(hm::new(), |mut accu, ((x, y), z)| {
                if let Some(d) = f(*x, *y) {
                    accu.insert(*z, d);
                }
                accu
            })
    }

    pub fn y_count(&self) -> Vec<(i32, usize)> {
        let mut v_res = self
            .y
            .map(|x| *x as i32)
            .value_count()
            .into_iter()
            .collect_vec();
        v_res.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        v_res
    }
}

impl IdxOut for ForecastRes {
    fn idx_out(&self, idx: Idx) -> Self {
        Self {
            predict: idx.index_out(&self.predict),
            y: idx.index_out(&self.y),
            t: idx.index_out(&self.t),
        }
    }

    fn get_time_vec(&self) -> std::borrow::Cow<'_, Vec<qust::prelude::dt>> {
        Cow::Borrowed(&self.t)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForecastRes2 {
    pub predict: (v32, v32, v32),
    pub y: v32,
    pub t: vdt,
}

impl ForecastRes2 {

    pub fn to_df(&self) -> PolarsResult<DataFrame> {
        df!(
            "t" => &self.t,
            "predict0" => &self.predict.0,
            "predict1" => &self.predict.1,
            "predict2" => &self.predict.2,
            "y" => &self.y,
        )
    }
    pub fn cut_counts(&self, m: f32) -> PolarsResult<DataFrame> {
        let res = self.to_df()?
            .lazy()
            .with_column(((col("predict2") - col("predict0")) * lit(10.)).round(0).cast(DataType::Int16).alias("predict2"))
            .group_by([col("predict2")])
            .agg([
                col("y").mean().alias("mean"),
                col("y").len().alias("num"),
            ])
            .sort(["predict2"], Default::default())
            .collect()?;
        Ok(res)
    } 

    pub fn drop_simi(&self, n: i64) -> Self {
        let mut simi_vec = Vec::with_capacity(self.t.len());
        let mut last_t = self.t[0];
        simi_vec.push(true);
        for t  in self.t.iter().skip(1) {
            if (*t - last_t).num_seconds() >= n {
                last_t = *t;
                simi_vec.push(true);
            } else {
                simi_vec.push(false);
            }
        }
        self.idx_out(Idx::Bool(simi_vec))
    }

    pub fn to_b_hm<F: Fn(f32, f32) -> Option<f32>>(&self, f: F) -> hm<cdt, f32> {
        self.predict.0.iter()
            .zip(self.predict.2.iter())
            .zip(self.t.iter())
            .fold(hm::new(), |mut accu, ((x, y), z)| {
                if let Some(d) = f(*x, *y) {
                    accu.insert(*z, d);
                }
                accu
            })
    }

    pub fn y_count(&self) -> Vec<(i32, usize)> {
        let mut v_res = self
            .y
            .map(|x| *x as i32)
            .value_count()
            .into_iter()
            .collect_vec();
        v_res.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        v_res
    }
}

impl IdxOut for ForecastRes2 {
    fn idx_out(&self, idx: Idx) -> Self {
        Self {
            predict: (
                idx.index_out(&self.predict.0),
                idx.index_out(&self.predict.1),
                idx.index_out(&self.predict.2),
            ),
            y: idx.index_out(&self.y),
            t: idx.index_out(&self.t),
        }
    }

    fn get_time_vec(&self) -> std::borrow::Cow<'_, Vec<qust::prelude::dt>> {
        Cow::Borrowed(&self.t)
    }
}


pub trait PlFrom<Value> {
    fn pl_from(v: Value) -> Self;
}

pub trait PlInto<Value> {
    fn pl_into(self) -> Value;
}

impl<T, N> PlInto<T> for N
where
    T: PlFrom<N>,
    // N: Sized,
    // T: Sized,
{
    fn pl_into(self) -> T {
        T::pl_from(self)
    }
}

impl PlFrom<i64> for NaiveDateTime {
    fn pl_from(v: i64) -> Self {
        chrono::DateTime::from_timestamp_millis(v).unwrap().naive_local()
    }
}



impl PlFrom<Column> for Vec<dt> {
    fn pl_from(v: Column) -> Self {
        v
            .datetime()
            .unwrap()
            .to_vec()
            .into_iter()
            .map(|x| dt::pl_from(x.unwrap()))
            .collect_vec()
    }
}



impl PlFrom<hm<dt, f32>> for DataFrame 
{
    fn pl_from(v: hm<dt, f32>) -> Self {
        let l = v.len();
        let mut d1 = Vec::with_capacity(l);
        let mut d2 = Vec::with_capacity(l);
        for (ks, vs) in v.into_iter() {
            d1.push(ks);
            d2.push(vs);
        }
        let res = df!(
            "k" => d1,
            "v" => d2,
        ).unwrap();
        res.sort(["k"], Default::default()).unwrap()
    }
}