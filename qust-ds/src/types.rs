#![allow(non_camel_case_types)]
use std::cell::Ref;
use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use std::sync::Arc;

pub type v32 = Vec<f32>;
pub type vv32 = Vec<v32>;
pub type vuz = Vec<usize>; 
pub type R32<'a> = Ref<'a, v32>;
pub type VVa<'a> = Vec<&'a v32>;
pub type dt = NaiveDateTime;
pub type da = NaiveDate;
pub type tt = NaiveTime;
pub type vdt = Vec<dt>;
pub type vda = Vec<da>;
pub type av32 = Arc<v32>;
pub type avv32 = Vec<av32>;
pub type av_v32<'a> = Vec<&'a v32>;
pub type avdt = Arc<vdt>;
pub type avda = Arc<vda>;
pub type vv<T> = Vec<Vec<T>>;
pub type hm<K, V> = std::collections::HashMap<K, V>;