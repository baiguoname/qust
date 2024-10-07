use crate::types::*;
use itertools::Itertools;
use num_traits::Pow;
use serde::{Deserialize, Serialize};
use std::iter::Sum;
use std::ops::{Deref, Sub};
use num_traits::cast::AsPrimitive;

/* #region Agg Func  */

pub trait AggFunc {
    type T;
    fn min(&self) -> Self::T;
    fn max(&self) -> Self::T;
    fn sum(&self) -> Self::T;
    fn mean(&self) -> Self::T;
    fn var(&self) -> Self::T
    where
        Self::T: Sub<Self::T, Output = Self::T> + num_traits::Pow<Self::T, Output = Self::T>;
    fn std(&self) -> Self::T
    where
        Self::T:
            Sub<Self::T, Output = Self::T> + num_traits::Pow<Self::T, Output = Self::T> + From<f32>;
}

impl<E> AggFunc for [E]
where
    E: PartialOrd + Copy + std::ops::Div<Output = E> + 'static,
    for<'l> E: Sum<&'l E>,
    usize: AsPrimitive<E>,
{
    type T = E;
    fn min(&self) -> Self::T {
        *self
            .iter()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap()
    }
    fn max(&self) -> Self::T {
        *self
            .iter()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap()
    }
    fn sum(&self) -> Self::T {
        self.iter().sum()
    }
    fn mean(&self) -> Self::T {
        let len: E = self.len().as_();
        self.sum() / len
    }
    fn var(&self) -> Self::T
    where
        Self::T: Sub<Self::T, Output = Self::T> + num_traits::Pow<Self::T, Output = Self::T>,
    {
        let m = self.mean();
        let var_sum = self
            .iter()
            .map(|x| E::pow(*x - m, 2usize.as_()))
            .collect::<Vec<E>>()
            .sum();
        var_sum / self.len().as_()
    }
    fn std(&self) -> Self::T
    where
        Self::T:
            Sub<Self::T, Output = Self::T> + num_traits::Pow<Self::T, Output = Self::T> + From<f32>,
    {
        E::pow(self.var(), E::from(0.5f32))
    }
}

pub trait RollApply<T, N> {
    fn roll_func(&self, f: fn(x: &[T]) -> T, n: usize) -> Vec<N>;
}

impl RollApply<f32, f32> for [f32] {
    fn roll_func(&self, f: fn(x: &[f32]) -> f32, n: usize) -> Vec<f32> {
        let mut res = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            let start_i = if i < n - 1 { 0 } else { i + 1 - n };
            let res_ = f(&self[start_i..i + 1]);
            res.push(res_);
        }
        res
    }
}

impl<T, N> RollApply<T, Vec<T>> for Vec<N>
where
    [T]: RollApply<T, T>,
    N: AsRef<[T]>,
{
    fn roll_func(&self, f: fn(x: &[T]) -> T, n: usize) -> Vec<Vec<T>> {
        self.iter().map(|x| x.as_ref().roll_func(f, n)).collect()
    }
}

fn _max(data: &[f32]) -> f32 {
    *data
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap()
}

pub trait Roll<T, N> {
    fn roll_max(&self, n: usize) -> Vec<N>
    where
        Self: RollApply<T, N>,
        [T]: AggFunc<T = T>,
    {
        self.roll_func(<[T] as AggFunc>::max, n)
    }
    fn roll_min(&self, n: usize) -> Vec<N>
    where
        Self: RollApply<T, N>,
        [T]: AggFunc<T = T>,
    {
        self.roll_func(<[T] as AggFunc>::min, n)
    }
    fn roll_sum(&self, n: usize) -> Vec<N>
    where
        Self: RollApply<T, N>,
        [T]: AggFunc<T = T>,
    {
        self.roll_func(<[T] as AggFunc>::sum, n)
    }
    fn roll_mean(&self, n: usize) -> Vec<N>
    where
        Self: RollApply<T, N>,
        [T]: AggFunc<T = T>,
    {
        self.roll_func(<[T] as AggFunc>::mean, n)
    }
    fn roll_std(&self, n: usize) -> Vec<N>
    where
        Self: RollApply<T, N>,
        [T]: AggFunc<T = T>,
        T: Sub<T, Output = T> + num_traits::Pow<T, Output = T> + From<f32>,
    {
        self.roll_func(<[T] as AggFunc>::std, n)
    }
}

impl Roll<f32, f32> for [f32] {}
impl Roll<f32, v32> for vv32 {}
impl Roll<f32, v32> for Vec<&[f32]> {}
impl Roll<f32, v32> for Vec<&v32> {}
/* #endregion */

/* #region RollFunc */
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq, Serialize, Deserialize)]
pub enum RollFunc {
    Sum,
    Mean,
    Min,
    Max,
    Var,
    Std,
    Momentum,
    Skewness,
}
use RollFunc::*;

pub trait AggFunc2 {
    type T;
    fn agg(&self, t: RollFunc) -> Self::T;
}

impl AggFunc2 for [f32] {
    type T = f32;
    fn agg(&self, t: RollFunc) -> Self::T {
        match t {
            Sum => self.iter().sum(),
            Mean => self.agg(Sum) / (self.len() as f32),
            Min => *self
                .iter()
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap(),
            Max => *self
                .iter()
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap(),
            Var => {
                let m = self.agg(Mean);
                let var_sum = self.iter().map(|x| f32::pow(*x - m, 2f32)).sum::<f32>();
                var_sum / (self.len() as f32 - 1.)
            }
            Std => f32::pow(self.agg(Var), 0.5),
            Momentum => {
                if self.is_empty() {
                    f32::NAN
                } else {
                    self[self.len() - 1] / self[0] - 1.
                }
            }
            Skewness => {
                if self.len() < 2 {
                    f32::NAN
                } else {
                    let mut cm2 = 0f32;
                    let mut cm3 = 0f32;
                    let m = self.agg(Mean);
                    for i in self {
                        let z = i - m;
                        let z2 = z * z;
                        cm2 += z2;
                        cm3 += z2 * z;
                    }
                    cm3 /= self.len() as f32;
                    cm2 /= self.len() as f32;
                    cm3 / f32::pow(cm2, 1.5f32)
                }
            }
        }
    }
}

/* #region RollCalc */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollOps {
    N(usize),
    InitMiss(usize),
    Vary(Box<Vec<usize>>)
}
impl AsRef<RollOps> for RollOps {
    fn as_ref(&self) -> &RollOps {
        self
    }
}
impl From<usize> for RollOps {
    fn from(value: usize) -> Self {
        RollOps::N(value)
    }
}

impl RollOps {
    pub fn roll(&self, f: RollFunc, data: &[f32]) -> v32 {
        match self {
            RollOps::N(n) => {
                let mut res = vec![f32::NAN; data.len()];
                for i in 0..data.len() {
                    let start_i = if i < *n { 0 } else { i + 1 - n };
                    let data_part = &data[start_i..i + 1];
                    res[i] = data_part.agg(f);
                }
                res
            },
            RollOps::InitMiss(n) => {
                let mut res = RollOps::N(*n).roll(f, data);
                res.iter_mut()
                    .take(n - 1)
                    .for_each(|x| *x = f32::NAN);
                res
            }
            RollOps::Vary(v) => {
                data.rolling(&**v).map(|x| x.agg(f)).collect_vec()
            }
        }
    }
}


pub trait RollCalc<T> {
    fn roll<N: AsRef<RollOps> + Clone>(&self, f: RollFunc, n: N) -> Vec<T>;
}

impl RollCalc<f32> for [f32] {
    fn roll<N: AsRef<RollOps> + Clone>(&self, f: RollFunc, n: N) -> Vec<f32> {
        n.as_ref().roll(f, self)
    }
}

impl<T> RollCalc<v32> for Vec<&T>
where
    T: AsRef<[f32]> + RollCalc<f32> + ?Sized,
{
    fn roll<N: AsRef<RollOps> + Clone>(&self, f: RollFunc, n: N) -> vv32
    {
        self.deref()
            .iter()
            .map(|&x| x.as_ref().roll(f, n.clone()))
            .collect_vec()
    }
}
/* #endregion */

/* #region RollSetp */
#[derive(Debug, Clone, PartialEq, Hash, Eq, Serialize, Deserialize)]
pub struct RollStep(pub usize, pub usize);

impl RollStep {
    pub fn roll(&self, data: &[f32], f: impl Fn(&[f32], usize) -> v32) -> v32 {
        let mut res = Vec::with_capacity(data.len());
        let slice_vec = self.get_slice(data.len());
        for (start_loc, end_loc, start_i) in slice_vec {
            let mut res_step = f(&data[start_loc..end_loc], start_i);
            res.append(&mut res_step);
        }
        res
    }

    pub fn get_slice(&self, n: usize) -> Vec<(usize, usize, usize)> {
        let window = self.0;
        let step = self.1;
        if n <= window {
            vec![(0, n, 0)]
        } else {
            let partition_size = (n - window) / step + 1 + 1;
            let mut res: Vec<(usize, usize, usize)> = Vec::with_capacity(partition_size);
            for i in 0..partition_size {
                let res_i = if i == 0 {
                    (0, window, 0usize)
                } else {
                    let start_loc = i * step;
                    let end_loc = if i == partition_size - 1 {
                        n
                    } else {
                        start_loc + window
                    };
                    let start_i = res[i - 1].1 - start_loc;
                    (start_loc, end_loc, start_i)
                };
                res.push(res_i);
            }
            res
        }
    }
}

/* #endregion */

/* #region Rolling */
use std::iter::Iterator;

#[derive(Clone)]
pub struct MovingWindow<'a, T, N> {
    slice: &'a [T],
    n: N,
    count: usize,
    init_size: usize
}

impl<'a, T> Iterator for MovingWindow<'a, T, usize> {
    type Item = &'a [T];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.n > self.slice.len() {
            if self.count < self.n.min(self.init_size + 1) {
                let ret = Some(&self.slice[..self.count]);
                self.count += 1;
                ret
            } else {
                None
            }
        } else if self.count < self.n {
            let ret = Some(&self.slice[..self.count]);
            self.count += 1;
            ret
        } else {
            let ret = Some(&self.slice[..self.n]);
            self.slice = &self.slice[1..];
            ret
        }
    }
}

impl<'a, 'b, T> Iterator for MovingWindow<'a, T, &'b Vec<usize>> {
    type Item = &'a [T];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.count <= self.init_size {
            let i = self.count;
            let win = self.n[i - 1];
            let start_i = if i < win { 0usize } else { i - win };
            // println!("i: {i}, win: {win}, start_i: {start_i}");
            let ret = Some(&self.slice[start_i..i]);
            self.count += 1;
            ret
        } else {
            None
        }
    }
}

pub trait Rolling {
    type T;
    fn rolling<N>(&self, n: N) -> MovingWindow<Self::T, N>;
}

impl<N> Rolling for [N] {
    type T = N;
    fn rolling<K>(&self, n: K) -> MovingWindow<Self::T, K>
    {
        MovingWindow { slice: self, n, count: 1, init_size: self.len()}
    }
}
/* #endregion */