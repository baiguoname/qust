use crate::prelude::*;
use num_traits::{self};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use num_traits::Num;

/* #region BroadcastIndex */
pub trait BroadcastIndex<T> {
    fn bi(&self, i: usize) -> &T;
}
impl<T: Num> BroadcastIndex<T> for T {
    fn bi(&self, _i: usize) -> &T {
        self
    }
}
impl<T: Num> BroadcastIndex<T> for [T] {
    fn bi(&self, i: usize) -> &T {
        &self[i]
    }
}

impl<T: Num> BroadcastIndex<T> for &Vec<T> {
    fn bi(&self, i: usize) -> &T {
        &self[i]
    }
}
/* #endregion */

/* #region [] methods */
pub trait Unique<'a, T: 'a>: AsRef<[T]> {
    fn unique(&self) -> Vec<T>
    where
        T: PartialEq + Clone,
    {
        let mut res: Vec<T> = vec![];
        for i in self.as_ref().iter() {
            if res.contains(i) {
                continue;
            }
            res.push(i.clone());
        }
        res
    }
    fn unique_by<F, N>(&self, f: F) -> Vec<N>
    where
        F: Fn(&T) -> N,
        N: PartialEq + Clone,
    {
        self.as_ref()
            .iter()
            .map(f)
            .collect_vec()
            .unique()
    }
    fn get_list_index(&self, i: &[usize]) -> Vec<T>
    where
        T: Clone,
    {
        i.iter().map(|x| self.as_ref()[*x].clone()).collect_vec()
    }
    fn position(&self, target: &T) -> usize
    where
        T: PartialEq,
    {
        self.as_ref().iter().position(|x| x == target).unwrap()
    }
    fn filter_position<F: Fn(&T) -> bool>(&self, f: F) -> vuz {
        self.as_ref()
            .iter()
            .enumerate()
            .filter_map(|(i, x)| if f(x) { Some(i) } else { None })
            .collect_vec()
    }
    fn index_out<N: Clone>(&self, data: &[N]) -> Vec<N>
    where
        T: Into<usize> + Clone,
    {
        let index: Vec<usize> = self.as_ref().map(|x| x.clone().into());
        data.get_list_index(&index)
            
    }
    fn value_count(&self) -> HashMap<T, usize>
    where
        T: std::hash::Hash + Eq + Clone,
    {
        let mut res = HashMap::new();
        for x in self.as_ref().iter() {
            if !res.contains_key(x) {
                res.insert(x.clone(), 0);
            }
            *res.get_mut(x).unwrap() += 1;
        }
        res
    }
    fn value_positions(&self) -> HashMap<T, vuz>
    where
        T: std::hash::Hash + Eq + Clone,
    {
        let mut res = HashMap::new();
        for (i, x) in self.as_ref().iter().enumerate() {
            if !res.contains_key(x) {
                res.insert(x.clone(), vec![]);
            }
            res.get_mut(x).unwrap().push(i);
        }
        res
    }
    fn drop_nan(&self) -> Vec<T>
    where
        T: num_traits::Float,
    {
        self.as_ref()
            .iter()
            .filter_map(|x| if x.is_nan() { None } else { Some(*x) })
            .collect_vec()
    }
    fn map<F: Fn(&T) -> N, N>(&self, f: F) -> Vec<N> {
        self.as_ref().iter().map(f).collect_vec()
    }
    fn into_map<F: Fn(T) -> N, N>(self, f: F) -> Vec<N>
    where
        Self: IntoIterator<Item = T> + Sized,
    {
        self.into_iter().map(f).collect_vec()
    }
    fn filter_map<F>(&self, f: F) -> Vec<T>
    where
        F: Fn(&T) -> bool,
        T: Clone,
    {
        self
            .as_ref()
            .iter()
            .filter_map(|x| if f(x) { Some(x.clone()) } else { None })
            .collect_vec()
    }
    fn to_ref(&self) -> Vec<&T> {
        self.as_ref().iter().collect_vec()
    }
    fn nlast(&self, n: usize) -> &[T] {
        let l = self.as_ref().len();
        &self.as_ref()[(l - l.min(n)) .. l]
    }
    fn cumsum(&'a self) -> Vec<T>
    where
        T: Default + std::ops::AddAssign<&'a T> + Copy + 'static,
    {
        self.as_ref()
            .iter()
            .scan(T::default(), |acc, x| {
                *acc += x;
                Some(*acc)
            })
            .collect()
    }

    fn cum_fn<F, B>(&self, f: F, init: B) -> Vec<B>
    where
        F: Fn(&B, &T) -> B,
        B: Clone,
    {
        self.as_ref()
            .iter()
            .scan(init, |accu, x| {
                *accu = f(accu, x);
                Some(accu.clone())
            })
            .collect()
    }
    fn cum_max(&self) -> Vec<T>
    where
        T: PartialOrd + Clone,
    {
        self.cum_fn(|accu: &T, x: &T| -> T {
            if x.gt(accu) { x.clone()} else { accu.clone() }
        }, self.as_ref()[0].clone())
    }
    fn cum_min(&self) -> Vec<T>
    where
        T: PartialOrd + Clone,
    {
        self.cum_fn(|accu: &T, x: &T| -> T {
            if x.lt(accu) { x.clone() } else { accu.clone() }
        }, self.as_ref()[0].clone())
    }
    fn ema(&self, i: usize) -> v32
    where
        T: Copy + Into<f32>,
    {
        let data = self.as_ref();
        let mut res = vec![f32::NAN; data.len()];
        let mul = 2f32 / i as f32;
        res[0] = <T as Into<f32>>::into(data[0]);
        for i in 1..data.len() {
            res[i] = mul * <T as Into<f32>>::into(data[i]) + (1.0 - mul) * res[i - 1];
        }
        res
    }
    fn union_vecs<N>(&self) -> Vec<N>
    where
        T: AsRef<[N]>,
        N: Eq + std::hash::Hash + Clone + std::cmp::Ord,
    {
        let data_set =
            HashSet::<N>::from_iter(self.as_ref().iter().flat_map(|x| x.as_ref()).cloned());
        let mut data_vec: Vec<N> = data_set.into_iter().collect();
        data_vec.sort();
        data_vec
    }
    fn quantile(&self, n: f32) -> T
    where
        T: Clone,
        for<'g> &'g T: PartialOrd,
    {
        let mut b = self.as_ref().to_vec();
        b.sort_by(|a, b| a.partial_cmp(&b).unwrap());
        b[(self.as_ref().len() as f32 * n) as usize].clone()
    }
    ///sort self by the indices
    fn sort_perm(self, indices: &[usize]) -> Vec<T>
    where
        Self: Sized + IntoIterator<Item = T>,
    {
        self.into_iter()
            .zip(indices.iter())
            .sorted_by_key(|(_, &y)| y)
            .map(|(x, _)| x)
            .collect_vec()
    }
    fn sort_rtn_position(&self) -> Vec<usize>
    where
        T: std::cmp::PartialOrd,
    {
        self
            .as_ref()
            .iter()
            .enumerate()
            .sorted_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(x, _)| x)
            .collect_vec()
    }
    fn get_perm(&self) -> vuz
    where
        T: std::cmp::PartialOrd
    {
        self
            .sort_rtn_position()
            .into_iter()
            .enumerate()
            .sorted_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(x, _)| x)
            .collect_vec()
    }
    fn mmap<K, E, G>(&self, f: K) -> Vec<Vec<G>>
    where
        K: Fn(&E) -> G + Clone,
        T: AsRef<[E]>,
    {
        self.as_ref()
            .iter()
            .map(|x| {
                x.as_ref()
                    .iter()
                    .map(f.clone())
                    .collect_vec()
            })
            .collect_vec()
    }
    fn if_else<F, N, K, G>(&self, f: F, true_side: N, false_side: K) -> Vec<G>
    where
        F: Fn(&T) -> bool,
        N: BroadcastIndex<G>,
        K: BroadcastIndex<G>,
        G: Default + Clone,
    {
        let data = self.as_ref();
        let mut res = vec![G::default(); data.len()];
        for i in 0..res.len() {
            if f(&data[i]) {
                res[i] = true_side.bi(i).clone();
            } else {
                res[i] = false_side.bi(i).clone();
            }
        }
        res
    }

    fn filter_thre<F, N, F2>(self, f: F, fi: F2) -> Vec<T>
    where
        F: Fn(&T) -> N,
        F2: Fn(&N, &[N]) -> bool,
        Self: std::iter::IntoIterator<Item = T> + Sized,
    {
        let f_res = self.map(f);
        let res = izip!(self.into_iter(), f_res.iter())
            .filter_map(|(x, y)| {
                if (fi)(y, &f_res) {
                    Some(x)
                } else {
                    None
                }
            })
            .collect_vec();
        println!("origin: {} -> res: {}", f_res.len(), res.len());
        res
    }
    fn find_first_ele<F, N>(&self, f: F) -> Vec<T>
    where
        F: Fn(&T) -> N,
        N: Eq,
        T: Clone,
    {
        self
            .as_ref()
            .iter()
            .fold((vec![], f(&self.as_ref()[0])), |mut accu, x| {
                let n_now = f(x);
                if accu.0.is_empty() || n_now != accu.1 {
                    accu.1 = n_now;
                    accu.0.push(x.clone());
                }
                accu
            })
            .0
    }
    fn similar_init<N>(&self) -> Vec<Vec<N>>
    where
        T: AsRef<Vec<N>>,
        N: Clone,
    {
        let inner_size = self.as_ref()[0].as_ref().len();
        let outer_size = self.as_ref().len();
        init_a_matrix(inner_size, outer_size)
    }
    fn vcat_other<N>(&mut self, other: &mut Vec<Vec<N>>)
    where
        Self: AsMut<Vec<Vec<N>>>,
    {
        self
            .as_mut()
            .iter_mut()
            .zip(other.iter_mut())
            .for_each(|(x, y)| {
                x.append(y);
            });
    }
    fn extract_one<F>(&self, f: F) -> Option<T>
    where
        F: Fn(&T) -> bool,
        T: Clone,
    {
        let position = self.as_ref().iter().position(f)?;
        Some(self.as_ref()[position].clone())
    }

    fn check_all<F>(&self, f: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        for x in self.as_ref() {
            if !f(x) {
                return false;
            }
        }
        true
    }

    fn is_last_distinct(&self) -> Vec<bool>
    where
        T: Eq,
        for<'b> &'b T: Hash,
    {
        let mut res = Vec::with_capacity(self.as_ref().len());
        let mut unique_pool: HashSet<&T> = HashSet::new();
        for data in self.as_ref().iter().rev() {
            if unique_pool.contains(&data) {
                res.push(false);
            } else {
                res.push(true);
                unique_pool.insert(data);
            }
        }
        res.into_iter().rev().collect()
    }
}
impl<'a, T: 'a> Unique<'a, T> for [T] {}
impl<'a, T: 'a> Unique<'a, T> for Vec<T> {}

pub fn init_a_matrix<T>(inner_size: usize, outer_size: usize) -> Vec<Vec<T>> {
    std::iter
        ::repeat_with(|| Vec::with_capacity(inner_size))
        .take(outer_size)
        .collect_vec()
}
pub fn repeat_to_vec<T: FnMut() -> A, A>(f: T, size: usize) -> Vec<A> {
    std::iter::repeat_with(f).take(size).collect_vec()
}
/* #endregion */

/* #region row index */
pub trait RowIndex<T: Num> {
    type Slice<'a>
    where
        Self: 'a;
    fn slice_index(&self, start: usize, end: usize) -> Self::Slice<'_>;
}

impl<T: Num> RowIndex<T> for Vec<T> {
    type Slice<'a> = &'a [T] where Self: 'a;
    fn slice_index(&self, start: usize, end: usize) -> Self::Slice<'_> {
        &self[start..end]
    }
}

impl<'s, T: Num> RowIndex<T> for Vec<&'s Vec<T>> {
    type Slice<'a> = Vec<&'s [T]> where Self: 'a;
    fn slice_index(&self, start: usize, end: usize) -> Self::Slice<'_> {
        self.iter().map(|x| &x[start..end]).collect()
    }
}

impl<T: Num> RowIndex<T> for Vec<Vec<T>> {
    type Slice<'a> = Vec<&'a [T]> where T: 'a;
    fn slice_index(&self, start: usize, end: usize) -> Self::Slice<'_> {
        self.iter().map(|x| &x[start..end]).collect()
    }
}
/* #endregion */

/* #region Grp */
pub struct Grp<T>(pub T);

impl<T: PartialEq + Clone> Grp<Vec<T>> {
    pub fn apply<P: Fn(&[N]) -> K, K, N>(&self, data: &[N], func: P) -> (Vec<T>, Vec<K>) {
        let mut start_i = 0usize;
        let mut index = vec![];
        let mut res = vec![];
        for i in 1..self.0.len() + 1 {
            if i == self.0.len() || self.0[i] != self.0[i - 1] {
                index.push(self.0[i - 1].clone());
                let res_ = func(&data[start_i..i]);
                res.push(res_);
                start_i = i;
            }
        }
        (index, res)
    }

    pub fn transform<P: Fn(&[N]) -> K, K, N>(&self, data: &[N], func: P) -> Vec<Option<K>>
    where
        T: PartialOrd,
        K: Copy,
    {
        let grp_res = self.apply(data, func);
        let ri = Reindex::new(&grp_res.0[..], &self.0[..]);
        ri.reindex(&grp_res.1[..])
    }

    pub fn unique(&self) -> usize {
        let mut res = 0;
        for i in 0..self.0.len() {
            if i == 0 || self.0[i] != self.0[i - 1] {
                res += 1;
            }
        } 
        res
    }

    pub fn sum(&self, data: &[f32]) -> (Vec<T>, v32) {
        self.apply(data, <[f32] as AggFunc>::sum)
    }
    pub fn max(&self, data: &[f32]) -> (Vec<T>, v32) {
        self.apply(data, <[f32] as AggFunc>::max)
    }
}

impl<T: PartialOrd + Clone> Grp<(Vec<T>, Vec<vuz>)> {
    pub fn new_without_order(data: &[T]) -> Self {
        let mut k_vec = data.unique();
        k_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let v_vec = k_vec
            .iter()
            .map(|x| {
                data.iter()
                    .enumerate()
                    .filter_map(|(i, y)| if x == y { Some(i) } else { None })
                    .collect_vec()
            })
            .collect_vec();
        Grp((k_vec, v_vec))
    }

    pub fn apply_without_order<P: Fn(&[N]) -> K, K, N: Clone>(
        &self,
        data: &[N],
        func: P,
    ) -> (Vec<T>, Vec<K>) {
        izip!(self.0 .0.iter(), self.0 .1.iter()).fold((vec![], vec![]), |mut accu, (x, y)| {
            accu.0.push(x.clone());
            let b = y.iter().map(|i| data[*i].clone()).collect_vec();
            accu.1.push(func(&b));
            accu
        })
    }
}
/* #endregion */

/* #region reindex */
#[derive(Debug)]
pub struct Reindex {
    pub loc_i: Vec<Option<usize>>,
}

impl Reindex {
    pub fn new<T: PartialOrd>(ori: &[T], target: &[T]) -> Self {
        let mut res = Vec::with_capacity(target.len());
        let mut ori_index = 0usize;
        let mut ori_value = &ori[ori_index];
        let mut tar_index = 0usize;
        let mut tar_value = &target[tar_index];
        let mut posi;
        loop {
            if tar_value < ori_value {
                posi = None;
            } else if tar_value == ori_value {
                posi = Some(ori_index);
            } else {
                loop {
                    if ori_index >= ori.len() - 1 {
                        posi = None;
                        break;
                    }
                    ori_index += 1;
                    ori_value = &ori[ori_index];
                    if ori_value < tar_value {
                        continue;
                    } else if ori_value == tar_value {
                        posi = Some(ori_index);
                        break;
                    } else {
                        posi = None;
                        break;
                    }
                }
            }
            res.push(posi);
            tar_index += 1;
            if tar_index >= target.len() {
                break;
            }
            tar_value = &target[tar_index];
        }
        Reindex { loc_i: res }
    }
    pub fn reindex<T: Clone>(&self, data: &[T]) -> Vec<Option<T>> {
        let mut res = Vec::with_capacity(self.loc_i.len());
        for loc_i in self.loc_i.iter() {
            let res_ = loc_i.as_ref().map(|i| data[*i].clone());
            res.push(res_);
        }
        res
    }
}
/* #endregion */

/* #region fillna */
pub trait Fillna<T: Clone> {
    fn fillna(&self, data: T) -> Vec<T>;
    fn ffill(&self, data: T) -> Vec<T>;
}

impl<T: Clone> Fillna<T> for [Option<T>] {
    fn fillna(&self, data: T) -> Vec<T> {
        self.iter()
            .map(|x| match x {
                Some(i) => i.clone(),
                None => data.clone(),
            })
            .collect()
    }
    fn ffill(&self, data: T) -> Vec<T> {
        let mut res = Vec::with_capacity(self.len());
        let mut last_data = data;
        for data in self.iter() {
            if let Some(i) = data {
                last_data = i.clone();
            }
            res.push(last_data.clone());
        }
        res
    }
}

pub trait FillnaMut<T: Copy> {
    fn fillna(&mut self, data: T);
    fn ffill(&mut self);
}

impl FillnaMut<f32> for v32 {
    fn fillna(&mut self, data: f32) {
        for data_ in self.iter_mut() {
            if data_.is_nan() {
                *data_ = data
            }
        }
    }
    fn ffill(&mut self) {
        if self.is_empty() {
            return;
        };
        let mut last_value = self[0];
        for data_ in self.iter_mut().skip(1) {
            if data_.is_nan() {
                *data_ = last_value;
            } else {
                last_value = *data_;
            }
        }
    }
}
/* #endregion */

/* #region Lag */
pub trait LagFor {
    type R;
    fn lag_for(&self) -> Vec<Self::R>;
}

impl<'a, T> LagFor for (&'a [T], usize)
where
    T: Clone + Default,
{
    type R = T;
    fn lag_for(&self) -> Vec<Self::R> {
        (self.0, (self.1, T::default())).lag_for()
    }
}

impl<'a, T> LagFor for (&'a [T], f32)
where
    T: Clone,
{
    type R = T;
    fn lag_for(&self) -> Vec<Self::R> {
        (self.0, (self.1 as usize, self.0[0].clone())).lag_for()
    }
}

impl<'a, T> LagFor for (&'a [T], (usize, T))
where
    T: Clone,
{
    type R = T;
    fn lag_for(&self) -> Vec<Self::R> {
        let mut res = self.0.to_owned();
        res.rotate_right(self.1 .0.min(self.0.len()));
        res
            .iter_mut()
            .take(self.1 .0.min(self.0.len()))
            .for_each(|x| *x = self.1 .1.clone());
        res
    }
}

pub trait Lag {
    type R;
    fn lag<'a, T>(&'a self, l: T) -> Vec<Self::R>
    where
        (&'a Self, T): LagFor<R = Self::R>,
    {
        (self, l).lag_for()
    }
}

impl<T> Lag for [T]
where
    T: Clone + Default,
{
    type R = T;
}
/* #endregion */

/* #region agg axis = 1 */
pub trait Agg2D<T> {
    fn sum2d(&self) -> Vec<T>;
    fn mean2d(&self) -> Vec<T>;
    fn max2d(&self) -> Vec<T>;
    fn min2d(&self) -> Vec<T>;
    fn nansum2d(&self) -> Vec<T>
    where
        T: num_traits::Float;
    fn nanmean2d(&self) -> Vec<T>
    where
        T: num_traits::Float;
}

impl<T, N> Agg2D<T> for Vec<N>
where
    T: Copy + From<u8> + std::ops::AddAssign<T> + std::ops::Div<T, Output = T> + PartialOrd,
    N: AsRef<[T]>,
    Vec<T>: FromIterator<<T as std::ops::Div>::Output>,
{
    fn sum2d(&self) -> Vec<T> {
        let col_len = self[0].as_ref().len();
        self.iter()
            .fold(vec![T::from(0u8); col_len], |mut res, row| {
                row.as_ref()
                    .iter()
                    .enumerate()
                    .for_each(|(i, cell)| res[i] += *cell);
                res
            })
    }
    fn mean2d(&self) -> Vec<T> {
        let sum_data = self.sum2d();
        let col_len = T::from(self.len() as u8);
        sum_data.iter().map(|x| *x / col_len).collect()
    }
    fn max2d(&self) -> Vec<T> {
        self.iter().fold(self[0].as_ref().to_vec(), |mut res, row| {
            row.as_ref().iter().enumerate().for_each(|(i, cell)| {
                if res[i] < *cell {
                    res[i] = *cell
                }
            });
            res
        })
    }
    fn min2d(&self) -> Vec<T> {
        self.iter().fold(self[0].as_ref().to_vec(), |mut res, row| {
            row.as_ref().iter().enumerate().for_each(|(i, cell)| {
                if res[i] > *cell {
                    res[i] = *cell
                }
            });
            res
        })
    }
    fn nansum2d(&self) -> Vec<T>
    where
        T: num_traits::Float,
    {
        let col_len = self[0].as_ref().len();
        self.iter()
            .fold(vec![<T as From<u8>>::from(0u8); col_len], |mut res, row| {
                row.as_ref().iter().enumerate().for_each(|(i, cell)| {
                    if !cell.is_nan() {
                        res[i] += *cell
                    };
                });
                res
            })
    }
    fn nanmean2d(&self) -> Vec<T>
    where
        T: num_traits::Float + Div<T, Output = T>,
    {
        let col_len = self[0].as_ref().len();
        let zz_ = <T as From<u8>>::from(1u8);
        let nan_count =
            self.iter()
                .fold(vec![<T as From<u8>>::from(0u8); col_len], |mut res, row| {
                    row.as_ref().iter().enumerate().for_each(|(i, cell)| {
                        if !cell.is_nan() {
                            res[i] += zz_;
                        }
                    });
                    res
                });
        let nan_sum_value = self.nansum2d();
        nan_sum_value
            .iter()
            .zip(nan_count.iter())
            .map(|(x, y)| *x / *y)
            .collect()
    }
}
/* #endregion */

/* #region ---------- Vector Function  ------------ */
use std::ops::{Add, Div, Mul, Sub};

pub struct S<'a, T>(pub &'a T);

/* #region v32 + f32 */
macro_rules! V32_f32 {
    ($ops1: ident, $ops2: ident, $x: ty, $y: ty) => {
        impl $ops1<$x> for S<'_, $y> {
            type Output = v32;
            fn $ops2(self, rhs: $x) -> Self::Output {
                self.0.iter().map(|x| x.$ops2(rhs)).collect()
            }
        }
    };
}
/* #endregion */

/* #region v32 + v32 */
macro_rules! V32_V32 {
    ($ops1: ident, $ops2: ident, $x: ty, $y: ty) => {
        impl $ops1<$x> for S<'_, $y> {
            type Output = v32;
            fn $ops2(self, rhs: $x) -> Self::Output {
                self.0
                    .iter()
                    .zip(rhs.iter())
                    .map(|(x, y)| x.$ops2(y))
                    .collect()
            }
        }
    };
}
/* #endregion */

/* #region vv32 + f32 */
macro_rules! VV32_f32 {
    ($ops1: ident, $ops2: ident, $x: ty, $y: ty) => {
        impl $ops1<$x> for S<'_, $y> {
            type Output = Vec<v32>;
            fn $ops2(self, rhs: $x) -> Self::Output {
                self.0.iter().map(|x| S(x).$ops2(rhs)).collect()
            }
        }
    };
}
/* #endregion */

/* #region vv32 + vv32 */
macro_rules! VV32_VV32 {
    ($ops1: ident, $ops2: ident, $x: ty, $y: ty) => {
        impl $ops1<$x> for S<'_, $y> {
            type Output = Vec<v32>;
            fn $ops2(self, rhs: $x) -> Self::Output {
                self.0
                    .iter()
                    .zip(rhs.iter())
                    .map(|(x, y)| S(x).$ops2(y))
                    .collect()
            }
        }
    };
}
/* #endregion */

/* #region Ops */
macro_rules! vec_ops {
    ($ops1: ident, $ops2: ident) => {
        V32_f32!($ops1, $ops2, f32, v32);
        V32_f32!($ops1, $ops2, f32, &v32);
        V32_f32!($ops1, $ops2, f32, &[f32]);

        V32_V32!($ops1, $ops2, &'_ v32, v32);
        V32_V32!($ops1, $ops2, &'_ [f32], v32);
        V32_V32!($ops1, $ops2, &'_ v32, &[f32]);
        V32_V32!($ops1, $ops2, &'_ [f32], &[f32]);
        V32_V32!($ops1, $ops2, &'_ v32, &'_ v32);
        V32_V32!($ops1, $ops2, &'_ [f32], &'_ v32);

        VV32_f32!($ops1, $ops2, f32, Vec<v32>);
        VV32_f32!($ops1, $ops2, f32, Vec<&'_ v32>);
        VV32_f32!($ops1, $ops2, f32, Vec<&'_ [f32]>);

        VV32_f32!($ops1, $ops2, &'_ v32, Vec<v32>);
        VV32_f32!($ops1, $ops2, &'_ v32, Vec<&'_ v32>);
        VV32_f32!($ops1, $ops2, &'_ v32, Vec<&'_ [f32]>);

        VV32_f32!($ops1, $ops2, &'_ [f32], Vec<v32>);
        VV32_f32!($ops1, $ops2, &'_ [f32], Vec<&'_ v32>);
        VV32_f32!($ops1, $ops2, &'_ [f32], Vec<&'_ [f32]>);

        VV32_VV32!($ops1, $ops2, &'_ Vec<v32>, Vec<v32>);
        VV32_VV32!($ops1, $ops2, &'_ Vec<v32>, Vec<&'_ v32>);
        VV32_VV32!($ops1, $ops2, &'_ Vec<v32>, Vec<&'_ [f32]>);
    };
}

vec_ops!(Add, add);
vec_ops!(Sub, sub);
vec_ops!(Mul, mul);
vec_ops!(Div, div);

/* #endregion */

/* #endregion */

pub trait SetIndex {
    fn set_index<T: Copy>(&self, data: &mut [T], value: &[T]);
}

impl SetIndex for vuz {
    fn set_index<T: Copy>(&self, data: &mut [T], value: &[T]) {
        for (&i, &v) in self.iter().zip(value.iter()) {
            data[i] = v;
        }
    }
}

impl SetIndex for Vec<bool> {
    fn set_index<T: Copy>(&self, data: &mut [T], value: &[T]) {
        let mut v_iter = value.iter();
        for i in 0..self.len() {
            if self[i] {
                data[i] = *v_iter.next().unwrap();
            }
        }
    }
}

pub trait Product {
    type T;
    fn product<K: Fn(Self::T) -> N, N>(self, x: K) -> Vec<N>;
}

impl<A1: Clone, A2: Clone> Product for (Vec<A1>, Vec<A2>) {
    type T = (A1, A2);
    fn product<K: Fn(Self::T) -> N, N>(self, x: K) -> Vec<N> {
        self.0
            .into_iter()
            .flat_map(|a1| {
                self.1
                    .clone()
                    .into_iter()
                    .map(|a2| x((a1.clone(), a2)))
                    .collect_vec()
            })
            .collect_vec()
    }
}


pub trait IndexProduct {
    fn index_product(&self, data: usize) -> Vec<Vec<usize>>;
}

impl IndexProduct for Vec<Vec<usize>> {
    fn index_product(&self, data: usize) -> Vec<Vec<usize>> {
        let mut res = vec![];
        for x in 0..data {
            for y in self.iter() {
                if &x <= y.last().unwrap() { 
                    continue
                }
                let mut z = y.clone();
                z.push(x);
                res.push(z);
            }
        }
        res
    }
}

impl IndexProduct for Vec<usize> {
    fn index_product(&self, data: usize) -> Vec<Vec<usize>> {
        self
            .iter()
            .map(|x| vec![*x])
            .collect_vec()
            .index_product(data)
    }
}

impl IndexProduct for usize {
    fn index_product(&self, data: usize) -> Vec<Vec<usize>> {
        (0..*self).collect_vec().index_product(data)
    }
}

pub trait InnerProduct {
    type Output;
    fn inner_product(&self, data: usize) -> Vec<Vec<Self::Output>>;
    fn inner_product_recur(&self, data: usize) -> Vec<Vec<Self::Output>> {
        (1..data + 1)
            .fold(vec![], |mut accu, x| {
                let mut c = self.inner_product(x);
                accu.append(&mut c);
                accu
            })

    }
}

impl<T: Clone> InnerProduct for [T] {
    type Output = T;
    fn inner_product(&self, data: usize) -> Vec<Vec<Self::Output>> {
        (1..data)
            .fold(
                (0..self.len()).map(|x| vec![x]).collect_vec(),
                |mut accu, _| {
                    accu = accu.index_product(self.len());
                    accu
                }
            )
            .into_iter()
            .map(|x| self.get_list_index(&x))
            .collect_vec()
    }
}

pub trait InnerProduct2 {
    type Output;
    fn inner_product2(self) -> Option<Self::Output>;
}

impl InnerProduct2 for vv32 {
    type Output = vv32;
    fn inner_product2(self) -> Option<Self::Output> {
        let mut g = self.into_iter().rev().collect_vec();
        let g_start = g.pop()?;
        let mut g_start = g_start.into_iter().map(|x| vec![x]).collect_vec();
        while let Some(g_next) = g.pop() {
            g_start = g_start
                .into_tuple(g_next)
                .product(|(mut x, y)| {
                    x.push(y);
                    x
                });
        }
        Some(g_start)
    }
}



