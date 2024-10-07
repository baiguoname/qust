use qust::prelude::Itertools;
use ndarray::{Array, Array1, Array2, Axis};
use num_traits::{Num, Float, FromPrimitive};
use ndarray_stats::CorrelationExt;

pub trait ToArray<R> {
    fn to_array(&self) -> R;
}


impl<T: Num + Clone> ToArray<Array1<T>> for [T] {
    fn to_array(&self) -> Array1<T> {
        Array::from_vec(self.to_vec())
    }
}

impl<T, R> ToArray<Array2<R>> for [T]
where
    T: AsRef<[R]>,
    R: Num + Default + Copy,
{
    fn to_array(&self) -> Array2<R> {
        let mut res = Array2::<R>::default((self.len(), self[0].as_ref().len()));
        for (i, mut row) in res.axis_iter_mut(Axis(0)).enumerate() {
            let b = self[i].as_ref();
            for (j, col) in row.iter_mut().enumerate() {
                *col = b[j];
            }
        }
        res
    }
}

pub trait Corr<T> {
    type OutputEle;
    fn e(&self) -> Vec<Self::OutputEle>;
    fn corr(&self) -> Array2<Self::OutputEle>;
    fn cov(&self) -> Array2<Self::OutputEle>;
    fn delta(&self) -> Vec<Vec<Self::OutputEle>>;
}

impl<T: FromPrimitive + Num + Default + Copy + Float + From<i8> + 'static> Corr<u16> for [&Vec<T>]
{
    type OutputEle = T;
    fn e(&self) -> Vec<Self::OutputEle> {
        self.as_ref()
            .iter()
            .map(|x| x.to_array().mean().unwrap())
            .collect_vec()
    }

    fn corr(&self) -> Array2<Self::OutputEle> {
        self.as_ref()
            .to_array()
            .pearson_correlation()
            .unwrap()
    }

    fn cov(&self) -> Array2<Self::OutputEle> {
        self.as_ref()
            .to_array()
            .cov(<T as From<i8>>::from(1i8))
            .unwrap()
    }

    fn delta(&self) -> Vec<Vec<Self::OutputEle>> {
        let k = self.cov();
        k.dot(&k)
            .axis_iter(Axis(0))
            .map(|x| x.to_vec())
            .collect_vec()
    }
}