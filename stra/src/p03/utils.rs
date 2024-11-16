use chrono::Timelike;
use qust_ds::prelude::*;




pub trait ModifyMillis {
    type Output;
    fn modify(&self) -> Self::Output;
}
impl ModifyMillis for [dt] {
    type Output = Vec<dt>;
    fn modify(&self) -> Self::Output {
        self.iter()
            .map(|x| {
                x.modify()
            })
            .collect_vec()
    }
}

impl ModifyMillis for dt {
    type Output = dt;
    fn modify(&self) -> Self::Output {
        let millis = self.and_utc().timestamp_subsec_millis();
        let millis_new = if let 0..500 = millis {
            0
        } else {
            500
        };
        self.with_nanosecond(millis_new * 1_000_000).unwrap()
    }
}


pub trait WindowsFlatten<T> {
    type Output;
    fn windows_flatten(&self, n: usize) -> Self::Output;
}

impl<T> WindowsFlatten<T> for [T] 
where
    T: Default + Clone,
{
    type Output = Vec<Vec<T>>;
    fn windows_flatten(&self, n: usize) -> Self::Output {
        let mut res = self.windows(n).map(|x| x.to_vec()).collect_vec();
        for _ in 0..(n - 1) {
            res.insert(0, vec![Default::default(); res[0].len()]);
        }
        res
    }
}

pub(super) fn vcat(data: vv32) -> vv32 {
    (0..data[0].len())
        .map(|i| {
            data.iter().map(|data_| data_[i]).collect_vec()
        })
        .collect_vec()
}