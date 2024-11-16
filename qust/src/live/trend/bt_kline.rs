use std::thread;
use crate::prelude::*;
use super::super::bt::*;


impl<'a> BtKline<(&'a Di, CommSlip)> for Ptm {
    type Output = PnlRes<dt>;
    fn bt_kline(&self, input: (&Di, CommSlip)) -> Self::Output {
        input.0.pnl(self, input.1)
    }
}

impl<'a> BtKline<(&'a Di, CommSlip)> for Vec<Ptm> {
    type Output = Vec<PnlRes<dt>>;
    fn bt_kline(&self, input: (&'a Di, CommSlip)) -> Self::Output {
        thread::scope(|scope| {
            let mut handles = vec![];
            for ptm in self.iter() {
                let di = input.0;
                let comm_slip = input.1.clone();
                let handle = scope.spawn(move || {
                    ptm.bt_kline((di, comm_slip))
                });
                handles.push(handle);
            }
            handles
                .into_iter()
                .map(|x| x.join().unwrap())
                .collect()
        })
    }
}

impl<'a, T, N> BtKline<(Vec<&'a Di>, CommSlip)> for T 
where
    T: BtKline<(&'a Di, CommSlip), Output = N> + Clone + Send + Sync,
    N: Send + Sync,
{
    type Output = Vec<N>;
    fn bt_kline(&self, input: (Vec<&'a Di>, CommSlip)) -> Self::Output {
        thread::scope(|scope| {
            let mut handles = vec![];
            for di in input.0.into_iter() {
                let ptm = self.clone();
                let comm_slip = input.1.clone();
                let handle = scope.spawn(move || {
                    ptm.bt_kline((di, comm_slip))
                });
                handles.push(handle);
            }
            handles
                .into_iter()
                .map(|x| x.join().unwrap())
                .collect()
        })
    }
}