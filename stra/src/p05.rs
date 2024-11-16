use std::sync::Arc;

use qust::prelude::*;
use qust_ds::prelude::*;
use qust_derive::*;


#[ta_derive2]
pub struct tat(pub usize);

#[typetag::serde]
impl Ta for tat {

    fn calc_di(&self,di: &Di) -> avv32 {
        vec![
            di.c(),
            di.t().iter().map(|x| x.and_utc().timestamp() as f32).collect_vec().pip(Arc::new)
        ]
        
    }
    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        let mut c_vec = vec![f32::NAN; self.0 - 1];
        let mut d_vec = vec![f32::NAN; self.0 - 1];
        let mut t_vec = vec![f32::NAN; self.0 - 1];
        da[0]
            .windows(self.0)
            .zip(da[1].windows(self.0))
            .for_each(|(x, y)| {
                let mut c = x[0];
                let mut t = y[0];
                let mut d = 1;
                for (i, (&a, &b)) in x.iter().zip(y.iter()).enumerate().skip(1) {
                    if a <= c {
                        c = a;
                        t = b;
                        d = self.0 - i;
                    }
                }
                c_vec.push(c);
                d_vec.push(d as f32);
                t_vec.push(t);
            });
        vec![c_vec, d_vec, t_vec]
        
    }
}


#[ta_derive2]
pub struct condt;


impl CondType1 for condt {
    fn cond_type1(&self, _di: &Di) -> RetFnCondType1 {
        let mut open_time = dt::default();
        let mut last_order_target = OrderTarget::No;
        // let mut last_comp = 0.;
        let mut signal_counts = 0;
        Box::new(move |stream| {
            if stream.stream_api.hold.sum() == 0. {
                let data = stream.di_kline_state.di_kline.di.calc(tat(20));
                let i = stream.di_kline_state.di_kline.i;
                if stream.stream_api.tick_data.c <=  data[0][i] && data[1][i] >= 5. && signal_counts <= 20 {
                    if let OrderTarget::No = last_order_target {
                        signal_counts = 0;
                    }
                    signal_counts += 1;
                    // last_comp = data[2][i];
                    open_time = stream.stream_api.tick_data.t;
                    last_order_target = OrderTarget::Sh(1.);
                } 
            } else if (stream.stream_api.tick_data.t - open_time).num_seconds() >= 60 {
                last_order_target = OrderTarget::No;
                signal_counts = 0;
            }
            last_order_target.clone()
        })
    }
}




