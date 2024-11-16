use qust::prelude::*;
use qust_derive::*;

#[ta_derive2]
pub struct cond1(pub Pms);


#[typetag::serde(name = "cond1_ops")]
impl CondType6 for cond1 {
    fn cond_type6(&self,_di: &Di) -> RetFnCondType6 {
        let pms = &self.0;
        Box::new(move |di_kline_o| {
            let data = di_kline_o.di_kline.di.calc(pms);
            // loge!(aler, "cond1 {} > 0 {:?}", data[0][di_kline_o.di_kline.i], di_kline_o.di_kline);
            data[0][di_kline_o.di_kline.i] > 0.
        })
    }
}

#[ta_derive2]
pub struct cond2(pub Pms);

#[typetag::serde(name = "cond2_ops")]
impl CondType6 for cond2 {
    fn cond_type6(&self,_di: &Di) -> RetFnCondType6 {
        let pms = &self.0;
        Box::new(move |di_kline_o| {
            let data = di_kline_o.di_kline.di.calc(pms);
            // loge!(aler, "cond2 {} < 0 {:?}", data[0][di_kline_o.di_kline.i], di_kline_o.di_kline);
            data[0][di_kline_o.di_kline.i] < 0.
        })
    }
}


#[ta_derive2]
pub struct cond3(pub Pms);

#[typetag::serde(name = "cond3_ops")]
impl CondType6 for cond3 {
    fn cond_type6(&self, _di: &Di) -> RetFnCondType6 {
        Box::new(move |di_kline_o| {
            let data1 = di_kline_o.di_kline.di.calc(&self.0);
            let data2 = di_kline_o.di_kline.di.h();
            let e = di_kline_o.di_kline.i;
            // loge!(aler, "cond3 {} > {}", data2[e], data1[0][e]);
            data2[e] > data1[0][e]
        })
    }
}

#[ta_derive2]
pub struct cond4(pub Pms);

#[typetag::serde(name = "cond4_ops")]
impl CondType6 for cond4 {
    fn cond_type6(&self, _di: &Di) -> RetFnCondType6 {
        Box::new(move |di_kline_o| {
            let data1 = di_kline_o.di_kline.di.calc(&self.0);
            let data2 = di_kline_o.di_kline.di.l();
            let e = di_kline_o.di_kline.i;
            // loge!(aler, "cond4 {} < {}", data2[e], data1[0][e]);
            data2[e] < data1[0][e]
        })
    }
}

#[ta_derive]
pub struct  FilterDay2;

#[typetag::serde(name = "Filterday_Ops")]
impl CondType6 for FilterDay2 {
    fn cond_type6(&self, _di: &Di) -> RetFnCondType6 {
        let t = tt::from_hms_opt(14, 56, 50).unwrap();
        Box::new(move |di_kline_o| {
            di_kline_o.di_kline.di.t()[di_kline_o.di_kline.i].time() > t
        })
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let pms1 = ori + ha + oos + super::c25::ta(10, 0.005);
        let pms2_l = ori + oos + maxta(KlineType::Close, 40);
        let pms2_s = ori + oos + minta(KlineType::Close, 40);
        let cond1_ = cond1(pms1.clone());
        let cond2_ = cond2(pms1);
        let cond3_ = cond3(pms2_l);
        let cond4_ = cond4(pms2_s);
        let cond_lo_open = cond1_.condtype6_box() & cond3_.condtype6_box();
        let cond_sh_close = cond2_.condtype6_box() | cond4_.condtype6_box() | FilterDay2.condtype6_box();
        let cond_sh_open = cond2_.condtype6_box() & cond4_.condtype6_box();
        let cond_lo_close =  cond1_.condtype6_box() | cond3_.condtype6_box() | FilterDay2.condtype6_box();
        let open_exit_lo = OpenExit {
            cond_open: cond_lo_open,
            cond_exit: cond_sh_close,
        };
        let open_exit_sh = OpenExit {
            cond_open: cond_sh_open,
            cond_exit: cond_lo_close,
        };
        let two_open_exit = TwoOpenExit { posi: Box::new(Posi1(1.)), open_exit_lo, open_exit_sh };
        Ptm::Ptm7(KtnVar::Two(two_open_exit))
    };
}