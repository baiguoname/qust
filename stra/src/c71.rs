use itertools::izip;
use qust::prelude::*;
use qust_derive::*;

#[ta_derive2]
pub struct ta(pub f32);

#[typetag::serde(name = "c71_ta")]
impl Ta for ta {
    fn calc_di(&self, di: &Di) -> avv32 {
        vec![
            (ono, ShiftDays(0, KlineType::Open, IndexSpec::First)).calc(di)[0].clone(),
            (ono, ShiftDays(1, KlineType::High, IndexSpec::Max)).calc(di)[0].clone(),
            (ono, ShiftDays(1, KlineType::Low, IndexSpec::Min)).calc(di)[0].clone(),
            di.c(),
        ]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let h_l_pre = izip!(da[1].iter(), da[2].iter())
            .map(|(&x, &y)| self.0 * (x - y))
            .collect::<v32>();
        let dnband = izip!(da[0].iter(), h_l_pre.iter())
            .map(|(&x, y)| x - y)
            .collect();
        let upband = izip!(da[0].iter(), h_l_pre.iter())
            .map(|(&x, y)| x + y)
            .collect();
        vec![dnband, da[3].to_vec(), upband]
    }
}

lazy_static! {
    pub static ref PMS1: Pms = ori + oos + ta(0.2);
    pub static ref PMS2: Pms = ori + oos + EffRatio(2, 10);
    pub static ref COND_L1: BandCond<Pms> = BandCond(Lo, BandState::Action, PMS1.clone());
    pub static ref COND_S1: BandCond<Pms> = BandCond(Sh, BandState::Action, PMS1.clone());
    pub static ref COND_L2: BandCond<Pms> = BandCond(Lo, BandState::Lieing, PMS2.clone());
    pub static ref COND_S2: BandCond<Pms> = BandCond(Sh, BandState::Lieing, PMS2.clone());
    pub static ref ptm: Ptm = {
        let msig_oc_l = qust::msig!(and, COND_L1.clone(), COND_L2.clone());
        let msig_ec_s = qust::msig!(or, COND_S1.clone(), Filterday);
        let msig_oc_s = qust::msig!(and, COND_S1.clone(), COND_S2.clone());
        let msig_ec_l = qust::msig!(or, COND_L1.clone(), Filterday);

        let tsig_l = Tsig::new(Lo, &msig_oc_l, &msig_ec_s);
        let tsig_s = Tsig::new(Sh, &msig_oc_s, &msig_ec_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };

    pub static ref ptm2: () = {
        let pms1 = ori + VolFilter(100, 50) + oos + ta(0.2) + vori;
        let pms2 = ori + VolFilter(100, 50) + oos + EffRatio(2, 10) + vori;
        let cond_l1 = BandCond(Lo, BandState::Action, pms1.clone());
        let cond_s1 = BandCond(Sh, BandState::Action, pms1);
        let cond_l2 = BandCond(Lo, BandState::Lieing, pms2.clone());
        let cond_s2 = BandCond(Sh, BandState::Lieing, pms2); 
        let _msig_oc_l = cond_l1.to_box() & cond_l2.to_box();
        let _msig_ec_s = cond_s1.to_box() | Filterday.to_box();
        let _msig_oc_s = cond_s1.to_box() & cond_s2.to_box();
        let _msig_ec_l = cond_l1.to_box() | Filterday.to_box();
    };
}



#[ta_derive2]
pub struct ta2(pub ta);

#[typetag::serde(name = "c71_ta2")]
impl Ta for ta2 {
    fn calc_di(&self, di: &Di) -> avv32 {
        di.calc(&self.0)
    }

    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let mut l_counts = vec![0f32];
        let mut s_counts = vec![0f32];
        let mut l = (false, false);
        let mut s = (false, false);
        let mut l_last = 0f32;
        let mut s_last = 0f32;
        for i in 1..da[0].len() {
            if da[1][i] >= da[2][i] && da[1][i - 1] < da[2][i - 1] {
                l.0 = true;
                if l.1 {
                    l_last += 1f32;
                    l.1 = false;
                }
                if s.0 {
                    s.1 = true;
                }
            } else if da[1][i] <= da[0][i] && da[1][i - 1] > da[0][i - 1] {
                s.0 = true;
                if s.1 {
                    s_last += 1f32;
                    s.1 = false;
                }
                if l.0 {
                    l.1 = true;
                }
            }
            s_counts.push(s_last);
            l_counts.push(l_last);
        }
        vec![s_counts, l_counts]
    }
}

#[ta_derive2]
pub struct cond(pub Pms, pub usize);

#[typetag::serde(name = "c71_cond")]
impl Cond for cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.0);
        let i = self.1;
        Box::new(
            move |e: usize, _o: usize| { {
                e > 1 && data[i][e - 1] == 0f32 && data[i][e] == 1f32
            }}
        )
    }
}


lazy_static! {
    pub static ref pms3: Pms = ori + oos + ta2(ta(0.075));
    pub static ref cond_l: cond = cond(pms3.clone(), 1);
    pub static ref cond_s: cond = cond(pms3.clone(), 0);
    pub static ref ptm3: Ptm = {
        let msig_oc_l = qust::msig!(and, cond_l.clone(), COND_L2.clone());
        let msig_ec_s = qust::msig!(or, COND_S1.clone(), Filterday);
        let msig_oc_s = qust::msig!(and, cond_s.clone(), COND_S2.clone());
        let msig_ec_l = qust::msig!(or, COND_L1.clone(), Filterday);

        let tsig_l = Tsig::new(Lo, &msig_oc_l, &msig_ec_s);
        let tsig_s = Tsig::new(Sh, &msig_oc_s, &msig_ec_l);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
    pub static ref stral: Stral = {
        use qust::prelude::rl5m;
        vec![cuer, SAer, per, fuer]
            .into_iter()
            .map(|x| Stra::new_with_name(PconIdent::new(rl5m.tri_box(), x), "c71_ta2_t", ptm3.clone()))
            .collect_vec()
            .to_stral_bare()
    };
}

#[ta_derive2]
pub struct ta3(pub f32, pub PriBox);

#[typetag::serde(name = "c71_ta3")]
impl Ta for ta3 {
    fn calc_di(&self, di: &Di) -> avv32 {
        let dcon_new = di.last_dcon() + Event(self.1.clone());
        let price_new = di.calc(dcon_new.clone());
        let mut open_price_pres = Vec::with_capacity(di.c().len());
        let mut open_time_iter = price_new.ki.iter();
        let mut open_time_last = open_time_iter.next().unwrap();
        let mut open_price_last = f32::NAN;
        let price_ori = di.calc(di.last_dcon());
        for (o, ki_ori) in izip!(price_ori.o.iter(), price_ori.ki.iter()) {
            if ki_ori.open_time == open_time_last.open_time {
                open_price_last = *o;
                if let Some(k) = open_time_iter.next() {
                    open_time_last = k;
                }
            }
            open_price_pres.push(open_price_last);
        }
        vec![
            open_price_pres.to_arc(),
            di.calc(dcon_new.clone() + ono + KlineType::High + Lag1 + vori)[0].clone(),
            di.calc(dcon_new.clone() + ono + KlineType::Low + Lag1 + vori)[0].clone(),
            di.c()
        ]
    }
    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let h_l_pre = izip!(da[1].iter(), da[2].iter())
            .map(|(&x, &y)| self.0 * (x - y)).collect::<v32>();
        let dnband = izip!(da[0].iter(), h_l_pre.iter())
            .map(|(&x, y)| x - y).collect();
        let upband = izip!(da[0].iter(), h_l_pre.iter())
            .map(|(&x, y)| x + y).collect();
        vec![
            // da[0].to_vec(),
            // da[1].to_vec(),
            // da[2].to_vec(),
            dnband, 
            da[3].to_vec(), 
            upband
        ]
    }
}

#[ta_derive2]
struct Lag1;

#[typetag::serde]
impl ForeTaCalc for Lag1 {
    fn fore_ta_calc(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        da
            .into_iter()
            .map(|x| x.lag(1))
            .collect_vec()
    }
}