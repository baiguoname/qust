use itertools::{izip, Itertools};
use crate::{
    ds::{aa::*, types::*, roll::*},
    sig::{livesig::*, posi::{M1, Dire::*}},
    stats::pnl::{Stra, DiStra, CalcStra, DtToDa, Stral},
    trade::di::Di,
    idct::pms::const_pms::*,
};
pub trait ExtractCond {
    type Output;
    fn extract_cond(&self) -> Self::Output;
}

pub struct StraCond(pub Stra, pub CondWeight, pub CondWeight);

impl StraCond {
    pub fn get_cond_m(&self, di: &mut Di) -> vv32 {
        let grp = Grp(di.pcon.price.t.iter().map(|x| x.date()).collect_vec());
        izip!(self.1.0.iter(), self.2.0.iter())
            .fold(vec![], |mut acc, (cond1, cond2)| {
                let w = cond1.1;
                let cond_x = &cond1.0;
                let cond_y = &cond2.0;
                cond_x.init(di);
                cond_y.init(di);
                let f_x = cond_x.cond(di);
                let f_y = cond_y.cond(di);
                let mut res = vec![0f32; di.len()];
                for i in 0..di.len() {
                    res[i] = if f_x(i, i) || f_y(i, i) { w } else { 0f32 };
                };
                acc.push(grp.apply(&res, |x| *x.last().unwrap()).1);
                acc
            })
    }

    pub fn get_stra_m(&self, di: &mut Di) -> (vda, v32, vv32) {
        let pnl = di.pnl(&self.0.1, &cs1).da();
        let m_vec = pnl.1[2].clone();
        let cond_m = self.get_cond_m(di);
        (pnl.0.clone(), m_vec, cond_m)
    }
}

pub struct EstMoney(pub usize);
impl EstMoney {
    fn est_money(&self, m: &v32, cond: &vv32) -> v32 {
        let n = self.0;
        let mut w_v = vec![0f32; m.len()];
        for i in 1..m.len() + 1 {
            let start = if i <= n { 0 } else { i - n };
            let m_ori = &m[start..i];
            let mut res_now = vec![0f32; n];
            for j in 0..cond.len() {
                let w_now = cond[j][i-1];
                if w_now != 0f32 {
                    izip!(res_now.iter_mut(), cond[j][start..i].iter(), m_ori.iter())
                        .for_each(|(x, y, z)| *x += y * z * w_now);
                }
            }
            w_v[i-1] = res_now.max();
        }
        w_v.lag(1)
    }
}
impl CalcStra for EstMoney {
    type Output = (vda, v32);
    fn calc_stra(&self, distra: &mut DiStra) -> Self::Output {
        let stra_cond = distra.1.extract_cond();
        let kk = stra_cond.get_stra_m(&mut distra.0);
        let m = self.est_money(&kk.1, &kk.2);
        (kk.0, m)
    }
}

impl ExtractCond for Stra {
    type Output = StraCond;
    fn extract_cond(&self) -> Self::Output {
        let (ptm, cond_weight1, cond_weight2) = self.1.extract_cond();
        StraCond(Stra(self.0.clone(), ptm), cond_weight1, cond_weight2)
    }
}
impl ExtractCond for Ptm {
    type Output = (Ptm, CondWeight, CondWeight);
    fn extract_cond(&self) -> Self::Output {
        match self {
            Ptm::Ptm1(_, _) => panic!(),
            Ptm::Ptm2(m, stp1, stp2) => {
                let (stp_new1, cond_weight1) = stp1.extract_cond();
                let (stp_new2, cond_weight2) = stp2.extract_cond();
                (Ptm::Ptm2(m.clone(), stp_new1, stp_new2), cond_weight1, cond_weight2)
            }
        }
    }
}
impl ExtractCond for Stp {
    type Output = (Stp, CondWeight);
    fn extract_cond(&self) -> Self::Output {
        match self {
            Stp::Stp(_) => panic!(),
            Stp::StpWeight(box_stp, cond_weight) => {
                (*box_stp.clone(), cond_weight.clone())
            }
        }
    }
}

pub trait ExtendCond {
    type Output;
    fn extend_cond(&self) -> Self::Output;
}

impl ExtendCond for Stral {
    type Output = Stral;
    fn extend_cond(&self) -> Self::Output {
        self.0
            .iter()
            .fold(Stral(vec![]), |x, y| x + y.extend_cond())
    }
}

impl ExtendCond for Stra {
    type Output = Stral;
    fn extend_cond(&self) -> Self::Output {
        let ptm_vec = self.1.extend_cond();
        let stra_vec = ptm_vec.iter()
            .map(|x| {
                Stra(self.0.clone(), x.clone())
            })
            .collect_vec();
        Stral(stra_vec)
    }
}

impl ExtendCond for Ptm {
    type Output = Vec<Ptm>;
    fn extend_cond(&self) -> Self::Output {
        match self {
            Ptm::Ptm1(_, _) => panic!(""),
            Ptm::Ptm2(m, stp1, stp2) => {
                let stp_vec1 = stp1.extend_cond();
                let stp_vec2 = stp2.extend_cond();
                izip!(stp_vec1.into_iter(), stp_vec2.into_iter())
                    .map(|(x, y)| Ptm::Ptm2({
                        let m_f = m.get_init_weight();
                        let m_new = M1(m_f * x.1);
                        Box::new(m_new)
                    }, x.0, y.0))
                    .collect_vec()
            }
        }
    }
}

impl ExtendCond for Stp {
    type Output = Vec<(Stp, f32)>;
    fn extend_cond(&self) -> Self::Output {
        match self {
            Stp::Stp(_) => panic!(""),
            Stp::StpWeight(stp, weight) => {
                let stp_new = *stp.clone();
                weight.0.iter()
                    .map(|x| {
                        (stp_new.attach_box_cond(and, &x.0, &Lo), x.1)
                    })
                    .collect_vec()
            }
        }
    }
}

pub trait ExtendDire {
    type Output;
    fn extend_dire(&self) -> Self::Output;
}

impl ExtendDire for Stral {
    type Output = Stral;
    fn extend_dire(&self) -> Self::Output {
        self.0
            .iter()
            .fold(Stral(vec![]), |x, y| x + y.extend_dire())
    }
}

impl ExtendDire for Stra {
    type Output = Stral;
    fn extend_dire(&self) -> Self::Output {
        match self {
            Stra(_, Ptm::Ptm1(_, _)) => {
                Stral(vec![self.clone()])
            },
            Stra(di_name, Ptm::Ptm2(m, stp1, stp2)) => {
                Stral(vec![
                    Stra(di_name.clone(), Ptm::Ptm1(m.clone(), stp1.clone())),
                    Stra(di_name.clone(), Ptm::Ptm1(m.clone(), stp2.clone())),
                    ])
            } 
        }
    }
}