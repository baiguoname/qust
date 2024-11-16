use serde::{Serialize, Deserialize};
use qust::{
    sig::prelude::*,
    trade::prelude::*,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond1;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond2;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond3;


#[typetag::serde(name = "test_stra1")]
impl Cond for cond1 {
    fn cond<'a>(&self, _di: &'a Di) -> LoopSig<'a> {
        Box::new(
            move |e: usize, _o: usize| {
                e % 5 == 0
            }
        )
    }
}

#[typetag::serde(name = "test_stra2")]
impl Cond for cond2 {
    fn cond<'a>(&self, _di: &'a Di) -> LoopSig<'a> {
        Box::new(
            move |e: usize, _o: usize| {
                e % 3 == 0
            }
        )
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct condl2;
#[typetag::serde(name = "test_stral2")]
impl Cond for condl2 {
    fn cond<'a>(&self, _di: &'a Di) -> LoopSig<'a> {
        Box::new(
            move |e: usize, _o: usize| {
                e % 2 == 0
            }
        )
    }
}


#[typetag::serde(name = "test_stra3")]
impl Cond for cond3 {
    fn cond<'a>(&self, _di: &'a Di) -> LoopSig<'a> {
        Box::new(move |e: usize, o: usize| {
            e - o > 3
        })
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let tsig_l = Tsig::new(Lo, &cond1, &cond3);
        let tsig_s = Tsig::new(Sh, &cond2, &cond3);
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
}

lazy_static! {
    pub static ref ptm2: Ptm = {
        let tsig_l = Tsig::new(Lo, &condl2, &cond3);
        let stp_l = Stp::Stp(tsig_l);
        Ptm::Ptm1(Box::new(M1(1.)), stp_l)
    };
}
