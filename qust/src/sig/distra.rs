#![allow(clippy::op_ref)]
use super::posi::Money;
use crate::{idct::ta::CommSlip, prelude::Dire, sig::livesig::Ptm, sig::pnl::*, trade::prelude::*};
use qust_ds::prelude::*;
use qust_derive::*;
use std::{hash::Hash, ops::Add, sync::Arc, thread};

#[ta_derive]
#[derive(PartialEq, Eq)]
pub enum StraKind {
    Trend,
    Reverse,
}

impl std::ops::Not for StraKind {
    type Output = StraKind;
    fn not(self) -> Self::Output {
        match self {
            StraKind::Trend => StraKind::Reverse,
            StraKind::Reverse => StraKind::Trend,
        }
    }
}

#[ta_derive]
#[derive(Default)]
pub struct StraName {
    pub dire: Option<Dire>,
    pub kind: Option<StraKind>,
    pub frame: Option<String>,
    pub id: Option<usize>,
}

impl std::fmt::Display for StraName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn o<T: std::fmt::Debug>(data: Option<&T>) -> String {
            data.map_or_else(|| "None".into(), |v| v.debug_string())
        }
        write!(
            f,
            "{} - {} - {}",
            o(self.dire.as_ref()),
            o(self.kind.as_ref()),
            self.frame
                .as_ref()
                .map_or_else(|| "None".into(), |v| v.clone()),
        )?;
        if let Some(i) = self.id {
            write!(f, "- {}", i)?;
        }
        Ok(())
    }
}

impl StraName {
    pub fn new_with_name(name: &str) -> Self {
        Self {
            frame: name.to_string().into(),
            ..Default::default()
        }
    }

    pub fn set_dire(mut self, dire: Dire) -> Self {
        self.dire = dire.into();
        self
    }

    pub fn set_kind(mut self, kind: StraKind) -> Self {
        self.kind = kind.into();
        self
    }

    pub fn set_frame(mut self, name: &str) -> Self {
        self.frame = name.to_string().into();
        self
    }

    pub fn set_id(mut self, id: usize) -> Self {
        self.id = id.into();
        self
    }

    pub fn set_reverse(mut self) -> Self {
        if let Some(dire) = self.dire {
            self.dire = (!dire).into();
        }
        if let Some(kind) = self.kind {
            self.kind = (!kind).into();
        }
        self
    }

    pub fn frame(&self) -> &str {
        self.frame.as_ref().unwrap()
    }
}

/* #region Stra */
#[ta_derive]
// #[serde(from = "crate::trade::version::Stra", into = "crate::trade::version::Stra")]
pub struct Stra {
    pub ident: PconIdent,
    pub name: StraName,
    pub ptm: Ptm,
}

impl Stra {
    pub fn new(ident: PconIdent, name: StraName, ptm: Ptm) -> Self {
        Self { ident, name, ptm }
    }

    pub fn new_with_name(ident: PconIdent, name: &str, ptm: Ptm) -> Self {
        Self {
            ident,
            name: StraName::new_with_name(name),
            ptm,
        }
    }

    pub fn change_name(&self, name: &str) -> Self {
        Self {
            name: self.name.clone().set_frame(name),
            ..self.clone()
        }
    }

    pub fn change_money<T: Money>(&self, money: T) -> Self {
        Self {
            ptm: self.ptm.change_money(money),
            ..self.clone()
        }
    }

    pub fn mul_money(&self, rhs: f32) -> Self {
        Stra {
            ptm: self.ptm.change_money_box(self.ptm.get_money_fn() * rhs),
            ..self.clone()
        }
    }
}

impl PartialEq for Stra {
    fn eq(&self, other: &Self) -> bool {
        self.ident == other.ident && self.ptm == other.ptm
    }
}
impl Eq for Stra {}
impl Hash for Stra {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ident.hash(state);
        self.name.debug_string().hash(state);
    }
}
/* #endregion */

/* #region Stral */
#[ta_derive]
pub struct Stral(pub Vec<Stra>);

impl Stral {
    pub fn get_part(&self, range: std::ops::Range<usize>) -> Self {
        Stral(self.0[range].to_vec())
    }

    pub fn change_money<T: Money + Clone>(&self, money: T) -> Self {
        self.0
            .map(|x| x.change_money(money.clone()))
            .to_stral_bare()
    }

    pub fn mul_money(&self, rhs: f32) -> Self {
        self.0
            .iter()
            .map(|x| x.mul_money(rhs))
            .collect_vec()
            .to_stral_bare()
    }

    pub fn change_name(&self, name: &str) -> Self {
        self.0.map(|x| x.change_name(name)).to_stral_bare()
    }
}

impl<T, N> std::ops::Index<T> for Stral
where
    Vec<Stra>: std::ops::Index<T, Output = N>,
{
    type Output = N;
    fn index(&self, index: T) -> &Self::Output {
        self.0.index(index)
    }
}

pub trait ToStralBare {
    fn to_stral_bare(&self) -> Stral;
}

impl ToStralBare for (&Ptm, &Vec<PconIdent>) {
    fn to_stral_bare(&self) -> Stral {
        self.1
            .iter()
            .map(|x| Stra::new(x.clone(), Default::default(), self.0.clone()))
            .collect_vec()
            .pip(Stral)
    }
}
impl ToStralBare for (&Ptm, &(Vec<Ticker>, TriBox)) {
    fn to_stral_bare(&self) -> Stral {
        self.1
             .0
            .iter()
            .map(|x| PconIdent::new(self.1 .1.clone(), *x))
            .collect_vec()
            .pip(|x| (self.0, &x).to_stral_bare())
    }
}
impl ToStralBare for (&Ptm, &Di) {
    fn to_stral_bare(&self) -> Stral {
        (self.0, &vec![self.1.pcon.ident()]).to_stral_bare()
    }
}
impl ToStralBare for (&Vec<Ptm>, &Di) {
    fn to_stral_bare(&self) -> Stral {
        self.0
            .map(|x| (x, self.1).to_stral_bare().0)
            .concat()
            .to_stral_bare()
    }
}
impl ToStralBare for (&Ptm, &Dil) {
    fn to_stral_bare(&self) -> Stral {
        (
            self.0,
            &self.1.dil.iter().map(|x| x.pcon.ident()).collect_vec(),
        )
            .to_stral_bare()
    }
}
impl ToStralBare for (&Vec<Ptm>, &Dil) {
    fn to_stral_bare(&self) -> Stral {
        self.0
            .map(|x| (x, self.1).to_stral_bare().0)
            .concat()
            .to_stral_bare()
    }
}
impl ToStralBare for [Stra] {
    fn to_stral_bare(&self) -> Stral {
        self.to_vec().pip(Stral)
    }
}
impl<T, N> ToStralBare for (Vec<T>, N)
where
    for<'a> (&'a T, N): ToStralBare,
    N: Clone,
{
    fn to_stral_bare(&self) -> Stral {
        self.0.iter().fold(Stral(vec![]), |mut accu, x| {
            let mut stral = (x, self.1.clone()).to_stral_bare();
            accu.0.append(&mut stral.0);
            accu
        })
    }
}
impl ToStralBare for [Stral] {
    fn to_stral_bare(&self) -> Stral {
        self.iter()
            .fold(vec![], |mut accu, x| {
                let mut g = x.0.clone();
                accu.append(&mut g);
                accu
            })
            .to_stral_bare()
    }
}

impl ToStralBare for Stra {
    fn to_stral_bare(&self) -> Stral {
        [self.clone()].to_stral_bare()
    }
}

pub trait ToStral {
    fn to_stral<T>(&self, data: T) -> Stral
    where
        Self: Sized,
        for<'a> (&'a Self, T): ToStralBare,
    {
        (self, data).to_stral_bare()
    }
}

impl<T> ToStral for T {}

impl Add<&Stral> for &Stral {
    type Output = Stral;
    fn add(self, rhs: &Stral) -> Self::Output {
        Stral([self.0.clone(), rhs.0.clone()].concat())
    }
}

impl Add<Stral> for Stral {
    type Output = Stral;
    fn add(self, rhs: Stral) -> Self::Output {
        Stral([self.0, rhs.0].concat())
    }
}
/* #endregion */

/* #region DiStral */
pub struct DiStra<'a, 'b> {
    pub di: &'a Di,
    pub stra: &'b Stra,
}

#[derive(AsRef)]
pub struct DiStral<'a> {
    pub dil: &'a Dil,
    pub stral: Stral,
    pub index_vec: Vec<vuz>,
}

impl DiStral<'_> {
    pub fn calc_with_progress<'a, T, N, P>(&self, f: T, p: &'a P) -> Vec<N>
    where
        T: CalcStra<Output = N> + Clone,
        N: Send + Sync,
        P: ToProgressBar<Output<'a> = usize>,
    {
        let res = thread::scope(|scope| {
            let p = Arc::new(p.to_progressbar());
            let mut handles = vec![];
            for (di, index_vec) in self.dil.dil.iter().zip(self.index_vec.iter()) {
                let stra_vec = index_vec.iter().map(|&i| &self.stral.0[i]);
                let f_ = &f;
                let p_ = Arc::clone(&p);
                let handle = scope.spawn(move || {
                    stra_vec
                        .map(|stra| {
                            di.data_save.clear_with_condition();
                            p_.inc();
                            f_.calc_stra(&DiStra { di, stra })
                        })
                        .collect_vec()
                });
                handles.push(handle);
            }
            handles
                .into_iter()
                .flat_map(|x| x.join().unwrap())
                .collect_vec()
        });
        let index_vec = self.index_vec.concat();
        res.sort_perm(&index_vec)
    }

    pub fn calc<T, N>(&self, f: T) -> Vec<N>
    where
        T: CalcStra<Output = N> + Clone,
        N: Send + Sync,
    {
        self.calc_with_progress(f, &(self.stral.size(), 1000usize))
    }

    // pub fn calc<T: CalcStra<Output = N> + Clone, N: Send + Sync>(&self, f: T) -> Vec<N> {
    //     let k = self.stral.size();
    //     let res = thread::scope(|scope| {
    //         let p = Arc::new(k.to_progressbar());
    //         let mut handles = vec![];
    //         for (di, index_vec) in
    //                 self.dil.dil.iter().zip(self.index_vec.iter()) {
    //             let stra_vec = index_vec.iter().map(|&i| &self.stral.0[i]);
    //             let clear_thre = 300usize;
    //             let f = f.clone();
    //             let p_ = Arc::clone(&p);
    //             let handle = scope.spawn(move || {
    //                 stra_vec
    //                     .map(|stra| {
    //                         if di.data_save.len_sum() > clear_thre { di.clear(); }
    //                         p_.inc();
    //                         f.calc_stra(&DiStra { di, stra })
    //                     })
    //                     .collect_vec()
    //             });
    //             handles.push(handle);
    //         };
    //         handles
    //             .into_iter()
    //             .flat_map(|x| x.join().unwrap())
    //             .collect_vec()
    //     });
    //     let index_vec = self.index_vec.concat();
    //     res.sort_perm(&index_vec)
    // }
}

pub trait CalcStra: Send + Sync {
    type Output;
    fn calc_stra(&self, distra: &DiStra) -> Self::Output;
}
impl CalcStra for CommSlip {
    type Output = PnlRes<da>;
    fn calc_stra(&self, distra: &DiStra) -> Self::Output {
        distra.di.pnl(&distra.stra.ptm, self.clone()).da()
    }
}
impl CalcStra for (CommSlip,) {
    type Output = PnlRes<dt>;
    fn calc_stra(&self, distra: &DiStra) -> Self::Output {
        distra.di.pnl(&distra.stra.ptm, self.0.clone())
    }
}
impl<T: Fn(&DiStra) -> N + Send + Sync, N> CalcStra for T {
    type Output = N;
    fn calc_stra(&self, distra: &DiStra) -> Self::Output {
        self(distra)
    }
}

#[derive(Clone)]
pub struct Aee<T>(pub T);
impl<T: CalcStra<Output = PnlRes<N>>, N> CalcStra for Aee<T> {
    type Output = InfoPnlRes<Stra, N>;
    fn calc_stra(&self, distra: &DiStra) -> Self::Output {
        InfoPnlRes(distra.stra.clone(), self.0.calc_stra(distra))
    }
}
/* #endregion */

/* #region generate distral */
pub trait GenDiStral {
    fn dil<'a>(&self, dil: &'a Dil) -> DiStral<'a>;
}

impl GenDiStral for Stral {
    fn dil<'a>(&self, dil: &'a Dil) -> DiStral<'a> {
        let index_vec = dil
            .dil
            .iter()
            .map(|di| {
                self.0
                    .iter()
                    .enumerate()
                    .filter_map(|(i, x)| {
                        if &x.ident == &di.pcon.ident() {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect_vec()
            })
            .collect_vec();
        let index_vec = even_slice_index(&index_vec);
        DiStral {
            dil,
            stral: self.clone(),
            index_vec,
        }
    }
}

impl<T> GenDiStral for T
where
    for<'c, 'b> (&'c T, &'b Dil): ToStralBare,
{
    fn dil<'a>(&self, dil: &'a Dil) -> DiStral<'a> {
        // self.to_stral(dil).dil(dil)
        (self, dil).to_stral_bare().dil(dil)
    }
}

fn even_slice_index(data: &[vuz]) -> Vec<vuz> {
    let mut res = vec![vec![]; data.len()];
    let value_positions = data.value_positions();
    for (k, v) in value_positions.iter() {
        let chunk_size = (k.len() / v.len() + k.len() % v.len()).max(1usize);
        let k_ = k.chunks(chunk_size).map(|x| x.to_vec()).collect_vec();
        for (k_, v_) in izip!(k_.into_iter(), v.iter()) {
            res[*v_] = k_;
        }
    }
    res
}

/* #endregion */

#[ta_derive]
pub struct NamedPtm {
    pub name: StraName,
    pub ptm: Ptm,
}

impl NamedPtm {
    pub fn reverse(mut self) -> Self {
        self.name = self.name.set_reverse();
        self.ptm = match self.ptm {
            Ptm::Ptm3(m, dire, cond1, cond2) => Ptm::Ptm3(m, !dire, cond1, cond2),
            _ => panic!(),
        };
        self
    }
}

pub trait NameForPtm {
    type Output;
    fn name_for_ptm(self, name: StraName) -> Self::Output;
}

impl NameForPtm for Ptm {
    type Output = NamedPtm;
    fn name_for_ptm(self, name: StraName) -> Self::Output {
        NamedPtm { name, ptm: self }
    }
}

impl NameForPtm for NamedPtm {
    type Output = NamedPtm;
    fn name_for_ptm(self, name: StraName) -> Self::Output {
        NamedPtm {
            name,
            ptm: self.ptm,
        }
    }
}

impl NameForPtm for Vec<Ptm> {
    type Output = Vec<NamedPtm>;
    fn name_for_ptm(self, name: StraName) -> Self::Output {
        self.into_map(|x| x.name_for_ptm(name.clone()))
    }
}

impl ToStralBare for (&Vec<NamedPtm>, &Dil) {
    fn to_stral_bare(&self) -> Stral {
        self.0
            .iter()
            .map(|x| {
                self.1
                    .dil
                    .map(|di| Stra::new(di.pcon.ident(), x.name.clone(), x.ptm.clone()))
            })
            .concat()
            .to_stral_bare()
    }
}
