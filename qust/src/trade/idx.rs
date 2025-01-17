use super::inter::TriBox;
use crate::prelude::{
    find_day_index_night_pre, PconIdent, PriceArc, Stra, StraKind, Stral, Ticker,
};
use crate::prelude::{Di, Dil, InfoPnlRes, PnlRes, PriceOri, PriceTick};
use qust_ds::prelude::*;
use std::borrow::Cow;
use std::ops::Range;

#[derive(Clone)]
pub enum Idx {
    Range(Range<usize>),
    List(Vec<usize>),
    Bool(Vec<bool>),
}

impl Idx {

    pub fn index_out<T>(&self, data: &[T]) -> Vec<T>
    where
        T: Clone,
    {
        match self {
            Idx::Range(r) => {
                data[r.clone()].to_vec()
            }
            Idx::List(v) => {
                data.get_list_index(v)
            }
            Idx::Bool(v) => {
                data.iter().cloned().zip(v.iter())
                    .filter_map(|(d, t)| {
                        t.then_some(d)
                    })
                    .collect_vec()
            }
        }

    }
}

pub trait IdxOut {
    fn idx_out(&self, idx: Idx) -> Self;
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>>;
}

pub trait GetPart<T> {
    fn get_part(&self, idx: T) -> Self;
}

impl<T> GetPart<Range<usize>> for T
where
    T: IdxOut,
{
    fn get_part(&self, idx: Range<usize>) -> Self {
        let idx = Idx::Range(idx);
        self.idx_out(idx)
    }
}

impl<T> GetPart<Vec<usize>> for T
where
    T: IdxOut,
{
    fn get_part(&self, idx: Vec<usize>) -> Self {
        let idx = Idx::List(idx);
        self.idx_out(idx)
    }
}

impl<T> GetPart<ForCompare<dt>> for T
where
    T: IdxOut + Clone,
{
    fn get_part(&self, idx: ForCompare<dt>) -> Self {
        let time_vec = self.get_time_vec();
        // let idx = idx.as_ref();
        let idx = match idx {
            ForCompare::List(_) => time_vec
                .iter()
                .enumerate()
                .filter_map(|(i, t)| if idx.compare_same(t) { Some(i) } else { None })
                .collect_vec()
                .pip(Idx::List),
            _ => {
                let start_i = time_vec.iter().position(|x| idx.compare_same(x));
                let end_i = time_vec.iter().rev().position(|x| idx.compare_same(x));
                match (start_i, end_i) {
                    (Some(i), Some(j)) => i..time_vec.len() - j,
                    (Some(i), None) => i..time_vec.len(),
                    (None, Some(j)) => 0..time_vec.len() - j,
                    (None, None) => 0..0,
                }
                .pip(Idx::Range)
            }
        };
        self.idx_out(idx)
    }
}

impl<T> GetPart<T> for Dil
where
    Di: GetPart<T>,
    T: Clone,
{
    fn get_part(&self, idx: T) -> Self {
        let di_vec = self.dil.map(|x| x.get_part(idx.clone()));
        Dil { dil: di_vec }
    }
}

impl<T> GetPart<Range<tt>> for T
where
    T: IdxOut,
{
    fn get_part(&self, idx: Range<tt>) -> Self {
        let index_vec = self
            .get_time_vec()
            .iter()
            .enumerate()
            .flat_map(|(i, x)| {
                if idx.contains(&x.time()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect_vec();
        let idx = Idx::List(index_vec);
        self.idx_out(idx)
    }
}

impl<T, N> GetPart<T> for Vec<N>
where
    N: GetPart<T>,
    T: Clone,
{
    fn get_part(&self, idx: T) -> Self {
        self.map(|x| x.get_part(idx.clone()))
    }
}

#[derive(Clone)]
pub enum NLast {
    Num(usize),
    Day(usize),
    DayFirst(usize),
    DayNth(usize),
}
pub const last_day: NLast = NLast::Day(1);

impl<T> GetPart<NLast> for T
where
    T: IdxOut + Clone + HasLen,
{
    fn get_part(&self, idx: NLast) -> Self {
        match idx {
            NLast::Num(n) => {
                let end = self.size();
                let start = end - n.min(end);
                let idx = Idx::Range(start..end);
                self.idx_out(idx)
            }
            NLast::Day(n) => {
                let time_vec = self.get_time_vec();
                let cut_points = find_day_index_night_pre(&time_vec);
                let start_point = cut_points.iter().cloned().nth_back(n).unwrap_or_default();
                let end_point = cut_points.last().cloned().unwrap_or_default();
                self.get_part(start_point..end_point)
            }
            NLast::DayFirst(n) => {
                let time_vec = self.get_time_vec();
                let cut_points = find_day_index_night_pre(&time_vec);
                let end_point = cut_points.iter().cloned().nth(n).unwrap_or_default();
                self.get_part(0..end_point)
            }
            NLast::DayNth(n) => {
                let time_vec = self.get_time_vec();
                let cut_points = find_day_index_night_pre(&time_vec);
                let start_point = cut_points[n];
                let end_point = cut_points[n + 1];
                self.get_part(start_point..end_point)
            }
        }
    }
}

impl IdxOut for PriceTick {
    fn idx_out(&self, idx: Idx) -> Self {
        PriceTick {
            t: idx.index_out(&self.t),
            c: idx.index_out(&self.c),
            v: idx.index_out(&self.v),
            ct: idx.index_out(&self.ct),
            bid1: idx.index_out(&self.bid1),
            ask1: idx.index_out(&self.ask1),
            bid1_v: idx.index_out(&self.bid1_v),
            ask1_v: idx.index_out(&self.ask1_v),
        }
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        Cow::Borrowed(&self.t)
    }
}

impl IdxOut for PriceOri {
    fn idx_out(&self, idx: Idx) -> Self {
        PriceOri {
            t: idx.index_out(&self.t),
            o: idx.index_out(&self.o),
            h: idx.index_out(&self.h),
            l: idx.index_out(&self.l),
            c: idx.index_out(&self.c),
            v: idx.index_out(&self.v),
            ki: idx.index_out(&self.ki),
            immut_info: {
                if self.immut_info.len() < self.t.len() {
                    self.immut_info.clone()
                } else {
                    idx.index_out(&self.immut_info)
                }
            },
        }

    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        Cow::Borrowed(&self.t)
    }
}

impl IdxOut for Di {
    fn idx_out(&self, idx: Idx) -> Self {
        self.pcon
            .price
            .idx_out(idx)
            .to_pcon(self.pcon.inter.clone(), self.pcon.ticker)
            .to_di()
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        self.pcon.price.get_time_vec()
    }
}

impl IdxOut for PnlRes<dt> {
    fn idx_out(&self, idx: Idx) -> Self {
        Self(
            idx.index_out(&self.0),
            self.1.map(|x| idx.index_out(x))
        )
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        Cow::Borrowed(&self.0)
    }
}

impl IdxOut for PnlRes<da> {
    fn idx_out(&self, idx: Idx) -> Self {
        Self(
            idx.index_out(&self.0),
            self.1.map(|x| idx.index_out(x))
        )
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        let data = self.0.map(|x| x.to_dt());
        Cow::Owned(data)
    }
}

impl<T, N> IdxOut for InfoPnlRes<T, N>
where
    T: Clone,
    PnlRes<N>: IdxOut,
{
    fn idx_out(&self, idx: Idx) -> Self {
        InfoPnlRes(self.0.clone(), self.1.idx_out(idx))
    }
    fn get_time_vec(&self) -> Cow<'_, Vec<dt>> {
        self.1.get_time_vec()
    }
}

trait GetEqual<I> {
    fn get_equal(&self, other: &I) -> bool;
}

impl GetEqual<Ticker> for Ticker {
    fn get_equal(&self, other: &Ticker) -> bool {
        self == other
    }
}
impl GetEqual<Ticker> for Di {
    fn get_equal(&self, other: &Ticker) -> bool {
        &self.pcon.ticker == other
    }
}
impl GetEqual<TriBox> for Di {
    fn get_equal(&self, other: &TriBox) -> bool {
        &self.pcon.inter == other
    }
}
impl GetEqual<PconIdent> for Di {
    fn get_equal(&self, other: &PconIdent) -> bool {
        &self.pcon.ident() == other
    }
}
impl<T> GetEqual<Vec<T>> for Di
where
    Di: GetEqual<T>,
    T: Sized,
{
    fn get_equal(&self, other: &Vec<T>) -> bool {
        for o in other.iter() {
            if self.get_equal(o) {
                return true;
            }
        }
        false
    }
}
impl<F> GetEqual<F> for Di
where
    F: Fn(&Di) -> bool,
{
    fn get_equal(&self, other: &F) -> bool {
        other(self)
    }
}

impl GetEqual<Ticker> for Stra {
    fn get_equal(&self, other: &Ticker) -> bool {
        &self.ident.ticker == other
    }
}
impl GetEqual<TriBox> for Stra {
    fn get_equal(&self, other: &TriBox) -> bool {
        &self.ident.inter == other
    }
}
impl GetEqual<PconIdent> for Stra {
    fn get_equal(&self, other: &PconIdent) -> bool {
        &self.ident == other
    }
}
impl GetEqual<StraKind> for Stra {
    fn get_equal(&self, other: &StraKind) -> bool {
        if let Some(x) = &self.name.kind {
            x == other
        } else {
            false
        }
    }
}
impl<F> GetEqual<F> for Stra
where
    F: Fn(&Stra) -> bool,
{
    fn get_equal(&self, other: &F) -> bool {
        other(self)
    }
}

#[derive(Clone)]
pub struct StraOnlyName<T>(pub T);
impl GetEqual<StraOnlyName<&str>> for Stra {
    fn get_equal(&self, other: &StraOnlyName<&str>) -> bool {
        self.name.frame() == other.0
    }
}
impl GetEqual<StraOnlyName<Vec<&str>>> for Stra {
    fn get_equal(&self, other: &StraOnlyName<Vec<&str>>) -> bool {
        other.0.contains(&self.name.frame())
    }
}
pub struct RevStraOnlyName<T>(pub T);
impl<T> GetEqual<RevStraOnlyName<T>> for Stra
where
    Stra: GetEqual<StraOnlyName<T>>,
    T: Clone,
{
    fn get_equal(&self, other: &RevStraOnlyName<T>) -> bool {
        !self.get_equal(&StraOnlyName(other.0.clone()))
    }
}
impl<T> std::ops::Not for StraOnlyName<T> {
    type Output = RevStraOnlyName<T>;
    fn not(self) -> Self::Output {
        RevStraOnlyName(self.0)
    }
}

impl<I, T, N> GetEqual<I> for InfoPnlRes<T, N>
where
    T: GetEqual<I>,
{
    fn get_equal(&self, other: &I) -> bool {
        self.0.get_equal(other)
    }
}

pub trait GetCdt<T> {
    type Output<'a>
    where
        Self: 'a;
    fn get_idx(&self, idx: T) -> Self::Output<'_>;
}

impl<T> GetCdt<T> for Dil
where
    Di: GetEqual<T>,
{
    type Output<'a> = Dil;
    fn get_idx(&self, idx: T) -> Self::Output<'_> {
        self.dil
            .get_idx(idx)
            .into_map(|x| x.clone())
            .pip(|x| Dil { dil: x })
    }
}

impl<T> GetCdt<T> for Stral
where
    Stra: GetEqual<T>,
{
    type Output<'a> = Stral;
    fn get_idx(&self, idx: T) -> Self::Output<'_> {
        self.0.get_idx(idx).into_map(|x| x.clone()).pip(Stral)
    }
}

impl<T, N> GetCdt<T> for [N]
where
    N: GetEqual<T>,
{
    type Output<'a> = Vec<&'a N> where N: 'a;
    fn get_idx(&self, idx: T) -> Self::Output<'_> {
        self.iter()
            .flat_map(|x| if x.get_equal(&idx) { Some(x) } else { None })
            .collect_vec()
    }
}

pub struct OnlyOne<T>(pub T);

impl<T> GetCdt<OnlyOne<T>> for Dil
where
    Di: GetEqual<T>,
{
    type Output<'a> = Option<&'a Di>;
    fn get_idx(&self, idx: OnlyOne<T>) -> Self::Output<'_> {
        let g = self.dil.iter().position(|x| x.get_equal(&idx.0))?;
        Some(&self.dil[g])
    }
}

pub trait HasLen {
    fn size(&self) -> usize;
}
impl<T> HasLen for [T] {
    fn size(&self) -> usize {
        self.len()
    }
}
impl HasLen for PriceTick {
    fn size(&self) -> usize {
        self.t.size()
    }
}
impl HasLen for PriceOri {
    fn size(&self) -> usize {
        self.t.size()
    }
}
impl HasLen for PriceArc {
    fn size(&self) -> usize {
        self.t.size()
    }
}
impl HasLen for Di {
    fn size(&self) -> usize {
        self.pcon.price.size()
    }
}
impl HasLen for Dil {
    fn size(&self) -> usize {
        self.dil.size()
    }
}
impl HasLen for Stral {
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl<T> HasLen for PnlRes<T> {
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl<T, N> HasLen for InfoPnlRes<T, N> {
    fn size(&self) -> usize {
        self.1.size()
    }
}


pub trait ExtractNear {
    fn extract_near(&self, i: usize, offset: usize) -> Self;
}

impl<T> ExtractNear for T
where
    T: IdxOut,
{
    fn extract_near(&self, i: usize, offset: usize) -> Self {
        let idx = Idx::Range((i - offset.min(i)) .. (i + offset));
        self.idx_out(idx)
    }
}
