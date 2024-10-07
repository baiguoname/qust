use crate::{idct::prelude::*, std_prelude::*, trade::prelude::*};
use qust_ds::prelude::*;
use qust_derive::*;

/* #region Price */
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PriceTick {
    #[serde(
        serialize_with = "serialize_vec_dt",
        deserialize_with = "deserialize_vec_dt"
    )]
    pub t: vdt,
    pub c: v32,
    pub v: v32,
    pub ct: Vec<i32>,
    pub bid1: v32,
    pub ask1: v32,
    pub bid1_v: v32,
    pub ask1_v: v32,
}

impl PriceTick {
    pub fn with_capacity(i: usize) -> Self {
        Self {
            t: Vec::with_capacity(i),
            c: Vec::with_capacity(i),
            v: Vec::with_capacity(i),
            ct: Vec::with_capacity(i),
            bid1: Vec::with_capacity(i),
            ask1: Vec::with_capacity(i),
            bid1_v: Vec::with_capacity(i),
            ask1_v: Vec::with_capacity(i),
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.t.shrink_to_fit();
        self.c.shrink_to_fit();
        self.v.shrink_to_fit();
        self.ct.shrink_to_fit();
        self.bid1.shrink_to_fit();
        self.ask1.shrink_to_fit();
        self.bid1_v.shrink_to_fit();
        self.ask1_v.shrink_to_fit();
    }

    pub fn cat(&mut self, price: &mut PriceTick) {
        self.t.append(&mut price.t);
        self.c.append(&mut price.c);
        self.v.append(&mut price.v);
        self.ct.append(&mut price.ct);
        self.bid1.append(&mut price.bid1);
        self.ask1.append(&mut price.ask1);
        self.bid1_v.append(&mut price.bid1_v);
        self.ask1_v.append(&mut price.ask1_v);
    }

    pub fn to_price_ori(&self, r: TriBox, ticker: Ticker) -> PriceOri {
        if self.t.is_empty() {
            return PriceOri::with_capacity(0);
        }
        let mut price_ori = r.gen_price_ori(self);
        let mut f = r.update_tick_func(ticker);
        for (&t, &c, &v, &bid1, &ask1, &bid1_v, &ask1_v, &ct) in izip!(
            self.t.iter(),
            self.c.iter(),
            self.v.iter(),
            self.bid1.iter(),
            self.ask1.iter(),
            self.bid1_v.iter(),
            self.ask1_v.iter(),
            self.ct.iter(),
        ) {
            let tick_data = TickData {
                t,
                c,
                v,
                bid1,
                ask1,
                bid1_v,
                ask1_v,
                ct,
            };
            f(&tick_data, &mut price_ori);
        }
        price_ori.shrink_to_fit();
        price_ori
    }

    pub fn to_di(&self, r: TriBox, ticker: Ticker) -> Di {
        self.to_price_ori(r.clone(), ticker)
            .to_pcon(r, ticker)
            .to_di()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KlineInfo {
    #[serde(serialize_with = "serialize_dt", deserialize_with = "deserialize_dt")]
    pub open_time: dt,
    pub pass_last: u16,
    pub pass_this: u16,
    pub contract: i32,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PriceOri {
    #[serde(
        serialize_with = "serialize_vec_dt",
        deserialize_with = "deserialize_vec_dt"
    )]
    pub t: vdt,
    pub o: v32,
    pub h: v32,
    pub l: v32,
    pub c: v32,
    pub v: v32,
    pub ki: Vec<KlineInfo>,
    pub immut_info: Vec<vv32>,
}

impl PriceOri {
    pub fn with_capacity(i: usize) -> Self {
        PriceOri {
            t: Vec::with_capacity(i),
            o: Vec::with_capacity(i),
            h: Vec::with_capacity(i),
            l: Vec::with_capacity(i),
            c: Vec::with_capacity(i),
            v: Vec::with_capacity(i),
            ki: Vec::with_capacity(i),
            immut_info: Default::default(),
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.t.shrink_to_fit();
        self.o.shrink_to_fit();
        self.h.shrink_to_fit();
        self.l.shrink_to_fit();
        self.c.shrink_to_fit();
        self.v.shrink_to_fit();
        self.ki.shrink_to_fit();
    }

    pub fn cat(&mut self, price: &mut PriceOri) {
        self.t.append(&mut price.t);
        self.o.append(&mut price.o);
        self.h.append(&mut price.h);
        self.l.append(&mut price.l);
        self.c.append(&mut price.c);
        self.v.append(&mut price.v);
        self.ki.append(&mut price.ki);
    }

    pub fn to_pcon(self, inter: TriBox, ticker: Ticker) -> Pcon {
        Pcon {
            price: self,
            inter,
            ticker,
        }
    }
    pub fn to_di(self, ticker: Ticker, inter: TriBox) -> Di {
        self.to_pcon(inter, ticker).to_di()
    }
}

#[derive(Clone)]
pub struct PriceArc {
    pub t: avdt,
    pub o: av32,
    pub h: av32,
    pub l: av32,
    pub c: av32,
    pub v: av32,
    pub ki: Arc<Vec<KlineInfo>>,
    pub immut_info: Vec<Arc<vv32>>,
    pub finished: Option<Vec<KlineState>>,
}

impl PriceArc {
    pub fn to_price_ori(self) -> PriceOri {
        PriceOri {
            t: self.t.to_vec(),
            o: self.o.to_vec(),
            h: self.h.to_vec(),
            l: self.l.to_vec(),
            c: self.c.to_vec(),
            v: self.v.to_vec(),
            ki: self.ki.to_vec(),
            immut_info: self
                .immut_info
                .into_iter()
                .map(|x| x.to_vec())
                .collect_vec(),
        }
    }
}

/* #endregion */

/* #region Pcon */
#[derive(Clone, Serialize, Deserialize)]
pub struct PconType<T, N> {
    pub ticker: Ticker,
    pub inter: T,
    pub price: N,
}
pub type Pcon = PconType<TriBox, PriceOri>;

#[ta_derive]
pub struct PconIdent {
    pub inter: TriBox,
    pub ticker: Ticker,
}

impl PconIdent {
    pub fn new(inter: TriBox, ticker: Ticker) -> Self {
        Self { inter, ticker }
    }
}

impl std::fmt::Display for PconIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {:?}", self.ticker, self.inter)
    }
}
impl PartialEq for PconIdent {
    fn eq(&self, other: &Self) -> bool {
        self.ticker == other.ticker && format!("{:?}", self.inter) == format!("{:?}", other.inter)
    }
}
impl Eq for PconIdent {}
impl std::hash::Hash for PconIdent {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        format!("{:?}", self).hash(state)
    }
}

impl PartialEq for Pcon {
    fn eq(&self, other: &Self) -> bool {
        self.ident() == other.ident()
    }
}

impl Pcon {
    pub fn from_price(price: PriceOri, inter: TriBox, ticker: Ticker) -> Self {
        Pcon {
            ticker,
            inter,
            price,
        }
    }

    pub fn ident(&self) -> PconIdent {
        PconIdent::new(self.inter.clone(), self.ticker)
    }

    pub fn to_di(self) -> Di {
        Di {
            pcon: self,
            data_save: DataSave::default(),
            dcon: RwLock::new(vec![Tf(0, 1)]),
            part: RwLock::new(vec![Part::ono]),
        }
    }
}

/* #endregion */

/* #region Di */
#[derive(Serialize, Deserialize, AsRef)]
pub struct DiType<T> {
    pub pcon: T,
    #[serde(skip)]
    pub data_save: DataSave,
    pub dcon: RwLock<Vec<Convert>>,
    pub part: RwLock<Vec<Part>>,
}
pub type Di = DiType<Pcon>;

impl Clone for Di {
    fn clone(&self) -> Self {
        self.pcon.clone().to_di()
    }
}

impl Di {
    pub fn size(&self) -> usize {
        self.pcon.price.t.len()
    }
    pub fn last_dcon(&self) -> Convert {
        let dcon_vec = self.dcon.read().unwrap();
        dcon_vec[dcon_vec.len() - 1].clone()
    }

    pub fn last_part(&self) -> Part {
        let part_vec = self.part.read().unwrap();
        part_vec[part_vec.len() - 1].clone()
    }

    pub fn get_kline(&self, p: &KlineType) -> av32 {
        match p {
            KlineType::Open => self.o(),
            KlineType::High => self.h(),
            KlineType::Low => self.l(),
            _ => self.c(),
        }
    }

    pub fn repeat(&self, n: usize) -> Dil {
        Dil {
            dil: vec![self.clone(); n],
        }
    }

    pub fn t(&self) -> avdt {
        self.calc(self.last_dcon()).t
    }
    pub fn o(&self) -> av32 {
        self.calc(self.last_dcon()).o
    }
    pub fn h(&self) -> av32 {
        self.calc(self.last_dcon()).h
    }
    pub fn l(&self) -> av32 {
        self.calc(self.last_dcon()).l
    }
    pub fn c(&self) -> av32 {
        self.calc(self.last_dcon()).c
    }
    pub fn v(&self) -> av32 {
        self.calc(self.last_dcon()).v
    }
    pub fn immut_info(&self) -> Vec<Arc<vv32>> {
        self.calc(self.last_dcon()).immut_info
    }

    pub fn len(&self) -> usize {
        self.pcon.price.t.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pcon.price.t.is_empty()
    }

    pub fn clear(&self) {
        self.data_save.clear();
    }

    pub fn clear2(&self) {
        self.data_save.save_pms2d.write().unwrap().clear();
        self.data_save.save_dcon.write().unwrap().clear();
        self.data_save.save_others.write().unwrap().clear();
    }

    pub fn calc<T: AsRef<N>, N: Calc<R> + ?Sized, R>(&self, x: T) -> R {
        x.as_ref().calc(self)
    }

    pub fn tz_profit(&self) -> f32 {
        let tz = self.pcon.ticker.info().tz;
        10000. * tz / self.pcon.price.c.last().unwrap()
    }
}

impl Debug for Di {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:<15} ---  {:<24} .. {:<24}  ---  {:<10} --- {}",
            self.pcon.ident().to_string(),
            self.pcon.price.ki.first().unwrap().open_time.to_string(),
            self.pcon.price.t.last().unwrap().to_string(),
            self.pcon.price.t.len().to_string(),
            (self.pcon.price.ki.map(|x| x.pass_this as f32).mean() / 120.) as usize,
        )
    }
}

/* #endregion */

/* #region Dil */
#[derive(Serialize, Deserialize, Clone)]
pub struct Dil {
    pub dil: Vec<Di>,
}
impl Dil {
    pub fn clear(&self) {
        self.dil.iter().for_each(|x| x.clear());
    }
    pub fn clear1(&self) {
        self.dil
            .iter()
            .for_each(|x| x.data_save.save_pms2d.write().unwrap().clear());
    }
    pub fn clear2(&mut self) {
        self.dil
            .iter()
            .for_each(|x| x.data_save.save_dcon.write().unwrap().clear());
    }

    pub fn total_kline_nums(&self) -> usize {
        self.dil.iter().map(|x| x.size()).sum::<usize>()
    }
}

impl Debug for Dil {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dil size {}\n{}",
            self.size(),
            self.dil
                .iter()
                .map(|x| x.debug_string() + "\n")
                .collect_vec()
                .concat(),
        )
    }
}
/* #endregion */

/* #region Price -> PriceArc */
pub trait ToArc {
    type Output;
    fn to_arc(self) -> Self::Output;
}

impl<T> ToArc for Vec<T> {
    type Output = Arc<Vec<T>>;
    fn to_arc(self) -> Self::Output {
        Arc::new(self)
    }
}

impl ToArc for PriceOri {
    type Output = PriceArc;
    fn to_arc(self) -> Self::Output {
        PriceArc {
            t: self.t.to_arc(),
            o: self.o.to_arc(),
            h: self.h.to_arc(),
            l: self.l.to_arc(),
            c: self.c.to_arc(),
            v: self.v.to_arc(),
            ki: self.ki.to_arc(),
            immut_info: self.immut_info.map(|x| x.clone().to_arc()),
            finished: None,
        }
    }
}

impl ToArc for (PriceOri, Option<Vec<KlineState>>) {
    type Output = PriceArc;
    fn to_arc(self) -> Self::Output {
        let mut res = self.0.to_arc();
        res.finished = self.1;
        res
    }
}
/* #endregion */
