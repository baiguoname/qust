#![allow(dead_code)]
use serde::{Deserialize, Serialize};
use regex::Regex;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Deserialize, Serialize)]
pub enum Ticker {
    al,
    cu,
    ni,
    sn,
    zn,
    bu,
    eg,
    MA,
    l,
    pp,
    TA,
    v,
    ru,
    eb,
    PF,
    SA,
    jm,
    FG,
    hc,
    i,
    j,
    SM,
    rb,
    SF,
    ZC,
    ss,
    p,
    y,
    OI,
    fu,
    sc,
    pg,
    au,
    ag,
    m,
    a,
    jd,
    RM,
    AP,
    SR,
    sp,
    CF,
    c,
    cs,
    SH,
    UR,
}

#[derive(Debug)]
pub enum Comm {
    F(f32),
    P(f32),
}

#[derive(Debug)]
pub struct TickerInfo {
    pub tz: f32,
    pub pv: f32,
    pub slip: f32,
    pub comm: Comm,
}
// pub struct TickerInfo(pub f32, pub f32, pub f32, pub Comm);

impl TickerInfo {
    const fn new(tz: f32, pv: f32, slip: f32, comm: Comm) -> Self {
        TickerInfo { tz, pv, slip, comm }
    }

    pub fn multi(&self, price: f32) -> f32 {
        self.pv * price
    }

    pub fn comm(&self, price: f32, num: f32) -> f32 {
        match self.comm {
            Comm::F(i) => num * i,
            Comm::P(i) => num * price * self.pv * i,
        }
    }

    pub fn slip(&self, num: f32) -> f32 {
        num * self.pv * self.slip
    }

    pub fn trade_money(&self, num: f32, price: f32) -> f32 {
        num * price * self.pv
    }
}

impl Ticker {
    pub const fn info(&self) -> TickerInfo {
        use Comm::*;
        use Ticker::*;
        match self {
            al => TickerInfo::new(5., 5., 1., F(3.)),
            cu => TickerInfo::new(10., 5., 1., P(0.5e-4)),
            ni => TickerInfo::new(10., 1., 1., F(3.)),
            sn => TickerInfo::new(10., 1., 10., F(3.)),
            zn => TickerInfo::new(5., 5., 1., F(3.)),
            bu => TickerInfo::new(1., 10., 0.5, P(1e-4)),
            eg => TickerInfo::new(1., 10., 0.5, F(3.)),
            MA => TickerInfo::new(1., 10., 0.5, F(2.)),
            l => TickerInfo::new(1., 5., 0.5, F(1.)),
            pp => TickerInfo::new(1., 5., 0.5, F(1.)),
            TA => TickerInfo::new(2., 5., 0.5, F(3.)),
            v => TickerInfo::new(1., 5., 2.5, F(1.)),
            ru => TickerInfo::new(5., 10., 0.3, F(3.)),
            eb => TickerInfo::new(1., 5., 2., F(3.)),
            PF => TickerInfo::new(2., 5., 0.5, F(3.)),
            SA => TickerInfo::new(1., 20., 0.5, F(3.5)),
            jm => TickerInfo::new(0.5, 60., 0.5, P(1.4e-4)),
            FG => TickerInfo::new(1., 20., 0.5, F(3.)),
            hc => TickerInfo::new(1., 10., 0.5, P(1e-4)),
            i => TickerInfo::new(0.5, 100., 0.5, P(1e-4)),
            j => TickerInfo::new(0.5, 100., 1., P(1.4e-4)),
            SM => TickerInfo::new(2., 5., 0.5, F(3.)),
            rb => TickerInfo::new(1., 10., 0.5, P(1e-4)),
            SF => TickerInfo::new(2., 5., 0.5, F(3.)),
            ZC => TickerInfo::new(0.2, 100., 0.5, F(151.)),
            ss => TickerInfo::new(5., 5., 0.5, F(2.)),
            p => TickerInfo::new(2., 10., 0.5, F(2.5)),
            y => TickerInfo::new(2., 5., 0.5, F(2.)),
            OI => TickerInfo::new(1., 10., 0.5, F(2.)),
            fu => TickerInfo::new(1., 10., 0.5, P(0.5e-4)),
            sc => TickerInfo::new(0.1, 1000., 0.5, F(20.)),
            pg => TickerInfo::new(1., 20., 0.5, F(6.)),
            au => TickerInfo::new(0.02, 1000., 0.5, F(10.)),
            ag => TickerInfo::new(1., 15., 0.5, P(0.5e-4)),
            m => TickerInfo::new(1., 10., 0.5, F(1.5)),
            a => TickerInfo::new(1., 10., 0.5, F(2.)),
            jd => TickerInfo::new(1., 10., 0.5, F(1.5)),
            RM => TickerInfo::new(1., 10., 0.5, F(1.5)),
            AP => TickerInfo::new(1., 10., 0.5, F(5.)),
            SR => TickerInfo::new(1., 10., 0.5, F(1.5)),
            sp => TickerInfo::new(2., 10., 0.5, P(0.5e-4)),
            CF => TickerInfo::new(5., 5., 0.5, F(5.)),
            c => TickerInfo::new(1., 10., 0.5, F(1.2)),
            cs => TickerInfo::new(1., 10., 0.5, F(1.5)),
            SH => TickerInfo::new(1., 30., 1., F(3.)),
            UR => TickerInfo::new(1., 20., 1., P(1e-4)),
        }
    }
}

impl std::fmt::Display for Ticker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub const aler: Ticker = Ticker::al;
pub const cuer: Ticker = Ticker::cu;
pub const nier: Ticker = Ticker::ni;
pub const sner: Ticker = Ticker::sn;
pub const zner: Ticker = Ticker::zn;
pub const buer: Ticker = Ticker::bu;
pub const eger: Ticker = Ticker::eg;
pub const MAer: Ticker = Ticker::MA;
pub const ler: Ticker = Ticker::l;
pub const pper: Ticker = Ticker::pp;
pub const TAer: Ticker = Ticker::TA;
pub const ver: Ticker = Ticker::v;
pub const ruer: Ticker = Ticker::ru;
pub const eber: Ticker = Ticker::eb;
pub const PFer: Ticker = Ticker::PF;
pub const SAer: Ticker = Ticker::SA;
pub const jmer: Ticker = Ticker::jm;
pub const FGer: Ticker = Ticker::FG;
pub const hcer: Ticker = Ticker::hc;
pub const ier: Ticker = Ticker::i;
pub const jer: Ticker = Ticker::j;
pub const SMer: Ticker = Ticker::SM;
pub const rber: Ticker = Ticker::rb;
pub const SFer: Ticker = Ticker::SF;
pub const ZCer: Ticker = Ticker::ZC;
pub const sser: Ticker = Ticker::ss;
pub const per: Ticker = Ticker::p;
pub const yer: Ticker = Ticker::y;
pub const OIer: Ticker = Ticker::OI;
pub const fuer: Ticker = Ticker::fu;
pub const scer: Ticker = Ticker::sc;
pub const pger: Ticker = Ticker::pg;
pub const auer: Ticker = Ticker::au;
pub const ager: Ticker = Ticker::ag;
pub const mer: Ticker = Ticker::m;
pub const aer: Ticker = Ticker::a;
pub const jder: Ticker = Ticker::jd;
pub const RMer: Ticker = Ticker::RM;
pub const APer: Ticker = Ticker::AP;
pub const SRer: Ticker = Ticker::SR;
pub const sper: Ticker = Ticker::sp;
pub const CFer: Ticker = Ticker::CF;
pub const cer: Ticker = Ticker::c;
pub const cser: Ticker = Ticker::cs;
pub const SHer: Ticker = Ticker::SH;
pub const URer: Ticker = Ticker::UR;

pub trait IntoTicker {
    fn into_ticker(self) -> Option<Ticker>;
}

impl<T> IntoTicker for T
where
    T: AsRef<str>,
{
    fn into_ticker(self) -> Option<Ticker> {
        let res = match self.as_ref() {
            "al" => aler,
            "cu" => cuer,
            "ni" => nier,
            "sn" => sner,
            "zn" => zner,
            "bu" => buer,
            "eg" => eger,
            "MA" => MAer,
            "l" => ler,
            "pp" => pper,
            "TA" => TAer,
            "v" => ver,
            "ru" => ruer,
            "eb" => eber,
            "PF" => PFer,
            "SA" => SAer,
            "jm" => jmer,
            "FG" => FGer,
            "hc" => hcer,
            "i" => ier,
            "j" => jer,
            "SM" => SMer,
            "rb" => rber,
            "SF" => SFer,
            "ZC" => ZCer,
            "ss" => sser,
            "p" => per,
            "y" => yer,
            "OI" => OIer,
            "fu" => fuer,
            "sc" => scer,
            "pg" => pger,
            "au" => auer,
            "ag" => ager,
            "m" => mer,
            "a" => aer,
            "jd" => jder,
            "RM" => RMer,
            "AP" => APer,
            "SR" => SRer,
            "sp" => sper,
            "CF" => CFer,
            "c" => cer,
            "cs" => cser,
            "SH" => SHer,
            "UR" => URer,
            _ => { return None; },
        };
        Some(res)
    }
}


pub const fn convert_ticker_to_str(ticker: Ticker) -> &'static str {
    match ticker {
        aler => "al",
        cuer => "cu",
        nier => "ni",
        sner => "sn",
        zner => "zn",
        buer => "bu",
        eger => "eg",
        MAer => "MA",
        ler => "l",
        pper => "pp",
        TAer => "TA",
        ver => "v",
        ruer => "ru",
        eber => "eb",
        PFer => "PF",
        SAer => "SA",
        jmer => "jm",
        FGer => "FG",
        hcer => "hc",
        ier => "i",
        jer => "j",
        SMer => "SM",
        rber => "rb",
        SFer => "SF",
        ZCer => "ZC",
        sser => "ss",
        per => "p",
        yer => "y",
        OIer => "OI",
        fuer => "fu",
        scer => "sc",
        pger => "pg",
        auer => "au",
        ager => "ag",
        mer => "m",
        aer => "a",
        jder => "jd",
        RMer => "RM",
        APer => "AP",
        SRer => "SR",
        sper => "sp",
        CFer => "CF",
        cer => "c",
        cser => "cs",
        SHer => "SH",
        URer => "UR",
    }
}

impl From<Ticker> for &'static str {
    fn from(value: Ticker) -> Self {
        convert_ticker_to_str(value)
    }
}

impl From<Ticker> for String {
    fn from(value: Ticker) -> Self {
        let ticker_str: &str = value.into();
        ticker_str.to_string()
    }
}

lazy_static! {
    pub static ref tickers_all: Vec<Ticker> = vec![
        buer, eger, MAer, ler, pper, TAer, ver, ruer, eber, PFer, SAer, jmer, FGer, hcer, ier, jer,
        SMer, rber, SFer, ZCer, sser, per, yer, OIer, fuer, scer, pger, auer, ager, mer, aer, jder,
        RMer, APer, SRer, sper, CFer, cer, cser, aler, nier, zner, cuer, sner, aer, SHer, URer,
    ];
}

#[derive(Debug)]
enum Comdty {
    Soft,
    NonferrousMetals,
    Ceral,
    ProteinMeals,
    PreciousMetals,
    Chemicals,
    BlackMaterial,
    Energy,
    Oil,
}

trait ToSection {
    fn to_section(self) -> Comdty;
}
impl ToSection for Ticker {
    fn to_section(self) -> Comdty {
        use Comdty::*;
        use Ticker::*;
        match self {
            AP | SR | sp | CF => Soft,
            al | cu | ni | sn | zn => NonferrousMetals,
            c | cs => Ceral,
            m | a | jd | RM => ProteinMeals,
            au | ag => PreciousMetals,
            bu | eg | MA | l | pp | TA | v | ru | eb | PF | SA => Chemicals,
            jm | FG | hc | i | j | SM | rb | SF | ZC | ss => BlackMaterial,
            fu | sc | pg => Energy,
            p | y | OI => Oil,
            _ => panic!("this ticker not implement section"),
        }
    }
}

#[derive(Debug)]
pub enum TradingPeriod {
    Light,
    LightNight,
    LightNightMorn,
}

impl From<Ticker> for TradingPeriod {
    fn from(value: Ticker) -> Self {
        use Ticker::*;
        use TradingPeriod::*;
        match value {
            AP | ZC | SM | SF => Light,
            al | au | ag | cu | zn | ni | sc | sn => LightNightMorn,
            _ => LightNight,
        }
    }
}


pub trait ExtractTicker {
    type Output;
    fn extract_ticker(self) -> Option<Self::Output>;
}

impl ExtractTicker for &str {
    type Output = (Ticker, i32);
    fn extract_ticker(self) -> Option<Self::Output> {
        let re = Regex::new(r"\d+").ok()?;
        let res = re.find(self)?;
        let ticker = self[0..res.start()].into_ticker()?;
        let contract_i = self[res.start()..res.end()].parse::<i32>().ok()?;
        Some((ticker, contract_i))
    }
}