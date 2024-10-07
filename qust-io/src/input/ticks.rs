use qust::{
    prelude::{ori, Event},
    trade::prelude::*,
};
use chrono::Duration;
use qust_ds::prelude::*;
use itertools::Itertools;
use std::{path::{Path, PathBuf},  thread};

pub struct GenDi(pub &'static str);

pub fn otimes<T: Clone, N: Clone>(x: &[T], y: &[N]) -> Vec<(T, N)> {
    let cc: Vec<Vec<(T, N)>> = x
        .iter()
        .map(|t| y.iter().map(|n| (t.clone(), n.clone())).collect())
        .collect();
    cc.concat()
}

impl GenDi {
    pub fn get_tick<T: Fromt<da> + PartialOrd>(
        &self,
        ticker: Ticker,
        range: ForCompare<T>,
    ) -> Option<PriceTick> {
        let p_str = self.0.to_owned() + "/Rtick/" + &ticker.to_string();
        let mut file_vec = p_str.get_file_vec().ok()?;
        file_vec.sort();
        let price_vec = file_vec
            .iter()
            .filter(|x| range.compare_time(&x.to_da()))
            .collect_vec();
        let es_len = 25_000 * price_vec.len();
        let mut res =
            price_vec
                .into_iter()
                .fold(PriceTick::with_capacity(es_len), |mut accu, item| {
                    let mut price_tick = rof::<PriceTick>(item, &p_str);
                    accu.cat(&mut price_tick);
                    accu
                });
        res.shrink_to_fit();
        res.into()
    }

    pub fn getl_tick<T: Fromt<da> + PartialOrd + Clone>(
        &self,
        ticker: &[Ticker],
        range: ForCompare<T>,
    ) -> Vec<PriceTick> {
        ticker
            .iter()
            .map(|x| self.get_tick(*x, range.clone()).unwrap())
            .collect_vec()
    }

    pub fn get_tick_data_vec<T: Fromt<da> + PartialOrd>(
        &self,
        ticker: Ticker,
        range: ForCompare<T>,
    ) -> Option<Vec<TickData>> {
        let price_tick = self.get_tick(ticker, range)?;
        Some(price_tick.to_tick_data())
    }

    pub fn get_tick_data_hm<T: Fromt<da> + PartialOrd + Clone + Send>(
        &self,
        tickers: Vec<Ticker>,
        range: ForCompare<T>,
    ) -> hm<Ticker, Vec<TickData>> {
        thread::scope(|scope| {
            let mut handles = vec![];
            for ticker in tickers {
                let range = range.clone();
                let handle = scope.spawn(move || {
                    (ticker, self.get_tick_data_vec(ticker, range))
                });
                handles.push(handle);
            }
            handles
                .into_iter()
                .fold(hm::new(), |mut accu, x| {
                    let (ticker, res_opt) = x.join().unwrap();
                    if let Some(res) = res_opt {
                        accu.insert(ticker, res);
                    }
                    accu
                })
        })
    }

    pub fn sof(&self, dil: &Dil) {
        dil.dil.iter().for_each(|di| {
            let path_str = self.0.to_owned() + "/" + &di.pcon.inter.debug_string();
            let path_dir = Path::new(&path_str);
            if !path_dir.is_dir() {
                std::fs::create_dir(path_dir).unwrap();
            }
            di.pcon.sof(&di.pcon.ticker.to_string(), &path_str)
        });
    }

    pub fn sof_tick_data(&self, price: &PriceTick, ticker: Ticker, date: da) {
        let save_path = self.0.to_owned() + "/Rtick/" + &ticker.to_string();
        price.sof(&date.to_string(), &save_path);
    }

    pub fn update_dil(&self, dil: &mut Dil) {
        dil.dil.iter_mut().for_each(|x| {
            let max_time = x.pcon.price.t.last().unwrap().date();
            let tick_data = self
                .get_tick(x.pcon.ticker, (max_time + Duration::days(1)).after())
                .unwrap();
            let mut price_data = tick_data.to_price_ori(x.pcon.inter.clone(), x.pcon.ticker);
            x.pcon.price.cat(&mut price_data);
        });
    }

    pub fn update_dil_file(&self, name: &str, path: &str) {
        let mut dil = rof::<Dil>(name, path);
        self.update_dil(&mut dil);
        dil.sof(name, path);
    }

    pub fn update_from_mongodb(&self) {
        todo!();
    }

    pub fn get_ticks_saved_date(&self, ticker: Ticker) -> Vec<da> {
        let mut res = (self.0.to_owned() + "/Rtick/" + &ticker.to_string())
            .get_file_vec()
            .unwrap()
            .iter()
            .map(|x| x.to_da())
            .collect_vec();
        res.sort();
        res
    }

    pub fn get_pcon(&self, x: &(TriBox, Ticker)) -> Option<Pcon> {
        let path = format!("{}/{:?}/{}", self.0, x.0, x.1);
        let path_file = Path::new(&path);
        if path_file.exists() {
            Some(<Pcon as Sof>::rof(
                path_file.file_name().unwrap().to_str().unwrap(),
                path_file.parent().unwrap().to_str().unwrap(),
            ))
        } else {
            None
        }
    }

    pub fn get<T: ToIdentVec>(&self, x: T) -> Dil {
        let pcon_ident_vec = x.to_ident_vec(self.0);
        let dil_vec = pcon_ident_vec
            .iter()
            .filter_map(|x| {
                let pcon = self.get_pcon(x);
                pcon.map(|x| x.to_di())
            })
            .collect();
        Dil { dil: dil_vec }
    }

    pub fn get_di_from_pcon_ident<T: Fromt<da> + PartialOrd + Clone>(
        &self,
        pcon_ident: &PconIdent,
        range: ForCompare<T>,
    ) -> Di {
        let di_tick = self.get_tick(pcon_ident.ticker, range).unwrap();
        di_tick
            .to_price_ori(pcon_ident.inter.clone(), pcon_ident.ticker)
            .to_pcon(pcon_ident.inter.clone(), pcon_ident.ticker)
            .to_di()
    }

    /// must be someting like (have tickers)
    /// code```
    /// (vec![rl5m.clone()], tickers_all)
    /// ```
    pub fn gen<T: ToIdentVec, N: Fromt<da> + PartialOrd + Clone>(
        &self,
        x: T,
        range: ForCompare<N>,
    ) -> Dil {
        let mut pcon_ident_vec = x.to_ident_vec(self.0);
        pcon_ident_vec.sort_by(|x, y| x.1.to_string().cmp(&y.1.to_string()));
        let grp = Grp(pcon_ident_vec.iter().map(|x| x.1).collect_vec());
        let pcon_hm = grp.apply(
            &pcon_ident_vec.iter().map(|x| x.0.clone()).collect_vec(),
            |x| x.to_vec(),
        );
        izip!(pcon_hm.0.into_iter(), pcon_hm.1.into_iter()).fold(
            Dil { dil: vec![] },
            |mut accu, (ticker, inters)| match self.get_tick(ticker, range.clone()) {
                Some(tick_data) => {
                    inters.iter().for_each(|inter| {
                        let di = tick_data
                            .to_price_ori(inter.clone(), ticker)
                            .to_pcon(inter.clone(), ticker)
                            .to_di();
                        accu.dil.push(di);
                    });
                    accu
                }
                None => accu,
            },
        )
    }

    pub fn delete_ticks(&self, range: ForCompare<dt>) {
        let p = PathBuf::from(&self.0).join("Rtick");
        p.get_file_vec().unwrap().iter().for_each(|x| {
            let pp = p.join(x);
            pp
                .get_file_vec()
                .unwrap()
                .iter()
                .for_each(|a| {
                    let date = a.to_da();
                    if range.compare_time(&date) {
                        pp.join(a).remove();
                    }
                })
        });
    }
}


pub trait ToIdentVec {
    fn to_ident_vec(&self, path: &str) -> Vec<(TriBox, Ticker)>;
}

impl ToIdentVec for TriBox {
    fn to_ident_vec(&self, path: &str) -> Vec<(TriBox, Ticker)> {
        format!("{}/{:?}", path, self)
            .get_file_vec()
            .unwrap_or_default()
            .iter()
            // .filter_map(|x| )
            .map(|x| (self.clone(), x.into_ticker().unwrap()))
            .collect_vec()
    }
}

impl ToIdentVec for Vec<TriBox> {
    fn to_ident_vec(&self, path: &str) -> Vec<(TriBox, Ticker)> {
        self.iter().fold(vec![], |mut accu, x| {
            let mut res_part = x.to_ident_vec(path);
            accu.append(&mut res_part);
            accu
        })
    }
}

impl ToIdentVec for (TriBox, Ticker) {
    fn to_ident_vec(&self, _path: &str) -> Vec<(TriBox, Ticker)> {
        vec![self.clone()]
    }
}

impl ToIdentVec for (Vec<TriBox>, Vec<Ticker>) {
    fn to_ident_vec(&self, _path: &str) -> Vec<(TriBox, Ticker)> {
        self.clone().product(|(x, k)| (x, k))
    }
}

impl ToIdentVec for (TriBox, Vec<Ticker>) {
    fn to_ident_vec(&self, path: &str) -> Vec<(TriBox, Ticker)> {
        (vec![self.0.clone()], self.1.clone()).to_ident_vec(path)
    }
}

pub trait DiToDi: AsRef<Di> + Sized {
    fn di_to_di<T: Pri + Clone>(self, pri: T) -> Di {
        self.as_ref()
            .calc(ori + Event(pri.pri_box()))
            .to_price_ori()
            .to_pcon(self.as_ref().pcon.inter.clone(), self.as_ref().pcon.ticker)
            .to_di()
    }
}
impl DiToDi for Di {}