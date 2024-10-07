use crate::prelude::*;
use crate::std_prelude::*;

#[derive(Debug, Clone)]
pub struct SigOri {
    pub t: dt,
    pub ticker: Ticker,
    pub target: NormHold,
    pub price: f32,
}

pub trait IntoStatusVec {
    fn into_status_vec(self) -> Vec<SigOri>;
}

impl IntoStatusVec for (Ticker, Arc<Vec<dt>>, Vec<NormHold>, av32) {
    fn into_status_vec(self) -> Vec<SigOri> {
        izip!(self.1.iter(), self.2, self.3.iter()).fold(vec![], |mut accu, (t, target, price)| {
            let res = SigOri {
                t: *t,
                ticker: self.0,
                target,
                price: *price,
            };
            accu.push(res);
            accu
        })
    }
}

impl IntoStatusVec for Vec<(Ticker, Arc<Vec<dt>>, Vec<NormHold>, av32)> {
    fn into_status_vec(self) -> Vec<SigOri> {
        let mut res: Vec<_> = self.into_iter().flat_map(|x| x.into_status_vec()).collect();
        res.sort_by(|a, b| a.t.cmp(&b.t));
        res
    }
}

impl IntoStatusVec for DiStral<'_> {
    fn into_status_vec(self) -> Vec<SigOri> {
        let mut ptm_res_vec = self.calc(|distra: &DiStra| {
            (
                distra.stra.ident.ticker,
                distra.di.t(),
                distra
                    .di
                    .calc(&distra.stra.ptm)
                    .downcast_ref::<RwLock<PtmRes>>()
                    .unwrap()
                    .read()
                    .unwrap()
                    .0
                    .clone(),
                distra.di.c(),
            )
        });
        ptm_res_vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let grp_ticker = Grp(ptm_res_vec.map(|x| x.0));
        grp_ticker
            .apply(&ptm_res_vec, |x| {
                let ticker = x[0].0;
                let time_union = x.iter().map(|v| v.1.to_vec()).collect_vec().union_vecs();
                let hold_vec = x.iter().fold(
                    (
                        vec![NormHold::No; time_union.len()],
                        vec![None; time_union.len()],
                    ),
                    |mut accu, (_, t, h, p)| {
                        let ri = Reindex::new(t, &time_union);
                        let h_vec = ri.reindex(h);
                        let p_vec = ri.reindex(p);
                        izip!(h_vec.into_iter(), p_vec.into_iter())
                            .enumerate()
                            .for_each(|(i, (h, p))| {
                                if let Some(c) = h {
                                    accu.0[i] = accu.0[i].add_norm_hold(&c);
                                }
                                if let Some(c) = p {
                                    accu.1[i] = Some(c);
                                }
                            });
                        accu
                    },
                );
                (
                    ticker,
                    time_union.pip(Arc::new),
                    hold_vec.0,
                    hold_vec
                        .1
                        .into_iter()
                        .map(|x| x.unwrap())
                        .collect_vec()
                        .pip(Arc::new),
                )
            })
            .1
            .into_status_vec()
    }
}

#[derive(Debug, Clone)]
pub struct Status {
    pub target: NormHold,
    pub hold: NormHold,
    pub price: f32,
}

impl Status {
    fn net_num(&self) -> f32 {
        self.target.to_num().abs() - self.hold.to_num().abs()
    }
}

#[derive(Debug)]
pub struct Order {
    pub open: NormOpen,
    pub exit: NormExit,
}

impl Default for Order {
    fn default() -> Self {
        Order {
            open: NormOpen::No,
            exit: NormExit::No,
        }
    }
}

impl Order {
    fn get_trade_fee(&self, price: f32, info: &TickerInfo) -> (f32, f32, f32) {
        let open_num = self.open.to_num().abs();
        let exit_num = self.exit.to_num().abs();
        let comm = info.comm(price, open_num) + info.comm(price, exit_num);
        let slip = info.slip(open_num) + info.slip(exit_num);
        let money_trade = info.trade_money(open_num + exit_num, price);
        (comm, slip, money_trade)
    }
}

#[derive(Debug)]
pub struct Transaction {
    pub t: dt,
    pub ticker: Ticker,
    pub pnl: f32,
    pub profit: f32,
    pub comm: f32,
    pub slip: f32,
    pub price: f32,
    pub money_hold: f32,
    pub money_trade: f32,
    pub order: Order,
}

#[derive(Debug, Clone, Default)]
pub struct MoneyCut {
    pub upper: f32,
    pub hold: hm<Ticker, Status>,
}

impl MoneyCut {
    pub fn get_hold_money(&self) -> f32 {
        self.hold.iter().fold(0f32, |mut accu, x| {
            accu += x.1.hold.to_num().abs() * x.0.info().pv * x.1.price;
            accu
        })
    }
}

#[derive(Debug)]
pub struct HoldTrans(pub MoneyCut, pub Transaction);

pub trait BackTest {
    fn calc_status(&mut self, data: &SigOri) -> HoldTrans;
    fn calc_pnl(&mut self, data: &[SigOri]) -> PnlRes<dt> {
        let mut t_vec = Vec::with_capacity(data.len());
        let mut s_vec = init_a_matrix(data.len(), 8);
        data.iter().for_each(|x| {
            let hold_trans = self.calc_status(x);
            let money_hold = hold_trans.0.get_hold_money();
            t_vec.push(hold_trans.1.t);
            s_vec[0].push(hold_trans.1.pnl);
            s_vec[1].push(hold_trans.1.profit);
            s_vec[2].push(money_hold);
            s_vec[3].push(hold_trans.1.money_trade);
            s_vec[4].push(hold_trans.1.comm + hold_trans.1.slip);
            s_vec[5].push(hold_trans.1.comm);
            s_vec[6].push(hold_trans.1.slip);
            s_vec[7].push(0f32);
        });
        PnlRes(t_vec, s_vec)
    }
}

impl BackTest for MoneyCut {
    fn calc_status(&mut self, data: &SigOri) -> HoldTrans {
        let money_in = self.get_hold_money();
        let hold = self.hold.entry(data.ticker).or_insert(Status {
            target: NormHold::No,
            hold: NormHold::No,
            price: 1f32,
        });
        hold.target = data.target.clone();
        let left_money = self.upper - money_in;

        let info = data.ticker.info();
        let multi = info.pv * data.price;
        let net_num = hold.net_num();
        let net_money = net_num * multi;
        let profit = hold.hold.to_num() * info.pv * (data.price - hold.price);
        hold.price = data.price;
        let order: Order =
            if (hold.hold == data.target) || (left_money <= 0f32 && net_money >= 0f32) {
                Order::default()
            } else if left_money > 0f32 && net_money > 0f32 && left_money < net_money {
                let sub_money = net_money - left_money;
                let sub_num = sub_money / hold.price / info.pv;
                let hold_adj = match data.target {
                    NormHold::Lo(x) => NormHold::Lo(x - sub_num),
                    NormHold::Sh(x) => NormHold::Sh(x - sub_num),
                    NormHold::No => panic!("what is going wrong?"),
                };
                let (open, exit) = hold_adj.sub_norm_hold(&hold.hold);
                hold.hold = hold_adj;
                Order { open, exit }
            } else {
                let (open, exit) = hold.target.sub_norm_hold(&hold.hold);
                hold.hold = data.target.clone();
                Order { open, exit }
            };
        let (comm, slip, money_trade) = order.get_trade_fee(data.price, &info);
        let slip = slip * 0.3;
        let transaction = Transaction {
            t: data.t,
            ticker: data.ticker,
            pnl: profit - comm - slip,
            profit,
            comm,
            slip,
            price: data.price,
            money_hold: hold.hold.to_num() * multi,
            money_trade,
            order,
        };
        HoldTrans(self.clone(), transaction)
    }
}

#[derive(Debug, Clone)]
pub struct MoneyAdj {
    pub money_cut: MoneyCut,
    pub his_record: (Vec<da>, Vec<f32>),
    pub back_window: usize,
    pub rate: f32,
    pub target_money: f32,
    pub rate_record: v32,
    pub ori_record: MoneyCut,
}

impl MoneyAdj {
    fn update_rate(&mut self, data: &SigOri) {
        self.ori_record.hold.insert(
            data.ticker,
            Status {
                target: NormHold::No,
                hold: data.target.clone(),
                price: data.price,
            },
        );
        let t_da = data.t.date();
        let m = self.ori_record.get_hold_money();
        if self.his_record.0.is_empty() {
            self.his_record.0.push(t_da);
            self.his_record.1.push(m);
            self.rate = 1f32;
        } else if self.his_record.0.last().unwrap() != &t_da {
            self.rate = if self.his_record.1.len() <= 10 {
                1f32
            } else {
                let dom = self.his_record.1.nlast(self.back_window).quantile(0.85);
                if dom <= 10f32 {
                    1f32
                } else {
                    self.target_money / dom
                }
            };
            self.his_record.0.push(t_da);
            self.his_record.1.push(m);
        } else if self.his_record.1.last().unwrap() <= &m {
            let l = self.his_record.1.len();
            self.his_record.1[l - 1] = m;
        }
        self.rate_record.push(self.rate);
    }

    pub fn init(&mut self) {
        self.money_cut.hold.clear();
        self.rate_record.clear();
    }
}

impl BackTest for MoneyAdj {
    fn calc_status(&mut self, data: &SigOri) -> HoldTrans {
        self.update_rate(data);
        let data_new = SigOri {
            target: &data.target * self.rate,
            ..data.clone()
        };
        self.money_cut.calc_status(&data_new)
    }
}

/*
let res_vec = status_vec
    .iter()
    .fold(vec![], |mut accu, x| {
        accu.push(money_adj.calc_status(x));
        accu
});
money_adj.init();
let money_hold_vec = res_vec.map(|x| x.0.get_hold_money());
money_hold_vec.agg(RollFunc::Max).print();
 */
