use serde::Deserialize;
use serde::Serialize;

/* #region Cond Loop1 */
#[ta_derive]
pub struct BollinLoop(pub usize, pub f32);

impl CondLoop for BollinLoop {
    fn cond<'a>(&self, di: &'a Di) -> Box<dyn FnMut(usize) -> f32 + 'a> {
        use std::f32::NAN;
        let length = self.0;
        let gap = self.1;
        let c = di.c();
        let mut top_line = vec![];
        let mut mid_line = vec![];
        let mut bot_line = vec![];
        let mut posi = 0f32;
        let mut _open_i = 0usize;
        Box::new(move |i| {
            if i < length - 1 {
                top_line.push(NAN);
                mid_line.push(NAN);
                bot_line.push(NAN);
                0.
            } else {
                let start_i = i + 1 - length;
                let data_ = &c[start_i .. i + 1];
                let ma = data_.mean();
                let std_ = data_.std() * gap;
                top_line.push(ma + std_);
                mid_line.push(ma);
                bot_line.push(ma - std_);
                if posi == 0f32 {
                    if c[i] > top_line[i] && c[i - 1] < top_line[i - 1] {
                        posi = 1f32;
                        _open_i = i;
                    } else if c[i] < bot_line[i] && c[i - 1] > bot_line[i - 1] {
                        posi = -1f32;
                        _open_i = i;
                    } else if (posi > 0f32 && c[i] < mid_line[i])
                        || (posi < 0f32 && c[i] > mid_line[i])
                    {
                        posi = 0f32;
                    }
                }
                posi
            }
        })
    }
}
/* #endregion */

/* #region Cond Loop2 */
#[ta_derive]
pub struct BollinLoop2(pub BollinTa);

#[ta_derive]
pub struct BollinTa(pub usize, pub f32);

impl BollinTa {
    fn calc_from_price(&self, data: &[f32]) -> vv32 {
        let mid_line = data.roll_mean(self.0);
        let std_ = data
            .roll_std(self.0)
            .into_iter()
            .map(|x| x * self.1)
            .collect::<v32>();
        let top_line = mid_line
            .iter()
            .zip(std_.iter())
            .map(|(&x, &y)| x + y)
            .collect::<v32>();
        let bot_line = mid_line
            .iter()
            .zip(std_.iter())
            .map(|(&x, &y)| x - y)
            .collect::<v32>();
        vec![bot_line, mid_line, top_line]
    }
}

impl CondLoop for BollinLoop2 {
    fn cond<'a>(&self, di: &'a Di) -> Box<dyn FnMut(usize) -> f32 + 'a> {
        let c = di.c();
        let data_s = self.0.calc_from_price(c);
        let mut posi = 0f32;
        let mut _open_i = 0usize;
        Box::new(move |i| {
            if i < 1 {
                0.
            } else {
                if posi == 0f32 {
                    if c[i] > data_s[2][i] && c[i - 1] < data_s[2][i - 1] {
                        posi = 1f32;
                        _open_i = i;
                    } else if c[i] < data_s[0][i] && c[i - 1] > data_s[0][i - 1] {
                        posi = -1f32;
                        _open_i = i;
                    } else if (posi > 0f32 && c[i] < data_s[1][i])
                        || (posi < 0f32 && c[i] > data_s[1][i])
                    {
                        posi = 0f32;
                    }
                }
                posi
            }
        })
    }
}
/* #endregion */

/* #region Cond1 */
#[ta_derive]
pub struct BollinCondOpenLong(pub BollinTa);
#[ta_derive]
pub struct BollinCondExitShort(pub BollinTa);
#[ta_derive]
pub struct BollinCondOpenShort(pub BollinTa);
#[ta_derive]
pub struct BollinCondExitLong(pub BollinTa);

impl Cond for BollinCondOpenLong {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = self.0.calc_from_price(c);
        Box::new(move |e, _o| e >= 1 && c[e] > data_s[2][e] && c[e - 1] < data_s[2][e - 1])
    }
}

impl Cond for BollinCondExitShort {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = self.0.calc_from_price(c);
        Box::new(move |e, _o| c[e] < data_s[1][e])
    }
}

impl Cond for BollinCondOpenShort {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = self.0.calc_from_price(c);
        Box::new(move |e, _o| e >= 1 && c[e] < data_s[0][e] && c[e - 1] > data_s[0][e - 1])
    }
}

impl Cond for BollinCondExitLong {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = self.0.calc_from_price(c);
        Box::new(move |e, _o| c[e] > data_s[1][e])
    }
}

/* #endregion */

/* #region Cond1 */



#[ta_derive]
pub struct BollinCondOpenLong2(pub Pms<BollinTa>);
#[ta_derive]
pub struct BollinCondExitShort2(pub Pms<BollinTa>);
#[ta_derive]
pub struct BollinCondOpenShort2(pub Pms<BollinTa>);
#[ta_derive]
pub struct BollinCondExitLong2(pub Pms<BollinTa>);

impl Ta for BollinTa {
    fn calc_di<'a>(&self, di: &'a mut Di) -> VVa<'a> {
        vec![di.c()]
    }
    fn calc_da(&self, da: Vec<&[f32]>) -> vv32 {
        self.calc_from_price(da[0])
    }
}

impl Cond for BollinCondOpenLong2 {
    fn calc_init(&self, di: &mut Di) {
        di.calc_init(&self.0);
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = di.calc_save(&self.0);
        Box::new(move |e, _o| e >= 1 && c[e] > data_s[2][e] && c[e - 1] < data_s[2][e - 1])
    }
}

impl Cond for BollinCondExitShort2 {
    fn calc_init(&self, di: &mut Di) {
        di.calc_init(&self.0);
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = di.calc_save(&self.0);
        Box::new(move |e, _o| c[e] < data_s[1][e])
    }
}

impl Cond for BollinCondOpenShort2 {
    fn calc_init(&self, di: &mut Di) {
        di.calc_init(&self.0);
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = di.calc_save(&self.0);
        Box::new(move |e, _o| e >= 1 && c[e] < data_s[0][e] && c[e - 1] > data_s[0][e - 1])
    }
}

impl Cond for BollinCondExitLong2 {
    fn calc_init(&self, di: &mut Di) {
        di.calc_init(&self.0);
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        let data_s = di.calc_save(&self.0);
        Box::new(move |e, _o| c[e] > data_s[1][e])
    }
}

/* #endregion */

