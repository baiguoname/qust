#![allow(unused_imports)]
use crate::{output::excel::Value, prelude::StatsString};

use super::profile::{Stats, PnlModify};
use qust::prelude::*;
use plotters::{
    coord::{
        ranged1d::{DefaultFormatting, KeyPointHint, AsRangedCoord, ValueFormatter},
        types::RangedCoordf32,
        Shift,
        CoordTranslate, ReverseCoordTranslate,
    },
    style::{RGBColor, RelativeSize},
    evcxr,
    prelude::*, element::{PointCollection, Drawable},
};
use chrono::Datelike;
// use plotters_backend::{DrawingErrorKind, text_anchor::{HPos, Pos, VPos}, BackendColor};
use std::{ops::Range, borrow::Borrow};

const color: RGBColor = WHITE;
const color_bg: RGBColor = RGBColor(40, 40, 40);

mod my_axis {
    use chrono::Timelike;
    use plotters::coord::ranged1d::NoDefaultFormatting;

    use super::*;

    #[derive(Debug, Clone)]
    pub struct MyAxis<'a, T>(pub &'a [T]);
    
    impl<'a, T> Ranged for MyAxis<'a, T>
    where
        T: PartialOrd + Clone,
        Self: GetKeyPoints<ValueType = T>,
    {
        type FormatOption = NoDefaultFormatting;
        type ValueType = T;
    
        fn range(&self) -> Range<Self::ValueType> {
            self.0.first().unwrap().clone()..self.0.last().unwrap().clone()
        }
    
        fn map(&self, value: &Self::ValueType, limit: (i32, i32)) -> i32 {
            let g = &self.0;
            let a = (g.iter().position(|v| v >= value).unwrap_or_default() as f64) / (g.len() as f64);
            limit.0 + ((a * f64::from(limit.1 - limit.0)) as i32)
        }
        fn key_points<Hint: KeyPointHint>(&self, _hint: Hint) -> Vec<Self::ValueType> {
            self.get_key_points()
        }
    }

    trait GetKeyPoints {
        type ValueType;
        fn get_key_points(&self) -> Vec<Self::ValueType>;
    }

    impl GetKeyPoints for MyAxis<'_, da> {
        type ValueType = da;
        fn get_key_points(&self) -> Vec<Self::ValueType> {
            let years_num = self.0.map(|x| x.year()).unique().len();
           let mut res = match years_num {
                1 => self.0.find_first_ele(|x| (x.year(), x.month())),
                _ => self.0.find_first_ele(|x| x.year()),
            };
            res.push(*self.0.last().unwrap());
            res
        }
    }

    impl GetKeyPoints for MyAxis<'_, dt> {
        type ValueType = dt;
        fn get_key_points(&self) -> Vec<Self::ValueType> {
            let dates_num = self.0.map(|x| x.date()).unique().len();
            match dates_num {
                1 => self.0.find_first_ele(|x| x.hour()),
                _ => self.0.find_first_ele(|x| x.date()),
            }
        }
    }
    
    impl ValueFormatter<dt> for MyAxis<'_, dt> {
        fn format(value: &dt) -> String {
            value.to_string()
        }
    
        fn format_ext(&self, value: &dt) -> String {
            let n = (*self.0.last().unwrap() - self.0[0]).num_days();
            if self.0.len() > 10 && n >= 1 {
                value.date().to_string()
            } else {
                value.format("%H:%M:%S").to_string()
            }
        }
    }

    impl ValueFormatter<da> for MyAxis<'_, da> {
        fn format(value: &da) -> String {
            value.to_string()
        }
        fn format_ext(&self, value: &da) -> String {
            if value.year() == self.0.last().unwrap().year() {
                if value == self.0.first().unwrap() {
                    value.format("%Y").to_string()
                } else {
                    value.format("%m%d").to_string()
                }
            } else {
                value.format("%Y").to_string()
            }
        }
    }

    impl<'a, T> DiscreteRanged for MyAxis<'a, T>
    where
        T: PartialOrd + Clone,
        MyAxis<'a, T>: Ranged<ValueType = T>,
    {
        fn size(&self) -> usize {
            self.0.len()
        }
    
        fn index_of(&self, value: &Self::ValueType) -> Option<usize> {
            self.0.iter().position(|x| value >= x)
        }
    
        fn from_index(&self, index: usize) -> Option<Self::ValueType> {
            self.0.get(index).cloned()
        }
    }

    #[derive(Clone)]
    pub struct AxisNumber<'a, T>(pub &'a [T]);

    impl<'a> Ranged for AxisNumber<'a, f32>
    {
        type FormatOption = NoDefaultFormatting;
        type ValueType = f32;

        fn range(&self) -> Range<Self::ValueType> {
            self.0.agg(RollFunc::Min) .. self.0.agg(RollFunc::Max)
        }

        fn map(&self, value: &Self::ValueType, limit: (i32, i32)) -> i32 {
            let r = self.range();
            if r.start == r.end {
                return (limit.1 - limit.0) / 2;
            }
            let logic_length = (*value - r.start) / (r.end - r.start);

            let actual_length = limit.1 - limit.0;

            if actual_length == 0 {
                return limit.1;
            }

            if actual_length > 0 {
                limit.0 + (actual_length as f64 * logic_length as f64 + 1e-3).floor() as i32
            } else {
                limit.0 + (actual_length as f64 * logic_length as f64 - 1e-3).ceil() as i32
            }
        }

        fn key_points<Hint: KeyPointHint>(&self, _hint: Hint) -> Vec<Self::ValueType> {
            let (s, e) = self.range().pip(|x| (x.start, x.end));
            let step = ((e - s) / 5.).max(1.);
            (s as usize .. e as usize)
                .step_by(step as usize)
                .map(|x| x as f32)
                .collect_vec()
        }
    }

    impl ValueFormatter<f32> for AxisNumber<'_, f32> {
        fn format_ext(&self, value: &f32) -> String {
            let v = self.0.agg(RollFunc::Max).abs().max(self.0.agg(RollFunc::Min).abs());
            let (div_num, suffix) = match (v * 10_000.) as usize {
                0..=10                      => (0.0001,     "bp".to_string()),
                11..=10_000_000             => (1.,         "".to_string()),
                10_000_001..=10_000_000_000 => (1000.,      "k".to_string()),
                _                           => (1_000_000., "m".to_string()),
            };
            format!("{:.0}{}", value / div_num, suffix)
        }
    }

    pub fn plot_text<T: std::fmt::Display>(area: &DrawingArea<SVGBackend, Shift>, data: &T, posi: (i32, i32)) {
        let mut multi_text: MultiLineText<(i32, i32), String> = MultiLineText::new(
            posi,
            ("Consolas", RelativeSize::Smaller(0.05), &RGBColor(117, 163, 209)).into_text_style(area),
        );
        data.to_string()
            .lines()
            .for_each(|x| multi_text.push_line(x.to_string()));
        area.draw(&multi_text).unwrap();
    }
}


use my_axis::{ MyAxis, AxisNumber, plot_text };
pub struct PlotWithText<T, N> {
    x: T,
    y: N,
    caption: Option<String>,
    text: Option<String>,
}

trait GetAxis<X, Y> {
    fn get_x_axis(&self) -> MyAxis<X>;
    fn get_y_axis(&self) -> AxisNumber<Y>;
}

impl<T, N, X, Y> GetAxis<X, Y> for PlotWithText<T, N>
where
    T: AsRef<[X]>,
    N: AsRef<[Y]>,
{
    fn get_x_axis(&self) -> MyAxis<X> {
        MyAxis(self.x.as_ref())
    }

    fn get_y_axis(&self) -> AxisNumber<Y> {
        AxisNumber(self.y.as_ref())
    }
}

pub trait BuildChart<X, Y> {
    fn build_chart(&self, area: &DrawingArea<SVGBackend, Shift>);
}

impl<T, N, X, Y> BuildChart<X, Y> for PlotWithText<T, N>
where
    for<'a> MyAxis<'a, X>: Ranged<ValueType = X> + ValueFormatter<X>,
    for<'a> AxisNumber<'a, Y>: Ranged<ValueType = Y> + ValueFormatter<Y>,
    X: Clone + 'static,
    Y: Clone + 'static,
    Self: GetAxis<X, Y>,
{
    fn build_chart(&self, area: &DrawingArea<SVGBackend, Shift>) {
        let mut chart = ChartBuilder::on(area)
            // .caption(&self.caption.clone().unwrap_or_default(), (FontFamily::Name(""), 12, "bold").into_font().with_color(color))
            // .caption(&self.caption.clone().unwrap_or_default(), ("Consolas", RelativeSize::Smaller(0.05), &color).into_text_style(area))
            .margin(5)
            .x_label_area_size(30)
            .y_label_area_size(70)
            .build_cartesian_2d(self.get_x_axis(), self.get_y_axis())
            .unwrap();
        chart
            .draw_series(LineSeries::new(
                self.get_x_axis().0.iter().zip(self.get_y_axis().0.iter()).map(|(x, y)| (x.clone(), y.clone())),
                color,
            ))
            .unwrap();
        chart
            .configure_mesh()
            .x_label_style(&color)
            .y_label_style(&color)
            .disable_x_mesh()
            .disable_y_mesh()
            .axis_style(color)
            .draw()
            .unwrap();
        if let Some(s) = &self.caption {
            plot_text(area, s, (area.dim_in_pixel().0 as i32 / 2, 0));
        }
        if let Some(s) = &self.text {
            plot_text(area, s, (100, 10));
        }
    }
}


impl<T, N, K, J> From<(T, N, K, J)> for PlotWithText<T, N>
where
    Option<String>: From<K>,
    Option<String>: From<J>,
{
    fn from(value: (T, N, K, J)) -> Self {
        PlotWithText { 
            x: value.0,  
            y: value.1, 
            caption: value.2.into(), 
            text: value.3.into() 
        }
    }
}
impl<T, N, K> From<(T, N, K)> for PlotWithText<T, N>
where
    Option<String>: From<K>,
{
    fn from(value: (T, N, K)) -> Self {
        (value.0, value.1, value.2, None).into()
    }
}

impl<T, N> From<(T, N)> for PlotWithText<T, N>
{
    fn from(value: (T, N)) -> Self {
        (value.0, value.1, None, None).into()
    }
}

pub trait BuildCharts<X, Y> {
    fn build_charts(&self, area: &DrawingArea<SVGBackend, Shift>, n: (u32, u32));
}

impl<T: BuildChart<X, Y>, X, Y> BuildCharts<X, Y> for [T] {
    fn build_charts(&self, area: &DrawingArea<SVGBackend, Shift>, n: (u32, u32)) {
        let sub_areas: Vec<DrawingArea<SVGBackend, Shift>> =
            area.split_evenly((n.0 as usize, n.1 as usize));
        izip!(self.iter(), sub_areas.iter()).for_each(|(x, area)| {
            x.build_chart(area);
        });
    }
}
fn split_size(x: u32, y: u32) -> (u32, u32) {
    (x / y + ({ if x % y == 0 { 0 } else { 1 }}), y)
}
fn layout_size(x: u32, y: u32) -> (u32, u32) {
    let single_col_size = match y {
        1 => 600,
        2 => 450,
        3..=5 => 400,
        _ => 280,
    };
    let sum_row_len = (((single_col_size as f32) / 1.8f32) as u32) * x;
    let sum_col_len = single_col_size * y;
    (sum_col_len, sum_row_len)
}

pub trait Plot<T, X, Y> {
    fn plot(&self) -> evcxr::SVGWrapper;
}

impl<T: BuildChart<X, Y>, X, Y> Plot<i32, X, Y> for T {
    fn plot(&self) -> evcxr::SVGWrapper {
        evcxr_figure((600, 300), |root| {
            root.fill(&color_bg)?;
            self.build_chart(&root);
            Ok(())
        })
    }
}

impl Plot<i32, da, f32> for PnlRes<da> {
    fn plot(&self) -> evcxr::SVGWrapper {
        let p: PlotWithText<_, _> = (&self.0, self.1[0].cumsum(), None, self.stats().to_string()).into();
        p.plot()
    }
}

impl Plot<i32, dt, f32> for PnlRes<dt> {
    fn plot(&self) -> evcxr::SVGWrapper {
        let p: PlotWithText<_, _> = (&self.0, self.1[0].cumsum(), None, self.da().stats().to_string()).into();
        p.plot()
    }
}

impl<T> Plot<usize, da, f32> for T
where
    for<'a> PnlRes<da>: From<&'a T>,
    T: 'static,
{
    fn plot(&self) -> evcxr::SVGWrapper {
        <&T as Into<PnlRes<da>>>::into(self).plot()
    }
}

pub trait Aplot<X, Y> {
    fn aplot(&self, col: usize) -> evcxr::SVGWrapper;
}

impl<T, X, Y> Aplot<X, Y> for [T]
where
    [T]: BuildCharts<X, Y>,
{
    fn aplot(&self, cols: usize) -> evcxr::SVGWrapper {
        let grid_size = split_size(self.len() as u32, cols as u32);
        let sum_size = layout_size(grid_size.0, grid_size.1);
        evcxr_figure(sum_size, |root| {
            root.fill(&color_bg)?;
            self.build_charts(&root, grid_size);
            Ok(())
        })
    }
}

impl<T, X> Aplot<X, f32> for Vec<InfoPnlRes<T, X>>
where
    T: std::fmt::Display,
    for<'a> PlotWithText<&'a [X], Vec<f32>>: BuildChart<X, f32> + GetAxis<X, f32>,
    X: Clone + PartialOrd + 'static,
    for<'a> MyAxis<'a, X>: Ranged<ValueType = X> + ValueFormatter<X>,
{
    fn aplot(&self, col: usize) -> evcxr::SVGWrapper {
        self.iter()
            .map(|x| {
                let g: PlotWithText<_, _> = (&x.1.0, x.1.1[0].cumsum(), x.0.to_string(), None).into();
                g
            })
            .collect_vec()
            .aplot(col)
    }
}

impl Aplot<da, f32> for [PnlRes<da>] {
    fn aplot(&self, col: usize) -> evcxr::SVGWrapper {
        self.iter()
            .map(|x| InfoPnlRes(estring, x.clone()))
            .collect_vec()
            .aplot(col)
    }
}

type InfoOutput = (Option<String>, Option<String>);
pub trait PnlWithInfo {
    type Input;
    fn with_info(&self, f: impl Fn(&Self::Input) -> InfoOutput) -> Vec<PlotWithText<&Vec<da>, v32>>;
    fn with_stats(&self) -> Vec<PlotWithText<&Vec<da>, v32>>
    where
        Self::Input: Stats,
    {
        self.with_info(|x| (None, x.stats().to_string().into()))
    }
}

impl<T> PnlWithInfo for [InfoPnlRes<T, da>] {
    type Input = InfoPnlRes<T, da>;
    fn with_info(&self, f: impl Fn(&Self::Input) -> (Option<String>, Option<String>)) -> Vec<PlotWithText<&Vec<da>, v32>>  {
        self.iter()
            .map(|x| {
                let (c1, c2) = f(x);
                PlotWithText {
                    x: &x.1.0,
                    y: x.1.1[0].cumsum(),
                    caption: c1,
                    text: c2,
                }
            })
            .collect_vec()
    }
}
impl PnlWithInfo for [PnlRes<da>] {
    type Input = PnlRes<da>;
    fn with_info(&self, f: impl Fn(&Self::Input) -> (Option<String>, Option<String>)) -> Vec<PlotWithText<&Vec<da>, v32>>  {
        self.iter()
            .map(|x| {
                let (c1, c2) = f(x);
                PlotWithText {
                    x: &x.0,
                    y: x.1[0].cumsum(),
                    caption: c1,
                    text: c2,
                }
            })
            .collect_vec()
    }
}

type Ipr<T, N> = Vec<InfoPnlRes<T, N>>;
lazy_static! {
    pub static ref split_time: Vec<ForCompare<dt>> = vec![
        Between((20150101).to_da().to_dt()..(20170101).to_da().to_dt()),
        Between((20170101).to_da().to_dt()..(20220101).to_da().to_dt()),
        Between((20220101).to_da().to_dt()..(20231010).to_da().to_dt()),
        2023.to_year().after(),
        Between((20150101).to_da().to_dt()..(20230531).to_da().to_dt()),
    ];
    pub static ref y2015: ForCompare<dt> = 2015.to_year().after();
    pub static ref y2018: ForCompare<dt> = 2018.to_year().after();
    pub static ref y2020: ForCompare<dt> = 2020.to_year().after();
    pub static ref y2021: ForCompare<dt> = 2021.to_year().after();
    pub static ref y2022: ForCompare<dt> = 2022.to_year().after();
    pub static ref y2023: ForCompare<dt> = 2023.to_year().after();
    pub static ref y2024: ForCompare<dt> = 2024.to_year().after();
    pub static ref y_da_begin: ForCompare<dt> = 20210601.to_da().after();
    pub static ref y2021_split: Vec<ForCompare<dt>> = vec![2021.to_year().before(), 2021.to_year().after()];
    pub static ref split_ticker: fn(&Stra) -> Ticker = |x: &Stra| -> Ticker { x.ident.ticker };
    pub static ref split_stra: fn(&Stra) -> String = |x: &Stra| -> String { x.name.frame().to_string() };
    pub static ref split_inter: fn(&Stra) -> String = |x: &Stra| -> String { x.ident.inter.debug_string() };
    pub static ref pip_sum_pnl: fn(Vec<PnlRes<da>>) -> Vec<PnlRes<da>> = |x: Vec<PnlRes<da>>| {
        let mut x = x;
        let x_sum = x.sum();
        x.push(x_sum);
        x
    };
    pub static ref pip_sum_info: fn(Ipr<Ticker, da>) -> Ipr<Ticker, da> = |x: Ipr<Ticker, da>| {
        let mut x = x;
        let x_sum = InfoPnlRes(aler, x.sum());
        x.push(x_sum);
        x
    };
    pub static ref info_ticker_with_stats: fn(&InfoPnlRes<Stra, da>) -> InfoOutput =
        |x: &InfoPnlRes<Stra, da>| (x.0.ident.ticker.debug_string().into(), x.1.stats().to_string().into());
    pub static ref info_stra_name_stats: fn(&InfoPnlRes<Stra, da>) -> InfoOutput = 
        |x: &InfoPnlRes<Stra, da>| (x.0.stats_string().into(), x.1.stats().to_string().into());
}


pub trait ShortPlot<'a>: AsRef<DiStral<'a>> {
    fn short_calc1(&self, x1: CommSlip, x2: f32, x3: usize) -> PnlRes<da> {
        self.as_ref()
            .calc(Aee(x1.tuple()))
            .pnl_modify(150, x2)
            .da()
            .sum()
            .get_part(x3.to_year().after())
    }
    fn short_calc2(&self, x3: usize) -> PnlRes<da> {
        self.short_calc1(cs1.clone(), 18_000_000., x3)
    }
    fn short_calc3(&self, n: usize) -> PnlRes<da> {
        self.as_ref()
            .calc(cs1)
            .sum()
            .get_part(n.to_year().after())
    }
    fn short_calc4(&self) -> PnlRes<da> {
        self.short_calc1(cs2.clone(), 18_000_000., 2023)
    }
    fn short_calc5(&self) -> PnlRes<da> {
        self.as_ref().calc(cs2).sum().get_part(y2023.clone())
    }
}
impl<'a, T: AsRef<DiStral<'a>>> ShortPlot<'a> for T {}

pub trait ShortDaPlot<T, N> {
    fn short_da_plot(&self) -> evcxr::SVGWrapper;
}

impl<T, N, K, J> ShortDaPlot<(N, J), u32> for [T]
where
    Self: PnlSumInnerDay<N, Output = Vec<K>>,
    [K]: PnlSum<J, Output = PnlRes<da>>,
{
    fn short_da_plot(&self) -> evcxr::SVGWrapper {
        self.da().sum().plot()
    }
}

impl<T, N> ShortDaPlot<N, u64> for [T]
where
    Self: PnlSum<N, Output = PnlRes<da>>,
{
    fn short_da_plot(&self) -> evcxr::SVGWrapper {
        self.sum().plot()
    }
}