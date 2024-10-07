#![allow(dead_code, warnings)]

// use bt::ds::prelude::*;
use bt::{ ds::prelude::*, prelude::{PnlRes, InfoPnlRes, Stra, PnlSumInnerDay, PnlSum, GroupbyPnl, Ticker, last_day, GetPart} };
use stra::cstras::prelude::Serialize;
use crate::prelude::{Stats, split_ticker, split_stra};
use plotly::{
    common::{
        Anchor, ColorScalePalette, Visible, Line,
        Fill, Font, Mode, TextAnchor, TextPosition, Position
    },
    layout::{
        update_menu::{
            ButtonBuilder, UpdateMenu, UpdateMenuDirection, UpdateMenuType, Button
        },
        BarMode, ColorAxis,
        Axis, GridPattern, LayoutGrid, Margin, Shape, ShapeLayer, ShapeLine,
        ShapeType, UniformTextMode, UniformText,
    },
    Bar, 
    HeatMap, 
    Layout, 
    Plot, 
    color::{ NamedColor, Rgb, Rgba },
    Scatter,
};
use itertools_num::linspace;

#[derive(AsRef)]
struct PlotUtile;
impl PlotUtile {
    fn get_axis(show_grid: f64) -> Axis {
        Axis::new()
            .show_grid(true)
            .grid_color(Rgba::new(180, 180, 180, show_grid))
            .color(Rgba::new(180, 180, 180, 1.))
            .show_line(true)
            .zero_line(true)
            .zero_line_color(Rgba::new(180, 180, 180, 0.2))
    }

    fn get_shape(x0: f32, x1: f32, y0: f32, y1: f32) -> Shape {
        Shape::new()
            .layer(ShapeLayer::Below)
            .x_ref("x")
            .y_ref("y")
            .shape_type(ShapeType::Rect)
            .x0(x0)
            .y0(y0)
            .x1(x1)
            .y1(y1)
            .line(ShapeLine::new().color(NamedColor::LightSeaGreen).width(3.))
            .fill_color(NamedColor::AntiqueWhite)
    }

    fn change_layout(layout: Layout) -> Layout {
        let margin = Margin::new().left(40).right(10).bottom(20);
        layout
            .plot_background_color(NamedColor::Transparent)
            .paper_background_color(NamedColor::Transparent)
            // .plot_background_color(NamedColor::Black)
            // .paper_background_color(NamedColor::Black)
            .margin(margin)
            .x_axis(PlotUtile::get_axis(0.2))
            .y_axis(PlotUtile::get_axis(0.))
            .show_legend(false)
    }
    
    fn get_layout() -> Layout {
        Self::change_layout(Layout::new())
    }

    fn get_bar_trace<'a, 'b, F, T>(data: &'b Vec<InfoPnlRes<Stra, da>>, f: F, axis: i32) -> Box<Bar<String, f32>>
    where
        Vec<InfoPnlRes<Stra, da>>: GroupbyPnl<'a, F, Output1 = T, Output2 = da>,
        T: std::fmt::Display,
        F: Fn(&'a Stra) -> T,
        'b: 'a,
    {
        let grp_res = data.groupby(f, |x: &[&PnlRes<da>]| x.sum());
        let (bar_trace_x, bar_trace_y) = grp_res
            .iter()
            .fold((vec![], vec![]), |mut accu, x| {
                accu.0.push(x.0.to_string());
                accu.1.push(*x.1.1[0].last().unwrap());
                accu
            });
        Bar::new(bar_trace_x, bar_trace_y)
            .x_axis(format!("x{}", axis))
            .y_axis(format!("y{}", axis))
            .visible(Visible::False)
    }
}

pub trait Ply {
    fn build_plot(&self) -> Plot;
    fn build_plot_size(&self, height:usize, width: usize) -> Plot {
        let mut plot = self.build_plot();
        let layout = plot
            .layout()
            .clone()
            .height(height)
            .width(width);
        plot.set_layout(layout);
        plot
    }
    fn ply(&self) {
        self.build_plot().evcxr_display();
    }
    fn ply_size(&self, height: usize, width: usize) {
        self.build_plot_size(height, width).evcxr_display()
    }
}

impl<T, N> Ply for (Vec<T>, Vec<N>)
where
    T: Serialize + Clone + 'static,
    N: Serialize + Clone + 'static,
{
    fn build_plot(&self) -> Plot {
        let trace = Scatter::new(self.0.clone(), self.1.clone())
            .line(Line::new().color(NamedColor::WhiteSmoke).width(1.0));
        let mut plot = plotly::Plot::new();
        plot.add_trace(trace);
        plot.set_layout(PlotUtile::get_layout());
        plot
    }
}

impl<T> Ply for [T]
where
    T: Serialize + Clone + 'static,
{
    fn build_plot(&self) -> Plot {
        ((0..self.len()).collect_vec(), self.to_vec()).build_plot()
    }
}

impl Ply for PnlRes<da> {
    fn build_plot(&self) -> Plot {
        let (x, y) = (self.0.clone(), self.1[0].cumsum());
        let x_position = x[(x.len() as f32 * 0.15) as usize];
        let y_position = y[(y.len() as f32 * 0.95) as usize];
        let mut plot = (x, y).build_plot();
        plot.add_string(x_position, y_position, self.stats());
        plot
    }
}

impl Ply for PnlRes<dt> {
    fn build_plot(&self) -> Plot {
        (self.0.clone(), self.1[0].cumsum()).build_plot()
    }
}

pub trait AddElement {
    fn add_string<T, N, K>(&mut self, x: T, y: N, s: K)
    where
        K: std::fmt::Debug,
        T: Serialize + Clone + 'static,
        N: Serialize + Clone + 'static,
    ;
}

impl AddElement for Plot {
    fn add_string<T, N, K>(&mut self, x: T, y: N, s: K)
    where
        K: std::fmt::Debug,
        T: stra::cstras::prelude::Serialize + Clone + 'static,
        N: stra::cstras::prelude::Serialize + Clone + 'static,
    {
        let out = s.debug_string().replace('\n', "<br>");
        let trace = Scatter::new(vec![x], vec![y])
            .text(out)
            .text_position(Position::BottomLeft)
            .text_font(Font::new().family("Consolas").color(Rgb::new(117, 163, 209)))
            .clip_on_axis(true)
            .mode(Mode::Text);
        self.add_trace(trace);
    }
}


pub struct OrderFlowSinglePlot(pub vv32);

impl OrderFlowSinglePlot {
    fn plot(&self, on: (f32, f32, f32), color_range: (f32, f32)) -> (Box<Scatter<f32, f32>>, Vec<Shape>) {
        use super::color::*;
        let (x0, x1, x2) = on;
        let mut g = self.0[0].clone();
        let mut shape_vec = vec![];
        g.push(g.last().unwrap() + 1.);
        let color_select = ColorSelect::new(color_range.0, color_range.1, ColorVec::Alphabet);
        let mut text_point_x = vec![];
        let mut text_point_y = vec![];
        let mut text_content = vec![];
        for (y, s, b) in izip!(g.windows(2), self.0[1].iter(), self.0[2].iter()) {
            let (y0, y1) = (y[0], y[1]);
            let shape1 = PlotUtile::get_shape(x0, x1, y0, y1)
                .fill_color(color_select.get(-s).to_string())
                .line(ShapeLine::new().width(0.));
            let shape2 = PlotUtile::get_shape(x1, x2, y0, y1)
                .fill_color(color_select.get(*b).to_string())
                .line(ShapeLine::new().width(0.));
            shape_vec.push(shape1);
            shape_vec.push(shape2);

            text_point_x.push(x0);
            text_point_y.push(y0);

            text_point_x.push(x1);            
            text_point_y.push(y0);

            text_content.push((*s as i32).to_string());
            text_content.push((*b as i32).to_string());
        };
        let trace_text = Scatter::new(text_point_x, text_point_y)
            .text_array(text_content)
            .text_position(Position::TopRight)
            // .text_font(Font::new().)
            .mode(Mode::Text);
        (trace_text, shape_vec)
    }
}

pub struct OrderFlowVecPlot(pub Vec<OrderFlowSinglePlot>);


impl Ply for OrderFlowVecPlot {
    fn build_plot(&self) -> Plot {
        let mut plot = Plot::new();
        let mut layout = Layout::new();
        for (i, of) in self.0.iter().enumerate() {
            let i = i as f32;
            let (trace_text, shape_vec) = of.plot((i - 0.3, i, i + 0.3), (-200., 200.));
            plot.add_trace(trace_text);
            shape_vec
                .into_iter()
                .for_each(|x| {
                    layout.add_shape(x);
                })
        }
        plot.set_layout(PlotUtile::change_layout(layout));
        plot
    }
}


pub struct StraPnlVec(pub Vec<InfoPnlRes<Stra, dt>>);

impl Ply for StraPnlVec {
    fn build_plot(&self) -> Plot {
        type ScatterType = Scatter<da, f32>;
        type BarType = Bar<Ticker, f32>;
        use Visible::*;
        let pnl_da = self.0.da();
        let mut plot = pnl_da.sum().build_plot();
        let bar_trace_ticker = PlotUtile::get_bar_trace(&pnl_da, split_ticker.clone(), 2);
        let bar_trace_stra   = PlotUtile::get_bar_trace(&pnl_da, split_stra.clone(), 3);
        let pnl_last_day     = self.0.iter().map(|x| x.1.get_part(last_day)).collect_vec().sum();
        let pnl_last_day     = Scatter::new(
            pnl_last_day.0.clone(), 
            pnl_last_day.1[0].cumsum(),
        )
            .x_axis("x4")
            .y_axis("y4")
            .visible(False);
        plot.add_trace(bar_trace_ticker);
        plot.add_trace(bar_trace_stra);
        plot.add_trace(pnl_last_day);
        let buttons = vec![
            ButtonBuilder::new()
                .label("Pnl Daily")
                .push_restyle(ScatterType::modify_visible(vec![True, True, False, False, False]))
                .push_relayout(Layout::modify_x_axis(PlotUtile::get_axis(0.2).visible(true)))
                .push_relayout(Layout::modify_y_axis(PlotUtile::get_axis(0.).visible(true)))
                .push_relayout(Layout::modify_x_axis2(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis2(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis3(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis3(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis4(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis4(PlotUtile::get_axis(0.).visible(false)))
                .build(),
            ButtonBuilder::new()
                .label("Last Pnl Ticker")
                .push_restyle(ScatterType::modify_visible(vec![False, False, True, False, False]))
                .push_relayout(Layout::modify_x_axis(PlotUtile::get_axis(0.2).visible(false)))
                .push_relayout(Layout::modify_y_axis(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis2(PlotUtile::get_axis(0.).visible(true)))
                .push_relayout(Layout::modify_y_axis2(PlotUtile::get_axis(0.).visible(true)))
                .push_relayout(Layout::modify_x_axis3(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis3(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis4(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis4(PlotUtile::get_axis(0.).visible(false)))
                .build(),
            ButtonBuilder::new()
                .label("Last Pnl Stra")
                .push_restyle(ScatterType::modify_visible(vec![False, False, False, True, False]))
                .push_relayout(Layout::modify_x_axis(PlotUtile::get_axis(1.).visible(false)))
                .push_relayout(Layout::modify_y_axis(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis2(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis2(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis3(PlotUtile::get_axis(0.).visible(true)))
                .push_relayout(Layout::modify_y_axis3(PlotUtile::get_axis(0.).visible(true)))
                .push_relayout(Layout::modify_x_axis4(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis4(PlotUtile::get_axis(0.).visible(false)))
                .build(),
            ButtonBuilder::new()
                .label("Last Inner Day")
                .push_restyle(ScatterType::modify_visible(vec![False, False, False, False, True]))
                .push_relayout(Layout::modify_x_axis(PlotUtile::get_axis(1.).visible(false)))
                .push_relayout(Layout::modify_y_axis(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis2(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis2(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis3(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_y_axis3(PlotUtile::get_axis(0.).visible(false)))
                .push_relayout(Layout::modify_x_axis4(PlotUtile::get_axis(0.).visible(true)))
                .push_relayout(Layout::modify_y_axis4(PlotUtile::get_axis(0.).visible(true)))
                .build(),    
        ];
        let layout =  Layout::new()
            .update_menus(vec![
                UpdateMenu::new()
                .x(0.2)
                .background_color(NamedColor::WhiteSmoke)
                .buttons(buttons)
                .border_width(0)
                .font(Font::new().color("black"))
                ]);
        let layout = PlotUtile::change_layout(layout)
            .x_axis2(PlotUtile::get_axis(0.).visible(false))
            .y_axis2(PlotUtile::get_axis(0.).visible(false))
            .x_axis3(PlotUtile::get_axis(0.).visible(false))
            .y_axis3(PlotUtile::get_axis(0.).visible(false))
            .x_axis4(PlotUtile::get_axis(0.).visible(false))
            .y_axis4(PlotUtile::get_axis(0.).visible(false))
            ;
        plot.set_layout(layout);
        plot
    }
}

use plotly::traces::Table;
use plotly::traces::table::{Cells, Header, Fill as FillColor};


pub struct BarWrapper<T>(pub T);

impl<T, N> Ply for BarWrapper<(Vec<T>, Vec<N>)>
where
    T: Serialize + Clone + 'static,
    N: Serialize + Clone + 'static,
{
    fn build_plot(&self) -> Plot {
        let mut plot = Plot::new();
        let trace = Bar::new(self.0.0.clone(), self.0.1.clone());
        plot.add_trace(trace);
        plot.set_layout(PlotUtile::get_layout());
        plot
    }
}

impl Ply for BarWrapper<Vec<f32>> {
    fn build_plot(&self) -> Plot {
        BarWrapper(((0..self.0.len()).collect_vec(), self.0.clone()))
            .build_plot()
    }
}

impl<T> Ply for BarWrapper<Vec<InfoPnlRes<T, da>>>
where
    T: std::fmt::Display,
{
    fn build_plot(&self) -> Plot {
        self.0
            .iter()
            .fold((vec![], vec![]), |mut accu, x| {
                accu.0.push(x.0.to_string());
                accu.1.push(*x.1.1[0].last().unwrap());
                accu
            })
            .pip(BarWrapper)
            .build_plot()
    }
}

