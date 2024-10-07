#![allow(unused_imports)]
extern crate plotly;
use plotly::common::Mode;
use plotly::{Plot, Scatter};
use plotly::layout::{Axis, BarMode, Layout, Legend, TicksDirection, Margin};
use serde::ser::Serialize;
use crate::types::*;
use crate::aa::CumSum;
use crate::pnl::PnlRes;


pub trait Plt {
    fn plot(&self);
}

impl<T: Clone + Serialize + 'static> Plt for (Vec<T>, Vec<f32>) {
    fn plot(&self) {
        let trace = Scatter::new(self.0.clone(), self.1.to_vec()).name("cumsum").mode(Mode::Lines);
        let mut plot = Plot::new();
        let margin = Margin::new().left(30).right(10).top(3).bottom(15).auto_expand(true);
        plot.add_trace(trace);
        let layout = Layout::new().margin(margin).height(300).width(600).plot_background_color("dark");
        plot.set_layout(layout);
        plot.lab_display();
    }
}

impl Plt for Vec<f32> {
    fn plot(&self) {
        ((0..self.len()).collect(), self.to_vec()).plot()
    }
}

impl Plt for PnlRes<da> {
    fn plot(&self) {
        (self.0.iter().map(|x| x.to_string()).collect(), self.1.cumsum()).plot();
    }
}

impl Plt for PnlRes<dt> {
    fn plot(&self) { self.da().plot() }
}