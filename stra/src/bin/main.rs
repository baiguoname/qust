#![allow(unused_imports, unused_variables, non_camel_case_types)]
use chrono::{Timelike, Duration};
use qust::prelude::*;
use qust::std_prelude::*;
use stra::prelude::*;
use qust_ds::prelude::*;
use qust_io::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use indicatif::{ProgressBar, ProgressStyle};



fn main() {

    ThreadPoolBuilder::new()
        .num_threads(15)
        .build_global()
        .unwrap();
    let gen_di = GenDi("/root/qust/data");
    let price_tick = gen_di.get_tick_data_hm(
        tickers3.clone(), 
        20240101
        // 20240501
            .to_da().after().clone()
        );
    // let cond_vec = p02::get_cond_vec2();
    let cond_vec = Vec::<p02::cond_vec>::rof("gstra3", "/root/qust/notebook/git_test");
    let num_tasks = cond_vec.len();
    let progress_bar = ProgressBar::new(num_tasks as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );
    
    let res: Vec<(p02::cond_vec, StatsRes)> = cond_vec
        .par_iter()
        .filter_map(|x| {
            let res = p02::backtestwrapper(x.clone(), &price_tick);
            progress_bar.inc(1);
            res.map(|y| (x.clone(), y))
        })
        .collect();
    
    res.sof("res", "/root/qust/notebook/git_test");

}