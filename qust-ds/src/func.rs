
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug, fs::{self, DirEntry, ReadDir}, hash::Hash, io::{self, Write}, path::Path, sync::Mutex, time::Instant
};
use crate::prelude::*;
use chrono::{Local, ParseError, Timelike};
/* #region dt */
pub trait Fromt<T> {
    fn fromt(data: &T) -> Self;
}

impl Fromt<dt> for dt {
    fn fromt(data: &dt) -> Self {
        *data
    }
}
impl Fromt<da> for dt {
    fn fromt(data: &da) -> Self {
        data.and_hms_opt(0, 0, 0).unwrap()
    }
}
impl Fromt<Year> for dt {
    fn fromt(data: &Year) -> Self {
        dt::fromt(&da::fromt(data))
    }
}
impl Fromt<&str> for dt {
    fn fromt(data: &&str) -> Self {
        dt::parse_from_str(data, "%Y-%m-%d %H:%M:%S%.f").unwrap()
    }
}

pub trait ToDt {
    fn to_dt(&self) -> dt;
}

impl<T> ToDt for T where dt: Fromt<T> {
    fn to_dt(&self) -> dt {
        dt::fromt(self)
    }
}
/* #endregion */

/* #region da */
impl Fromt<dt> for da {
    fn fromt(data: &dt) -> Self {
        data.date()
    }
}
impl Fromt<da> for da {
    fn fromt(data: &da) -> Self {
        *data
    }
}
impl Fromt<Year> for da {
    fn fromt(data: &Year) -> Self {
        da::from_ymd_opt(data.0 as i32, 1, 1).unwrap()
    }
}
impl Fromt<usize> for da {
    fn fromt(data: &usize) -> Self {
        let year = data / 10000;
        let month_date = data % 10000;
        let month = month_date / 100;
        let date = month_date % 100;
        da::from_ymd_opt(year as i32, month as u32, date as u32).unwrap()
    }
}
impl Fromt<String> for da {
    fn fromt(data: &String) -> Self {
        da::parse_from_str(data, "%Y-%m-%d").unwrap()
    }
}
impl Fromt<&str> for da {
    fn fromt(data: &&str) -> Self {
        let res = da::parse_from_str(data, "%Y-%m-%d");
        res.expect(data)
    }
}
pub trait ToDa {
    fn to_da(&self) -> da;
}

impl<T> ToDa for T where da: Fromt<T> {
    fn to_da(&self) -> da {
        da::fromt(self)
    }
}

/* #endregion */

/* #region tt */
impl Fromt<dt> for tt {
    fn fromt(data: &dt) -> Self {
        tt::from_hms_opt(data.hour(), data.minute(), data.second()).unwrap()
    }
}

impl Fromt<f64> for tt {
    fn fromt(data: &f64) -> Self {
        let usize_part = *data as u32;
        let float_part = ((data % usize_part as f64) * 1000.) as u32;
        let hour = usize_part / 10000;
        let minitue_second = usize_part % 10000;
        let minitue = minitue_second / 100;
        let second = minitue_second % 100;
        tt::from_hms_milli_opt(hour, minitue, second, float_part).unwrap()
    }
}
impl Fromt<usize> for tt {
    fn fromt(data: &usize) -> Self {
        tt::fromt(&(*data as f64))
    }
}
impl Fromt<&str> for tt {
    fn fromt(data: &&str) -> Self {
        tt::parse_from_str(data, "%H:%M:%S").unwrap()
    }
}

pub trait ToTt {
    fn to_tt(&self) -> tt;
}

impl<T> ToTt for T where tt: Fromt<T> {
    fn to_tt(&self) -> tt {
        tt::fromt(self)
    }
}

/* #endregion */

/* #region Year */
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Year(pub usize);

impl Fromt<usize> for Year {
    fn fromt(data: &usize) -> Self {
        Year(*data)
    }
}

pub trait ToYear {
    fn to_year(&self) -> Year;
}

impl<T> ToYear for T where Year: Fromt<T> {
    fn to_year(&self) -> Year {
        Year::fromt(self)
    }
}
/* #endregion */

/* #region ForCompare */
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForCompare<T> {
    After(T),
    Before(T),
    Between(std::ops::Range<T>),
    List(Vec<Box<ForCompare<T>>>),
}
impl<T: std::fmt::Debug> std::fmt::Display for ForCompare<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Debug>::fmt(self, f)
    }
}

impl<T> ForCompare<T> 
where 
    T: PartialOrd,
{
    pub fn compare_time<N>(&self, other: &N) -> bool 
    where 
        T: Fromt<N>, 
    {
        self.compare_same(&T::fromt(other))
    }

    pub fn compare_same(&self, other: &T) -> bool {
        match self {
            ForCompare::After(x) => other >= x,
            ForCompare::Before(x) => other < x,
            ForCompare::Between(x) => x.contains(other),
            ForCompare::List(x) => {
                for i in x.iter() {
                    if i.compare_same(other) { 
                        return true
                    }
                }
                false
            }
        }
    }

    pub fn compare<N>(&self, other: N) -> bool
    where
        T: From<N>,
    {
        self.compare_same(&T::from(other))
    }
}
/* #endregion */

/* #region Convenient select */
pub trait TimeSelect: ToDt {
    fn before(&self) -> ForCompare<dt> {
        ForCompare::Before(self.to_dt())
    }

    fn after(&self) -> ForCompare<dt> {
        ForCompare::After(self.to_dt())
    }

    fn to<T: ToDt>(&self, other: T) -> ForCompare<dt> {
        ForCompare::Between(self.to_dt()..other.to_dt())
    }
}
impl<T: ToDt + Clone> TimeSelect for T {}

pub fn last_days(n: i64) -> ForCompare<dt> {
    let today = Local::now().date_naive();
    let start_date = today - chrono::Duration::days(n);
    start_date.to_da().after()
}
/* #endregion */

pub trait ToDa2 {
    fn to_da2(self) -> Result<da, ParseError>;
}

impl ToDa2 for &str {
    fn to_da2(self) -> Result<da, ParseError> {
        da::parse_from_str(self, "%Y%m%d")
    }
}

/* #region ProgressBar */
pub struct ProgressBar<T> {
    t: Instant,
    total: Vec<T>,
    last_i: Mutex<usize>,
    last_l: Mutex<usize>,
    thre: usize,
    count: usize,
    size: usize,
}

impl<T> ProgressBar<T> {
    pub fn inc(&self) {
        *self.last_i.lock().unwrap() += 1;
        let b = *self.last_i.lock().unwrap();
        let l = *self.last_l.lock().unwrap();
        let t = b.checked_sub(l);
        match t {
            Some(i) => {
                if i >= self.thre || (self.size >= self.thre && b == self.size) {
                    *self.last_l.lock().unwrap() = b;
                    self.print();
                }
            }
            None => {
                println!("b: {b} l: {l}");
            }
        }
    }
    pub fn print(&self) {
        println!("{:?} / {:?}, {:.0?}", self.last_i.lock().unwrap(), self.size, self.t.elapsed());
    }
}

impl<'a, T> Iterator for ProgressBar<&'a T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        let res = if self.count < self.size {
            self.inc();
            Some(self.total[self.count])
        } else {
            None
        };
        self.count += 1;
        res
    }
}

pub trait ToProgressBar {
    type Output<'a> where Self: 'a;
    fn to_progressbar(&self) -> ProgressBar<Self::Output<'_>>;
}

impl<T> ToProgressBar for [T] {
    type Output<'a> = &'a T where Self: 'a;
    fn to_progressbar(&self) -> ProgressBar<Self::Output<'_>> {
        ProgressBar {
            t: Instant::now(),
            size: self.len(),
            total: self.iter().collect_vec(),
            last_i: Mutex::new(0),
            last_l: Mutex::new(0),
            thre: 1000,
            count: 0,
        }
    }
}

impl Iterator for ProgressBar<usize> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        let res = if self.count < self.size {
            self.inc();
            Some(self.total[self.count])
        } else {
            None
        };
        self.count += 1;
        res
    }
}

impl ToProgressBar for usize {
    type Output<'a> = usize;
    fn to_progressbar(&self) -> ProgressBar<Self::Output<'_>> {
        ProgressBar {
            t: Instant::now(),
            size: *self,
            total: (0..*self).collect_vec(),
            last_i: Mutex::new(0),
            last_l: Mutex::new(0),
            thre: 1000,
            count: 0,
        }
    }
}

impl ToProgressBar for (usize, usize) {
    type Output<'a> = usize;
    fn to_progressbar(&self) -> ProgressBar<Self::Output<'_>> {
        ProgressBar {
            t: Instant::now(),
            size: self.0,
            total: (0..self.0).collect_vec(),
            last_i: Mutex::new(0),
            last_l: Mutex::new(0),
            thre: self.1,
            count: 0,
        }
    }
}

impl ToProgressBar for  (usize, f32) {
    type Output<'a> = usize;
    fn to_progressbar(&self) -> ProgressBar<Self::Output<'_>> {
        (self.0, (self.0 as f32 * self.1) as usize).to_progressbar()
    }
}
/* #endregion */

pub trait Pip: Sized {
    fn pip<F: Fn(Self) -> T, T>(self, f: F) -> T {
        f(self)
    }
    fn pip_ref<F: Fn(&Self) -> T, T>(&self, f: F) -> T {
        f(self)
    }
    fn pip_clone<F, T>(&self, f: F) -> T
    where
        Self: Clone,
        F: Fn(Self) -> T,
    {
        self.clone().pip(f)
    }
}
impl<T> Pip for T {}

pub trait BoolToOption: Sized {
    fn bool_to_option<F>(self, f: F) -> Option<Self>
    where
        F: Fn(&Self) -> bool,
    {
        if f(&self) { Some(self) } else { None }
    }
    fn bool_to_option_else<F, N>(self, f: F, other: N) -> Option<N>
    where
        F: Fn(Self) -> bool,
    {
        if f(self) { Some(other) } else { None }
    }
}
impl<T> BoolToOption for T {}


pub trait StrHandle: AsRef<str> {
    fn split_path(&self) -> (&str, &str) {
        let p = Path::new(self.as_ref());
        (
            p.file_name().unwrap().to_str().unwrap(), 
            p.parent().unwrap().to_str().unwrap()
        )
    }
}
impl<T: AsRef<str>> StrHandle for T {}

///```
///     let k = HandleDir { 
///  dir: String::from("D:/Rust/arcta/data"),
///   exclude: vec![String::from("Rtick")],
///   let x_path = x.split_path();
///    let y_path = y.split_path();
///    let pcon = rof::<Pcon>(x_path.0, x_path.1);
///    let price = pcon.price.clone();
///    (
///        pcon.ident(),
///        pcon.price.t,
///        vec![price.o, price.h, price.l, price.c, price.v],
///        price.ct,
///    ).sof(y_path.0, y_path.1);
///}
///};
///k.file_move_change("D:/Rust/arcta/data2");
/// 
/// use std::fs;
///DirHandle { 
///    dir: "..".into(),
///    exclude: vec!["notebook".into(), "target".into(), "axum".into()],
///    f: |x: &str, y: &str| { fs::copy(x, y); },
///}
///    .file_move_change("../axum/crates");
/// 
/// {
///let dir_handle = DirHandle {
///    dir: String::from("/root/arcta/data/Rtick"),
///    exclude: vec![],
///    f: |x: &str, y: &str| {
///        let x_path = x.split_path();
///        let y_path = y.split_path();
///        let price_tick = rof::<PriceTick>(x_path.0, x_path.1);
///        let price_tick2: PriceTickVersion = price_tick.into();
///        price_tick2.sof(y_path.0, y_path.1);
///    }
///};
///dir_handle.file_move_change("/mnt/d/Rtick");
///}
/// ```
pub struct DirHandle<T> {
    pub dir: String,
    pub exclude: Vec<String>,
    pub f: T
}

impl<T> DirHandle<T>
where
    T: Fn(&str, &str) + Clone,
{
    pub fn read_dir(&self) -> ReadDir {
        let p = Path::new(&self.dir);
        fs::read_dir(p).unwrap()
    }

    pub fn file_move_change(&self, target: &str) {
        if !Path::new(target).is_dir() {
            fs::create_dir(target).unwrap_or_else(|_| panic!("not such dir: {:?}", target));
        }
        for entry in self.read_dir() {
            let dir = entry.unwrap();
            let b = dir.path();
            let f_n = b.file_name().unwrap().to_str().unwrap();
            let t_a = format!("{}/{}", target, f_n);
            if self.exclude.contains(&f_n.to_string()) {
                continue
            } else if b.is_dir() {
                let handle_dir_next = DirHandle {
                    dir: b.to_str().unwrap().to_string(),
                    exclude: self.exclude.clone(),
                    f: self.f.clone()
                };
                handle_dir_next.file_move_change(&t_a);
            } else {
                (self.f)(b.to_str().unwrap(), &t_a);
            }
        }
    }
}


pub trait IntoTuple
where
    Self: Sized,
{
    fn tuple(&self) -> (Self,)
    where
        Self: Clone,
    {
        (self.clone(),)
    }
    fn into_tuple<T>(self, other: T) -> (Self, T) {
        (self, other)
    }
}
impl<T: Sized> IntoTuple for T {}

pub trait Print {
    fn print(&self)
    where
        Self: std::fmt::Debug,
    {
        println!("{:?}", self);
    }
    fn println(&self)
    where
        Self: std::fmt::Display,
    {
        println!("{}", self);
    }
    fn print_type(&self)
    where
        Self: Sized,
    {
        type_of(self);
    }
}
impl<T> Print for T {}

pub trait DebugString {
    fn debug_string(&self) -> String;
}
impl<T: std::fmt::Debug> DebugString for T {
    fn debug_string(&self) -> String {
        format!("{:?}", self)
    }
}


pub fn copy_dir_for_axum() {
    DirHandle { 
        dir: "/root/arcta".into(),
        exclude: vec![
            "notebook".into(), "target".into(), "axum".into(),
             "OPT2".into(), "data".into()],
        f: |x: &str, y: &str| { fs::copy(x, y).unwrap(); },
    }
        .file_move_change("/root/arcta/axum/crates")
}

pub trait FileStr: AsRef<Path> {
    fn clear_dir(&self) {
        let path = self.as_ref();
        if !path.is_dir() { return }
        self
            .get_file_vec()
            .unwrap_or_default()
            .iter()
            .for_each(|x| {
                path.join(x).remove();
            })
    }

    fn remove(&self) {
        let path = Path::new(self.as_ref());
        if path.is_dir() {
            self.clear_dir();
            std::fs::remove_dir(self).unwrap();
        } else if path.is_file() {
            std::fs::remove_file(self).unwrap();
        }
    }

    fn build_an_empty_dir(&self) {
        let path = Path::new(self.as_ref());
        if path.is_dir() {
            self.clear_dir();
        } else {
            if path.exists() && path.is_file() {
                std::fs::remove_file(path).unwrap();
            }
            std::fs::create_dir(path).unwrap();
        }
    }

    fn get_file_vec(&self) -> io::Result<Vec<String>> {
        self
            .as_ref()
            .read_dir()?
            .map(|x| {
                let path_str = x.unwrap().path();
                path_str.file_name_str().to_string()
            })
            .collect_vec()
            .pip(Ok)
    }

    fn get_file_vec_sort(&self) -> io::Result<Vec<String>> {
        let mut res = self.get_file_vec()?;
        res.sort();
        Ok(res)
    }

    fn get_file_map(&self) -> io::Result<impl Iterator<Item = DirEntry>> {
        self
            .as_ref()
            .read_dir()?
            .map(|x| x.unwrap())
            .pip(Ok)
    }

    fn write_by<T: AsRef<Path> + Debug>(&self, data: T) {
        let mut path = std::fs::File
            ::create(Path::new(self.as_ref())).unwrap();
        write!(path, "{:?}", data).unwrap();
    }

    fn write_to<T: AsRef<Path>>(&self, data: T)
    where
        Self: Debug,
    {
        data.write_by(self.as_ref());
    }

    fn handle_file_recur(&self, f: fn(&Path)) {
        if self.as_ref().is_dir() {
            self
                .get_file_vec()
                .unwrap()
                .iter()
                .for_each(|x| {
                    self.as_ref().join(x).as_path().handle_file_recur(f);
                })
        } else {
            f(self.as_ref());
        }
    }

    fn file_name_str(&self) -> &str {
        self.as_ref().file_name().unwrap().to_str().unwrap()
    }

    fn parent_str(&self) -> &str {
        self.as_ref().parent().unwrap().to_str().unwrap()
    }

    fn split_dir_and_file(&self) -> (&str, &str) {
        (
            self.parent_str(),
            self.file_name_str(),
        )
    }

    fn file_size(&self) -> f64 {
        self.as_ref().metadata().unwrap().len() as f64 / 1024. / 1024.
    }

    fn check_or_make(&self) {
        if !self.as_ref().exists() {
            self.build_an_empty_dir()
        }
    }
}
impl<T: AsRef<Path>> FileStr for T {}


pub trait ToStr {
    fn to_str(&self) -> &str;
}

impl ToStr for [u8] {
    fn to_str(&self) -> &str {
        std::str::from_utf8(self).unwrap()
    }
}

pub trait IoTest {
    fn io_test(&self);
}

impl<T> IoTest for T
where
   for<'de> T: Serialize + Deserialize<'de>,
{
    fn io_test(&self) {
        t!(self.sof("_", "."));
        t!(rof::<T>("_", "."));
        "_".file_size().print();
        "_".remove();
    }
}

use std::path::PathBuf;
pub fn rename_date(p: &PathBuf) -> std::io::Result<()> {
    p
        .get_file_vec()
        .unwrap()
        .into_iter()
        .for_each(|x| {
            let x_new = x.replace('-', "");
            let path_old = p.join(&x);
            if path_old.is_dir() { panic!() }
            let path_new = p.join(x_new);
            std::fs::rename(path_old, path_new).unwrap();
        });
    Ok(())
}


use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use chrono::{DateTime, TimeZone};
pub fn system_time_to_date_time(t: SystemTime) -> DateTime<Local> {
    let (sec, nsec) = match t.duration_since(UNIX_EPOCH) {
        Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
        Err(e) => { // unlikely but should be handled
            let dur = e.duration();
            let (sec, nsec) = (dur.as_secs() as i64, dur.subsec_nanos());
            if nsec == 0 {
                (-sec, 0)
            } else {
                (-sec - 1, 1_000_000_000 - nsec)
            }
        },
    };
    Local.timestamp_opt(sec, nsec).unwrap()
}

pub struct DateRange {
    start: da,
    end: da,
}

pub struct DateRangeIter {
    end: da,
    last: da,
}

impl DateRange {
    pub fn new(start: da, end: da) -> Self {
        Self { start, end }
    }
}

impl IntoIterator for DateRange {
    type Item = da;
    type IntoIter = DateRangeIter;
    fn into_iter(self) -> Self::IntoIter {
        DateRangeIter {
            end: self.end,
            last: self.start.pred_opt().unwrap(),
        }        
    }
}

impl Iterator for DateRangeIter {
    type Item = da;
    fn next(&mut self) -> Option<Self::Item> {
        if self.last >= self.end {
            return None;
        }
        self.last = self.last.succ_opt()?;
        Some(self.last)
    }
}


pub trait LeakData {
    type Output;
    fn leak_data(&self) -> Self::Output;
}

impl<T: Clone + Hash + std::cmp::Eq> LeakData for std::collections::HashMap<T, String> {
    type Output = std::collections::HashMap<T, &'static str>;
    fn leak_data(&self) -> Self::Output {
        self
            .iter()
            .fold(Self::Output::default(), |mut accu, (k, v)| {
                let v_str: &'static str = Box::leak(v.clone().into_boxed_str());
                accu.insert(k.clone(), v_str);
                accu
            })
    }
}