
#[macro_export]
macro_rules! t {
    ($($tt:tt)+) => {
        let timer = std::time::Instant::now();
        $($tt)+;
        println!("{:?}", timer.elapsed());
    }
}

pub fn type_of<T>(_: &T) {
    let res = std::any::type_name::<T>();
    println!("{res}");
}

use std::fmt::Debug;
pub trait ForDisplay {
    fn evcxr_display(&self); 
}
pub trait ForDisplay2 {
    fn evcxr_display(&self); 
}
impl<'a, T> ForDisplay for [&'a [T]]
where
    T: Debug,
{
    fn evcxr_display(&self) {
        let mut html = String::new();
        html.push_str("<table>");
        for i in 0 .. 50.min(self[0].len()) {
            html.push_str("<tr>");
            for j in self.iter() {
                html.push_str("<td>");
                html.push_str(&format!("{:?}", j[i]));
                html.push_str("</td>");
            }
            html.push_str("</tr>");
        }
        html.push_str("</table>");
        println!("EVCXR_BEGIN_CONTENT text/html\n{}\nEVCXR_END_CONTENT", html);
    }
}

impl<T: Debug> ForDisplay for Vec<Vec<T>> 
{
    fn evcxr_display(&self) {
        self.iter().map(|x| &x[..]).collect::<Vec<_>>().evcxr_display()
    }
}

impl<'a, T> ForDisplay for Vec<&'a [T]>
where
    T: Debug,
{
    fn evcxr_display(&self) {
        let mut html = String::new();
        html.push_str("<table>");
        for i in 0 .. 50.min(self[0].len()) {
            html.push_str("<tr>");
            for j in self.iter() {
                html.push_str("<td>");
                html.push_str(&format!("{:?}", j[i]));
                html.push_str("</td>");
            }
            html.push_str("</tr>");
        }
        html.push_str("</table>");
        println!("EVCXR_BEGIN_CONTENT text/html\n{}\nEVCXR_END_CONTENT", html);
    }
}

impl<T: Debug> ForDisplay2 for [T] {
    fn evcxr_display(&self) {
        <[&Self] as ForDisplay>::evcxr_display(&[self])
    }
}
impl<'b, T: Debug, N: Debug> ForDisplay for (Vec<T>, Vec<&'b [N]>) {
    fn evcxr_display(&self) {
        let mut html = String::new();
        html.push_str("<table>");
        for i in 0 .. 50.min(self.0.len()) {
            html.push_str("<tr>");
            html.push_str("<td>");
            html.push_str(&format!("{:?}", self.0[i]));
            html.push_str("</td>");
            for j in 0 .. self.1.len() {
                html.push_str("<td>");
                html.push_str(&format!("{:?}", self.1[j][i]));
                html.push_str("</td>");
            }
            html.push_str("</tr>");
        }
        html.push_str("</table>");
        println!("EVCXR_BEGIN_CONTENT text/html\n{}\nEVCXR_END_CONTENT", html);
    }
}

pub trait DisplayString {
    fn dispaly_string(&self) -> String;
}

impl<T: DisplayString> ForDisplay for T {
    fn evcxr_display(&self) {
        println!("{:?}", self.dispaly_string());
    }
}

#[macro_export]
macro_rules! match_trait {
    ($ta1: ty, $ta2: path) => {
        fn oll<'a>(data: $ta1) -> impl $ta2 + 'a {
            data
        }
    }
}