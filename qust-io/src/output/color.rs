#![allow(dead_code)]
pub enum ColorVec {
    Plotly,
    D3,
    G10,
    T10,
    Alphabet,
    Dark24,
    Light24,
}

impl ColorVec {
    pub const fn get_vec(&self) -> &[&str] {
        match self {
            ColorVec::Plotly => Plotly,
            ColorVec::D3 => D3,
            ColorVec::G10 => G10,
            ColorVec::T10 => T10,
            ColorVec::Alphabet => Alphabet,
            ColorVec::Dark24 => Dark24,
            ColorVec::Light24 => Light24
        }
    }
}


pub struct ColorSelect(f32, f32, ColorVec, f32);

impl ColorSelect {
    pub fn new<T>(lower: T, up: T, color_vec: ColorVec) -> Self
    where
        f32: From<T>,
    {
        let s = color_vec.get_vec().len() as f32;
        let (l, u): (f32, f32) = (lower.into(), up.into());
        let m = (u - l) / s;
        Self(l, u, color_vec, m)
    }

    pub fn get<T>(&self, i: T) -> &str
    where
        f32: From<T>,
    {
        let i: f32 = i.into();
        let p = (i - self.0) / self.3;
        if p < 0. {
            return self.2.get_vec().first().unwrap()
        }
        let p = p as usize;
        if p >= self.2.get_vec().len() {
            self.2.get_vec().last().unwrap()
        } else {
            self.2.get_vec()[p]
        }
    }
}

const Plotly: &[&str] = &[
    "#636EFA",
    "#EF553B",
    "#00CC96",
    "#AB63FA",
    "#FFA15A",
    "#19D3F3",
    "#FF6692",
    "#B6E880",
    "#FF97FF",
    "#FECB52",
];
const D3: &[&str] = &[
    "#1F77B4",
    "#FF7F0E",
    "#2CA02C",
    "#D62728",
    "#9467BD",
    "#8C564B",
    "#E377C2",
    "#7F7F7F",
    "#BCBD22",
    "#17BECF",
];
const G10: &[&str] = &[
    "#3366CC",
    "#DC3912",
    "#FF9900",
    "#109618",
    "#990099",
    "#0099C6",
    "#DD4477",
    "#66AA00",
    "#B82E2E",
    "#316395",
];
const T10: &[&str] = &[
    "#4C78A8",
    "#F58518",
    "#E45756",
    "#72B7B2",
    "#54A24B",
    "#EECA3B",
    "#B279A2",
    "#FF9DA6",
    "#9D755D",
    "#BAB0AC",
];
const Alphabet: &[&str] = &[
    "#AA0DFE",
    "#3283FE",
    "#85660D",
    "#782AB6",
    "#565656",
    "#1C8356",
    "#16FF32",
    "#F7E1A0",
    "#E2E2E2",
    "#1CBE4F",
    "#C4451C",
    "#DEA0FD",
    "#FE00FA",
    "#325A9B",
    "#FEAF16",
    "#F8A19F",
    "#90AD1C",
    "#F6222E",
    "#1CFFCE",
    "#2ED9FF",
    "#B10DA1",
    "#C075A6",
    "#FC1CBF",
    "#B00068",
    "#FBE426",
    "#FA0087",
];
const Dark24: &[&str] = &[
    "#2E91E5",
    "#E15F99",
    "#1CA71C",
    "#FB0D0D",
    "#DA16FF",
    "#222A2A",
    "#B68100",
    "#750D86",
    "#EB663B",
    "#511CFB",
    "#00A08B",
    "#FB00D1",
    "#FC0080",
    "#B2828D",
    "#6C7C32",
    "#778AAE",
    "#862A16",
    "#A777F1",
    "#620042",
    "#1616A7",
    "#DA60CA",
    "#6C4516",
    "#0D2A63",
    "#AF0038",
];
const Light24: &[&str] = &[
    "#FD3216",
    "#00FE35",
    "#6A76FC",
    "#FED4C4",
    "#FE00CE",
    "#0DF9FF",
    "#F6F926",
    "#FF9616",
    "#479B55",
    "#EEA6FB",
    "#DC587D",
    "#D626FF",
    "#6E899C",
    "#00B5F7",
    "#B68E00",
    "#C9FBE5",
    "#FF0092",
    "#22FFA7",
    "#E3EE9E",
    "#86CE00",
    "#BC7196",
    "#7E7DCD",
    "#FC6955",
    "#E48F72",
];