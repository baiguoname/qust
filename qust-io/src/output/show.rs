use qust::prelude::*;
use serde_json;

const i: &str = "   |";

trait Show {
    fn show(&self, ident: usize) -> String;
    fn showi(&self, ident: usize) -> String {
        let mut res = self.show(ident);
        if !res.ends_with('\n') {
            res.push('\n');
        }
        res
    }
}

impl Show for Stra {
    fn show(&self, ident: usize) -> String {
        format!(
            "{}{}{}", 
            self.ident.to_string().showi(ident), 
            self.name.to_string().showi(ident), 
            self.ptm.showi(ident)
        )
    }
}

impl Show for Ptm {
    fn show(&self, ident: usize) -> String {
        use Ptm::*;
        match self {
            Ptm1(m, stp) => format!(
                "{}{}{}",
                "Ptm1".showi(ident), 
                m.showi(ident + 1), 
                stp.showi(ident + 1),
            ),
            Ptm2(m, stp1, stp2) => format!(
                "{}{}{}{}",
                "Ptm2".showi(ident),
                m.showi(ident + 1),
                stp1.showi(ident + 1),
                stp2.showi(ident + 1),
            ),
            Ptm3(m, dire, cond1, cond2) => format!(
                "{}{}{}{}{}",
                "Ptm3".showi(ident),
                m.showi(ident + 1),
                dire.debug_string().showi(ident + 1),
                cond1.showi(ident + 1),
                cond2.showi(ident + 1),
            ),
            Ptm4(ptm1, ptm2) => format!(
                "{}{}{}",
                "Ptm4".showi(ident),
                ptm1.showi(ident + 1),
                ptm2.showi(ident + 1),
            ),
            Ptm5(ptm) => format!(
                "{}{}",
                "Ptm5".showi(ident),
                ptm.showi(ident + 1),
            ),
            _ => panic!(),
        }
    }
}

impl Show for str {
    fn show(&self, ident: usize) -> String {
        format!("{}{}", i.repeat(ident), self)
    }
}

impl Show for Box<dyn Money> {
    fn show(&self, ident: usize) -> String {
        format!("{}{:?}", i.repeat(ident), self)
    }
}

impl Show for Stp {
    fn show(&self, ident: usize) -> String {
        match self {
            Stp::Stp(tsig) => format!(
                "{}{}",
                "Stp".showi(ident),
                tsig.showi(ident + 1),
            ),
            Stp::StpWeight(stp, cond_weight) => format!(
                "{}{}{}",
                "StpWeight".showi(ident),
                stp.showi(ident + 1),
                cond_weight.showi(ident + 1),
            )
        }
    }
}

impl Show for CondWeight {
    fn show(&self, ident: usize) -> String {
        let (cond_vec, i_vec) = self
            .0
            .iter()
            .fold((String::new(), String::new()), |mut accu, x| {
                let cond_str = x.0.showi(ident + 1);
                let i_str = x.1.debug_string().showi(ident + 1);
                accu.0.push_str(&cond_str);
                accu.1.push_str(&i_str);
                accu
            });
        format!(
            "{}{}{}",
            "CondWeight".showi(ident),
            cond_vec,
            i_vec,
        )
    }
}

impl Show for Tsig {
    fn show(&self, ident: usize) -> String {
        match self {
            Tsig::Tsig(dire1, dire2, cond1, cond2) => format!(
                "{}{}{}{}{}",
                "Tsig".showi(ident),
                dire1.debug_string().showi(ident + 1),
                dire2.debug_string().showi(ident + 1),
                cond1.showi(ident + 1),
                cond2.showi(ident + 1),
            ),
            Tsig::TsigFilter(tsig, iocond) => format!(
                "{}{}{}",
                "TsigFilter".showi(ident),
                tsig.showi(ident + 1),
                iocond.showi(ident + 1),
            ),
            _ => todo!(),
        }
    }
}

impl Show for Box<dyn Cond> {
    fn show(&self, ident: usize) -> String {
        let mut value = serde_json::to_value(self).unwrap();
        if value["Cond"] == "Msig" {
            let msig: Msig = serde_json::from_value(value["value"].clone()).unwrap();
            msig.showi(ident)
        } else if value["Cond"] == "Iocond" {
            value.as_object_mut().unwrap().remove("Cond");
            let io_cond: Iocond = serde_json::from_value(value.clone()).unwrap();
            io_cond.showi(ident)
        } else if value["Cond"] == "NotCond" {
            value.as_object_mut().unwrap().remove("Cond");
            let not_cond: NotCond = serde_json::from_value(value.clone()).unwrap();
            format!("!{}", not_cond.cond.debug_string()).showi(ident)
        } else {
            self.debug_string().showi(ident)
        }
    }
}

impl Show for Msig {
    fn show(&self, ident: usize) -> String {
        format!(
            "{}{}{}{}",
            "Msig".showi(ident),
            self.0.debug_string().showi(ident + 1),
            self.1.showi(ident + 1),
            self.2.showi(ident + 1),
        )
    }
}

impl Show for Iocond {
    fn show(&self, ident: usize) -> String {
        format!(
            "{}{}{}",
            "Iocond".showi(ident),
            self.pms.debug_string().showi(ident + 1),
            self.range.debug_string().showi(ident + 1),
        )
    }
}

pub trait EvcxrPrinting {
    fn evcxr_get_string(&self) -> String;
    fn evcxr_display(&self) {
        self.evcxr_get_string().println();
    }
}

impl<T: Show> EvcxrPrinting for T {
    fn evcxr_get_string(&self) -> String {
        self.show(0)
    }
}

pub trait GetDire {
    fn get_dire(&self) -> Option<Dire>;
}

impl GetDire for Ptm {
    fn get_dire(&self) -> Option<Dire> {
        if let Ptm::Ptm3(_, d, _, _) = self {
            Some(*d)
        } else {
            None
        }
    }
}
impl GetDire for Stra {
    fn get_dire(&self) -> Option<Dire> {
        self.ptm.get_dire()
    }
}

pub trait StatsString {
    fn stats_string(&self) -> String;
}

impl StatsString for Stra {
    fn stats_string(&self) -> String {
        format!("{} -- {} -- {:?}", self.ident, self.name, self.get_dire())
    }
}

impl EvcxrPrinting for Stral {
    fn evcxr_get_string(&self) -> String {
        let mut res = String::new();
        self
            .0
            .iter()
            .for_each(|x| {
                let s = x.stats_string();
                res.push_str(&s);
                res.push('\n')
            });
        res
    }
}