use qust::prelude::*;
use serde::{Serialize, Deserialize};
use serde_json::{Value, to_string, from_str, error::Error};

pub trait EvalString {
    fn eval_string(&self) -> String;
}

impl EvalString for Msig {
    fn eval_string(&self) -> String {
        format!(
            "msig!({:?}, {}, {})",
            self.0,
            self.1.eval_string(),
            self.2.eval_string(),
        )
    }
}

impl EvalString for Box<dyn Cond> {
    fn eval_string(&self) -> String {
        format!(
            "{:?}",
            self,
        )
    }
}

pub fn ooo<A, T, N, K>(data: T, ty_str: &str, f: K) -> Result<N, Error>
where
    for<'a> A: Serialize + Deserialize<'a>,
    for<'a> T: Serialize + Deserialize<'a>,
    for<'a> N: Serialize + Deserialize<'a>,
    K: Fn(A) -> N,
{
    let k = to_string(&data)?;
    let k_value: Value = from_str(&k)?;
    if k_value["name"] != ty_str {
        from_str::<i32>("aall")?;
    }
    let mut z_value = if let Value::Object(m) = k_value { m } else { panic!() };
    z_value.remove("name");
    let ob_string = if z_value.is_empty() { 
        String::from("null")
    } else if z_value.contains_key("value") {
        to_string(&z_value["value"])?
    } else {
        to_string(&z_value)?
    };
    let ob: A = from_str(&ob_string)?; 
    Ok(f(ob))
}

pub fn oll1(stra: Stra) -> Stra {
    Stra::new(
        stra.ident,
        stra.name,
        match stra.ptm {
            Ptm::Ptm2(m, stp1, stp2) => Ptm::Ptm2(m, oll2(stp1), oll2(stp2)),
            _ => panic!(),
        }
    )
}

pub fn oll2(stp: Stp) -> Stp {
    match stp {
        Stp::Stp(tsig) => Stp::Stp(oll3(tsig)),
        Stp::StpWeight(box_stp, weight) => Stp::StpWeight(Box::new(oll2(*box_stp)), weight) ,
    }
}

pub fn oll3(tsig: Tsig) -> Tsig {
    match tsig {
        Tsig::Tsig(a, b, c, d) => Tsig::Tsig(a, b, oll7(c), oll7(d)),
        Tsig::TsigFilter(tsig_box, iocond) => Tsig::TsigFilter(Box::new(oll3(*tsig_box)), iocond),
        _ => panic!("this is TsigQ"),
    }
}

pub fn oll5(cond_box: Box<dyn Cond>) -> Box<dyn Cond> {
    let res = ooo(cond_box.clone(), "MsigType", |x: Msig| -> Box<dyn Cond> {
        let y_last = ooo(x.2.clone(), "Filterday", |y: Filterday| -> Filterday { y });
        match y_last {
            Ok(_) => {
                oll5(x.1)
            },
            _ => {
                let msig = MsigType(x.0, oll5(x.1), oll5(x.2));
                Box::new(msig)
            }
        }
    });
    match res {
        Ok(res) => {
            // res.print();
            res
        },
        _ => {
        //  other.print();
         cond_box   
        },
    }
}

pub fn oll6(cond_box: Box<dyn Cond>) -> Box<dyn Cond> {
    let res = ooo(cond_box.clone(), "MsigType", |x: Msig| -> Box<dyn Cond> {
        let y_last = ooo(x.2.clone(), "FilterdayTime", |y: FilterdayTime| -> FilterdayTime { y });
        match y_last {
            Ok(_) => {
                oll6(x.1)
            },
            _ => {
                let msig = MsigType(x.0, oll6(x.1), oll6(x.2));
                Box::new(msig)
            }
        }
    });
    match res {
        Ok(res) => res,
        _ => cond_box,
    }
}

pub fn oll7(cond_box: Box<dyn Cond>) -> Box<dyn Cond> {
    oll6(oll5(cond_box))
}