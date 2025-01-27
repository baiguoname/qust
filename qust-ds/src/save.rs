use bincode;
use serde::{ 
    Serialize, 
    Serializer, 
    ser::SerializeSeq, 
    de::{self, Error, SeqAccess, DeserializeOwned, DeserializeSeed}, 
    Deserializer
};
use std::{io::{BufReader, BufWriter, Write}, path::PathBuf, sync::RwLock};
use serde_json;
use crate::prelude::*;
use chrono::Datelike;

struct VecEle;

impl<'de> de::Visitor<'de> for VecEle {
    type Value  = dt;
    
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let res = chrono::DateTime::from_timestamp_millis(v).unwrap();
        let res = res.naive_local();
        Ok(res)
    }
}

impl<'de> DeserializeSeed<'de> for VecEle {
    type Value = dt;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_i64(self)
    }
}


pub fn serialize_dt<S>(id: &dt, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let es = id.and_utc().timestamp_millis();
    s.serialize_i64(es)
}

pub fn deserialize_dt<'de, D>(deserializer: D) -> Result<dt, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_i64(VecEle)
}

pub fn serialize_vec_dt<S>(id: &vdt, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = s.serialize_seq(Some(id.len()))?;
    for e in id {
        let es = e.and_utc().timestamp_millis();
        seq.serialize_element(&es)?;
    }
    seq.end()
}

pub fn deserialize_vec_dt<'de, D>(deserializer: D) -> Result<vdt, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct VecDt;

    impl<'de> de::Visitor<'de> for VecDt {
        type Value = Vec<dt>;
    
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string containing json data")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut res: Self::Value = Vec::with_capacity(seq.size_hint().unwrap_or_default());
            while let Some(ele) = seq.next_element_seed(VecEle)? {
                res.push(ele);
            }
            Ok(res)
        }
    }
    deserializer.deserialize_seq(VecDt)
}

pub fn serialize_vec_da<S, T>(dates: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: AsRef<[da]>,
{
    let days_since_epoch: Vec<i32> = dates.as_ref().iter().map(|d| d.num_days_from_ce()).collect();
    days_since_epoch.serialize(serializer)
}

pub fn deserialize_vec_da<'de, D>(deserializer: D) -> Result<Vec<da>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let days_since_epoch: Vec<i32> = Vec::deserialize(deserializer)?;
    Ok(days_since_epoch
        .into_iter()
        .map(|x| da::from_num_days_from_ce_opt(x).unwrap())
        .collect())
}


pub fn serialize_rwlock<T, S>(data: &RwLock<T>, s: S) -> Result<S::Ok, S::Error>
where
    T: Clone + Serialize,
    S: Serializer,
{
    let data = data.read().unwrap().clone();
    data.serialize(s)
}

pub fn deserialize_rwlock<'de, D, T>(desrializer: D) -> Result<RwLock<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    let data = T::deserialize(desrializer)?;
    Ok(RwLock::new(data))
}


pub trait Sof: Sized {
    fn sof(&self, name: &str, path: &str)
    where
        Self: Serialize,
    {
        let full_path = path.to_owned() + "/" + name;
        let w = std::fs::File::create(full_path).unwrap();
        let mut f = BufWriter::new(w);
        bincode::serialize_into(&mut f, self).unwrap();
    }
    fn rof(name: &str, path: &str) -> Self 
    where
        Self: DeserializeOwned,
    {
        let full_path = path.to_owned() + "/" + name;
        let r = std::fs::File::open(&full_path)
            .unwrap_or_else(|_| panic!("file not exist: {}", full_path));
        let mut f = BufReader::new(r);
        let res: Self = bincode::deserialize_from::<&mut BufReader<std::fs::File>, Self>(&mut f).unwrap();
        res
    }

    fn sof_json(&self, name: &str, path: &str)
    where
        Self: Serialize,
    {
        let sof_string = serde_json::to_string(self).unwrap();
        sof_string.sof(name, path);
    }
    fn rof_json(name: &str, path: &str) -> Self
    where
        Self: DeserializeOwned,
    {
        let rof_string = rof::<String>(name, path);
        serde_json::from_str(&rof_string).unwrap()
    }

    fn sof_json_pretty(&self, name: &str, path: &str)
    where
        Self: Serialize,
    {
        let sof_string = serde_json::to_string_pretty(self).unwrap();
        let full_path = path.to_owned() + "/" + name;
        let w = std::fs::File::create(full_path).unwrap();
        let mut f = BufWriter::new(w);
        write!(f, "{}", sof_string).unwrap();
    }

    fn json_string(&self) -> String
    where
        Self: Serialize,
    {
        serde_json::to_string(self).unwrap()
    }

    fn json_string_pretty(&self) -> String
    where
        Self: Serialize,
    {
        serde_json::to_string_pretty(self).unwrap()
    }

    fn my_obj<T>(&self) -> T
    where
        Self: AsRef<str>,
        T: DeserializeOwned,
    {
        serde_json::from_str(self.as_ref()).unwrap()
    }

    fn test_save(&self, n: usize) -> (f32, f32)
    where
        Self: Serialize + DeserializeOwned,
    {
        let timer = std::time::Instant::now();
        for _ in 0..n {
            self.sof("__sof__", ".");
            let _data = rof::<Self>("__sof__", ".");
        }
        let time_pass = timer.elapsed().as_millis() as f32 / n as f32;
        let p = PathBuf::from("__sof__");
        let data_size = p.metadata().unwrap().len() as f32 / (1024. * 1024.);
        p.remove();
        (time_pass, data_size)
    }

    fn rof_vec(data: &[u8]) -> Self
    where
        Self: DeserializeOwned,
    {
        bincode::deserialize(data).unwrap()
    }
}

impl<T> Sof for T {}
pub fn rof<T: DeserializeOwned>(name: &str, path: &str) -> T {
    <T as Sof>::rof(name, path)
}
pub fn rof_json<T: DeserializeOwned + Serialize>(name: &str, path: &str) -> T {
    <T as Sof>::rof_json(name, path)
}
pub fn rof_serialized<T: DeserializeOwned>(data: &[u8]) -> T {
    bincode::deserialize(data).unwrap()
}