use std::{
    fs,
    fs::File,
    path::Path,
    collections::HashMap,
    sync::Mutex, 
    io::Write,
};
use chrono::Local;

pub type HmLog = HashMap<String, Mutex<File>>;
pub struct MyLog<T> {
    pub path: String,
    pub io: T,
}

impl<T> MyLog<T> {
    pub fn from_file(path: &str) -> MyLog<File> {
        let file = File::create(path.to_string() + ".log").unwrap();
        MyLog { path: path.to_string(), io: file }
    }

   pub fn from_dir(path_str: &str) -> MyLog<HmLog> {
        let path = Path::new(path_str);
        if path.exists() {
            fs::remove_dir_all(path).unwrap();
        }
        fs::create_dir(path_str).unwrap();
        MyLog {
            path: path_str.to_string(),
            io: HashMap::new()
        }
    }
}

impl MyLog<File> {
    pub fn info_file(&mut self, data: &str) {
        self.io.write_fmt(format_args!("{:.23} {}\n", Local::now().to_string(), data)).unwrap();
    }
}

impl MyLog<HmLog> {
    pub fn info_dir<T: std::fmt::Display>(&self, file: T, data: &str) {
        let mut k = self.io.get(&(file.to_string())).unwrap().lock().unwrap();
        k.write_fmt(format_args!("{:.23} {}\n", Local::now().to_string(), data)).unwrap();
    }
    pub fn create_file(&mut self, file_name: &str) {
        let file = File::create(format!("{}/{}.log", self.path, file_name)).unwrap();
        self.io.insert(file_name.to_string(), Mutex::new(file));
    }
}