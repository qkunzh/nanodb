use clap::error::ContextValue::String;
//use kvs::engine::KvStore; use std::io::Read;
use std::io::Read;
use std::{env, fs};

enum Cmd {
    Version,
    Set,
    Get,
    Rmv,
    WrongCmd,
}
impl Cmd {
    pub fn new(cmd: &str) -> Cmd {
        match cmd {
            "-V" => Cmd::Version,
            "set" => Cmd::Set,
            "get" => Cmd::Get,
            "rm" => Cmd::Rmv,
            _ => Cmd::WrongCmd,
        }
    }
}
fn main() {
    let mut file = fs::File::open("./resource/test.txt").expect("none exeist");
    let mut content = "".to_string();
    file.read_to_string(&mut content).expect("fail read file");
    println!("{}", &content);
    println!("{:?}", &file);
}

fn run() {
    // let mut storage = KvStore::new();
    // let args: Vec<String> = env::args().collect();
    // if args.len() < 2 {
    //     println! {"Usage:"};
    // }
    // let cmd = Cmd::new(&args[1]);
    // match cmd {
    //     Cmd::WrongCmd => {
    //         println!("wrong cmd");
    //     }
    //     Cmd::Set => storage.set(args[2].clone(), args[3].clone()),
    //     Cmd::Get => {
    //         storage.get(args[2].clone()).expect("");
    //     }
    //     Cmd::Rmv => storage.remove(args[2].clone()),
    //     Cmd::Version => println!("0.1.1"),
    // }
}
