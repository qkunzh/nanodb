use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::windows::fs::FileExt;
use std::{fs, io};

pub enum KvEngineOpenOpts {}
#[derive(Debug)]
pub enum DbError {
    OpenErr,
    CreationErr,
    WriteErr,
    ReadErr,
}
#[derive(Debug)]
pub struct KvStore {
    file: fs::File,
    mem_kv_table: HashMap<String, LogMemValue>,
    current_pos: u32,
}
#[derive(Debug)]
enum Cmd {
    Set,
    //Get,
    Rmv,
}
impl Cmd {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Cmd::Set => vec![0, 0, 0, 0],
            //Cmd::Get => vec![0, 0, 0, 1],
            Cmd::Rmv => vec![0, 0, 0, 2],
        }
    }
}
impl KvStore {
    pub fn set(&mut self, key: String, value: String) -> Result<(), io::Error> {
        // let k_size = self.current_pos + key.len() as u32;
        // let v_size = self.current_pos + value.len() as u32;
        let log_entry = LogDiskKvEntry {
            time_stamp: 0,
            key_size: key.len() as u32,
            value_size: value.len() as u32,
            key: key.as_bytes().to_vec(),
            cmd: Cmd::Set,
            value: value.as_bytes().to_vec(),
        };
        //
        self.current_pos += (key.len() + value.len() + 16) as u32;
        let bytes = log_entry.as_bytes();
        //写入文件
        self.file.write_all(&bytes)?;
        let log_mem_value = LogMemValue {
            time_stamp: 0,
            value_size: value.len() as u32,
            value: value.clone(),
            value_pos: 0,
        };

        //写入 kv_dir
        self.mem_kv_table.insert(key, log_mem_value);
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>, io::Error> {
        let log_mem_value;
        if let Some(v) = self.mem_kv_table.get(&key) {
            log_mem_value = v;
        } else {
            return Ok(None);
        };
        let mut buf = vec![0; log_mem_value.value_size as usize];
        self.file
            .seek_read(&mut buf, log_mem_value.value_pos as u64)?;
        match String::from_utf8(buf) {
            Ok(s) => {
                return Ok(Some(s));
            }
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        }
    }
    //应该改进 返回Option<String>
    pub fn remove(&mut self, key: String) -> Result<(), io::Error> {
        let log_entry = LogDiskKvEntry {
            time_stamp: 0,
            key_size: key.len() as u32,
            value_size: 0,
            key: key.as_bytes().to_vec(),
            cmd: Cmd::Rmv,
            value: vec![],
        };
        self.mem_kv_table.remove(&key);
        self.current_pos += (key.len() as u32 + 16);
        self.file.write(&mut log_entry.as_bytes())?;
        Ok(())
    }
    pub fn open(path: String) -> Result<KvStore, io::Error> {
        //todo 使用match进行文件操作
        /*
        let mut file = OpenOptions::new()
            .create(true) // 如果文件不存在则创建
            .write(true) // 以写入模式打开
            .open(path)?;
        */
        let mut first_open;
        let mut file;
        match File::open(&path) {
            Ok(f) => {
                file = f;
                first_open = false;
            }
            Err(_) => match File::create(&path) {
                Ok(f) => {
                    file = f;
                    first_open = true;
                }
                Err(err) => {
                    return Err(err);
                }
            },
        };
        let mut store = KvStore {
            file,
            mem_kv_table: HashMap::new(),
            current_pos: 0,
        };
        store.rebuild_key_dir(first_open)?;
        Ok(store)
    }
    fn rebuild_key_dir(&mut self, first_open: bool) -> Result<(), io::Error> {
        if first_open {
            //写入元数据信息 magic+本身长度
            let meta_header = KvStoreMethHeader {
                magic: 1314,
                len: 8,
            };
            //这个方法只是借用一下meta_header
            self.file.write(&meta_header.as_bytes())?;
            println! {"{:?}",meta_header};
        } else {
            //读取meta header
            let mut buf = vec![0; 12];
            self.file.seek_read(&mut buf, 0)?;
            let meta_header = KvStoreMethHeader::load_from_bytes(&buf);
            let offset = 0;
            //todo 把磁盘上内容反序列化到key_dir

            while offset <= meta_header.len {
                let mut buf = vec![0; 100];
                let kv_entry = LogDiskKvEntry::load_from_bytes(&buf);
                let (key, value) = (
                    String::from_utf8(kv_entry.key)
                        .ok()
                        .unwrap_or("".to_string()),
                    String::from_utf8(kv_entry.value)
                        .ok()
                        .unwrap_or("".to_string()),
                );
                self.mem_kv_table.insert(
                    key,
                    LogMemValue {
                        value_size: kv_entry.value_size,
                        value_pos: offset,
                        time_stamp: 0,
                        value,
                    },
                );
            }
        }
        Ok(())
    }
    pub fn open_with_opts(path: String, opts: KvEngineOpenOpts) -> Result<KvStore, io::Error> {
        let mut file = OpenOptions::new()
            .create(true) // 如果文件不存在则创建
            .write(true) // 以写入模式打开
            .open(path)?;
        Ok(KvStore {
            file,
            mem_kv_table: HashMap::new(),
            current_pos: 0,
        })
    }
    // pub fn list_keys(self) -> Vec<&str> {
    //    // let mut keys: Vec<&str> = self.mem_kv_table.into_keys().collect();
    //     keys
    // }
    // todo 控制刷盘时机
    fn sync_to_disk(self) {}
    // todo 命令压缩
    fn compact_cmds(&self) {}
}
#[derive(Debug)]
struct LogDiskKvEntry {
    time_stamp: u32,
    key_size: u32,
    value_size: u32,
    cmd: Cmd,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl LogDiskKvEntry {
    pub fn as_bytes(&self) -> Vec<u8> {
        //不需要类型标记
        let mut bytes = vec![];
        bytes.append(&mut self.time_stamp.to_be_bytes().to_vec());
        bytes.append(&mut self.key_size.to_be_bytes().to_vec());
        bytes.append(&mut self.value_size.to_be_bytes().to_vec());
        bytes.append(&mut self.cmd.as_bytes().to_vec());
        //这里需要注意
        bytes.append(&mut self.key.clone());
        bytes.append(&mut self.value.clone());
        bytes
    }
    pub fn load_from_bytes(buf: &Vec<u8>) -> Self {
        LogDiskKvEntry {
            time_stamp: 0,
            key_size: 0,
            value_size: 0,
            cmd: Cmd::Set,
            key: vec![],
            value: vec![],
        }
    }
}
#[derive(Debug)]
struct KvStoreMethHeader {
    magic: u64,
    len: u32,
}
impl KvStoreMethHeader {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.append(&mut self.magic.to_be_bytes().to_vec());
        bytes.append(&mut self.len.to_be_bytes().to_vec());
        bytes
    }
    fn load_from_bytes(buf: &Vec<u8>) -> Self {
        // //let b1: [u8; 8] = buf[0..8].to_vec().into_iter().as_slice() as [u8; 8];
        // let magic = u64::from_be_bytes(b1);
        // let len = u32::from_be_bytes(buf[8..].to_vec().into_iter().collect() as [u8; 4]);
        KvStoreMethHeader { magic: 0, len: 0 }
    }
}
#[derive(Debug)]
struct LogMemValue {
    value_size: u32,
    value_pos: u32,
    value: String,
    time_stamp: u32,
}
