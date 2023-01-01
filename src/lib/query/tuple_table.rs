use crate::common::fileops::read_file;
use crate::query::concrete_types::ConcreteType;
use crate::query::tuple_table::TableErrors::{InvalidColumn, MissingFields};
use crate::schema::types::Type;
use rand::{random, thread_rng, Rng};
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env::current_exe;
use std::fmt::{Display, Formatter, Pointer};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::mem;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableErrors {
    #[error("Number of Columns does not match")]
    MissingFields,
    #[error("Invalid Column Header Found")]
    InvalidColumn,
}

type TableHeaders = HashMap<String, (usize, Type)>;
type Column = ConcreteType;
type Row = Vec<Column>;
type RowMap = HashMap<String, Option<Vec<u8>>>;
type InsertResult = Result<(), TableErrors>;

struct RowPrint<'a>(&'a Row);
impl<'a> Display for RowPrint<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = self.0.len();
        for idx in 0..len - 1 {
            let col = &self.0[idx];
            // let col = col.to_string()
            f.write_fmt(format_args!("{col},"));
        }
        let col = &self.0[len - 1];
        // let col = col.as_ref().map_or("".to_string(),|column| column.to_string());
        f.write_fmt(format_args!("{col}"))
    }
}

const TMP_DIR: &str = "tests/db/db_tmp";

pub struct Table {
    name: String,
    num_rows: u32,
    num_cols: u32,
    index_type_map: TableHeaders,
    data: Vec<Row>,
    segments: Vec<(u32, File)>,
    current_segment: u32,
    max_data_size: usize,
    current_memory_use: usize,
}
impl Table {
    pub fn new(name: &str, headers: HashMap<String, Type>, max_memory: usize) -> Self {
        let num_cols = headers.len();
        let mut table_headers = TableHeaders::new();
        for (idx, (col_name, col_type)) in headers.into_iter().enumerate() {
            table_headers.insert(col_name, (idx, col_type));
        }
        Self {
            name: name.to_string(),
            num_cols: num_cols as u32,
            num_rows: 0,
            data: vec![],
            segments: vec![],
            index_type_map: table_headers,
            current_segment: 0,
            max_data_size: max_memory,
            current_memory_use: 0,
        }
    }
    pub fn add_row_map(&mut self, row_map: RowMap) -> InsertResult {
        if self.current_memory_use >= self.max_data_size {
            let tmp_dir = Path::new(TMP_DIR);
            let path = format!("tmp{}{}", self.name, self.current_segment);
            let path = tmp_dir.join(path.as_str());
            let mut segment = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path.clone())
                .unwrap();
            let mut current_segment_data = std::mem::take(&mut self.data);

            /////// sort test
            current_segment_data.sort_unstable_by(|r1, r2| r1[0].cmp(r2.get(0).unwrap()));

            bincode::encode_into_std_write(
                current_segment_data,
                &mut segment,
                bincode::config::standard(),
            )
            .unwrap();
            let segment = File::options().read(true).write(true).open(path).unwrap();
            self.segments.push((self.current_segment, segment));
            self.current_segment += 1;
            self.num_rows = 0;
            self.current_memory_use = 0;
        }
        if row_map.len() != self.num_cols as usize {
            return Err(MissingFields);
        }
        let mut row = vec![Column::default(); self.num_cols as usize];
        for (field_name, data) in row_map.into_iter() {
            match self.index_type_map.get(field_name.as_str()) {
                None => {
                    return Err(InvalidColumn);
                }
                Some((idx, schema_type)) => {
                    row[*idx] = data.map_or(Default::default(), |data| {
                        self.current_memory_use += data.len();
                        ConcreteType::from_bytes(*schema_type, data.as_slice())
                    })
                }
            }
        }
        self.data.push(row);
        self.num_rows += 1;
        Ok(())
    }
    // pub fn add_row_vec(&mut self,row:Row) -> InsertResult{
    //     if row.len() != self.num_cols as usize{
    //         return Err(MissingFields)
    //     }
    //     self.data.push(row);
    //     self.num_rows +=1;
    //     Ok(())
    // }

    fn load_segment(&mut self, segment: u32) {
        let file = &mut self
            .segments
            .iter_mut()
            .find(|(seg, file)| *seg == segment)
            .unwrap()
            .1;
        self.current_memory_use = file.metadata().unwrap().len() as usize;
        let mut data = vec![];
        let v = file.read_to_end(&mut data).unwrap();
        self.data = bincode::decode_from_slice(&data, bincode::config::standard())
            .unwrap()
            .0;
        self.num_rows = self.data.len() as u32;
        self.current_segment = segment;
    }
    pub fn print_all(&mut self) {
        let current_seg = self.current_segment;
        for row in self.data.iter() {
            println!("{}", RowPrint(row));
        }
        let rest: Vec<u32> = self
            .segments
            .iter()
            .filter(|(seg, _)| *seg != current_seg)
            .map(|(seg, _)| *seg)
            .collect();
        for seg in rest {
            self.load_segment(seg);
            for row in self.data.iter() {
                println!("{}", RowPrint(row));
            }
        }
    }

    pub fn sort(&mut self, sort_key: &str) {
        let key_index = self.index_type_map.get(sort_key).unwrap().0;
        if self.segments.is_empty() {
            self.data
                .sort_unstable_by(|r1, r2| r1[key_index].cmp(&r2[key_index]));
        } else {
            self.external_merge(key_index)
        }
    }

    fn external_merge(&mut self, key_index: usize) {
        self.num_rows = 0;
        self.current_memory_use = 0;
        let mut runs = vec![];
        let mut disk_segments = mem::take(&mut self.segments);
        let (current_seg, current_data) = (self.current_segment, mem::take(&mut self.data));
        let purge_run = SortingRun::memory_purge_run(self.name.as_str(),key_index,
            current_seg,current_data,disk_segments.remove(0)
        );
        runs.push(purge_run);
        while !disk_segments.is_empty(){
            let run = SortingRun::init(vec![ disk_segments.swap_remove(0),disk_segments.swap_remove(0)],key_index);
            runs.push(run);
        }
        while runs.len() > 1 {
            runs = runs.chunks_mut(2).map(SortingRun::merge).collect();
        }
        let sorted = runs.remove(0);
        let mut rng = thread_rng();
        self.segments = sorted.segments.into_iter().map(|f| (rng.gen(),f) ).collect();
    }
}

struct SortingRun {
    key_index: usize,
    segments: VecDeque<File>,
    current_data: VecDeque<Row>,
    current_seg: Option<File>,
}
impl SortingRun {
    fn memory_purge_run(
        table_name: &str,
        key_index: usize,
        current_seg: u32,
        mut current_data: Vec<Row>,
        mut disk_seg: (u32, File),
    ) -> Self {
        let config = bincode::config::standard();
        let mut disk_data = vec![];
        disk_seg.1.read_to_end(&mut disk_data).unwrap();
        let mut disk_data: Vec<Row> = bincode::decode_from_slice(&disk_data, config).unwrap().0;
        current_data.append(&mut disk_data);
        current_data.sort_unstable_by(|r1, r2| r1[key_index].cmp(&r2[key_index]));
        let len = current_data.len();
        let mut dst = bincode::encode_to_vec(&current_data[..len / 2], config).unwrap();
        let tmp_dir = Path::new(TMP_DIR);
        let path = format!("tmp{}{}", table_name, current_seg);
        let path = tmp_dir.join(path.as_str());
        let mut segment = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path.clone())
            .unwrap();
        segment.write_all(&dst).unwrap();
        dst.clear();
        dst = bincode::encode_to_vec(&current_data[len / 2..], config).unwrap();
        disk_seg.1.write_all(&dst).unwrap();
        Self {
            key_index,
            segments: VecDeque::from([segment, disk_seg.1]),
            current_data: VecDeque::new(),
            current_seg: None,
        }
    }
    fn init(mut segments: Vec<(u32, File)>, key_index: usize) -> Self {
        let config = bincode::config::standard();
        let mut data: Vec<Row> = vec![];
        let mut disk_buff = vec![];
        for mut seg in &mut segments {
            seg.1.read_to_end(&mut disk_buff).unwrap();
            data.append(&mut bincode::decode_from_slice(&disk_buff, config).unwrap().0);
            disk_buff.clear();
        }
        data.sort_unstable_by(|r1, r2| r1[key_index].cmp(&r2[key_index]));
        let mut run_segments = VecDeque::new();
        let part_size = data.len() / segments.len();
        let mut data_iter = data.chunks(part_size);
        for (mut seg_num, mut seg_file) in segments {
            let mem_buff = bincode::encode_to_vec(data_iter.next().unwrap(), config).unwrap();
            seg_file.write_all(&mem_buff).unwrap();
            run_segments.push_back(seg_file);
        }
        Self {
            key_index,
            segments: run_segments,
            current_data: VecDeque::new(),
            current_seg: None,
        }
    }
    fn get_row(&mut self) -> (Row, Option<File>) {
        if self.current_seg.is_none() {
            self.load_segment();
        }
        let row = self.current_data.pop_front().unwrap();
        if self.current_data.is_empty() {
            (row, mem::take(&mut self.current_seg))
        } else {
            (row, None)
        }
    }
    fn load_segment(&mut self) {
        let config = bincode::config::standard();
        let mut seg = self.segments.pop_front().unwrap();
        let disk_buff = read_file(&mut seg);
        self.current_data
            .append(&mut bincode::decode_from_slice(&disk_buff, config).unwrap().0);
        self.current_seg = Some(seg);
    }
    fn peek_row(&self) -> &Row {
        self.current_data.front().unwrap()
    }
    fn has_no_more(&self) -> bool {
        self.current_data.is_empty() && self.segments.is_empty()
    }

    fn has_more(&self) -> bool {
        !self.current_data.is_empty() || !self.segments.is_empty()
    }

    fn add_row(&mut self, max_rows: usize, row: Row, random: u64) {
        self.current_data.push_back(row);
        if self.current_data.len() >= max_rows {
            let mem_buff =
                bincode::encode_to_vec(&self.current_data, bincode::config::standard()).unwrap();
            let tmp_dir = Path::new(TMP_DIR);
            let path = format!("tmp{random}");
            let path = tmp_dir.join(path.as_str());
            let mut segment = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .unwrap();
            segment.write_all(&mem_buff).unwrap();
            self.segments.push_back(segment);
            self.current_data.clear();
        }
    }

    fn merge(mut runs: &mut [SortingRun]) -> Self {
        let mut rng = thread_rng();
        let sort_key = runs[0].key_index;
        let max_rows = runs[0].current_data.len();
        let mut removed = HashSet::new();
        let mut output_run = SortingRun {
            current_data: VecDeque::new(),
            current_seg: None,
            key_index: sort_key,
            segments: VecDeque::new(),
        };
        while !runs.is_empty() {
            let (idx, least_run) = runs
                .iter_mut()
                .enumerate().filter(|(idx,_)| !removed.contains(idx) )
                .min_by(|(_, run1), (_, run2)| {
                    run1.peek_row()[sort_key].cmp(&run2.peek_row()[sort_key])
                })
                .unwrap();
            let (row, seg) = least_run.get_row();
            output_run.add_row(max_rows, row, rng.gen());
            if least_run.has_no_more() {
                removed.insert(idx);
            }
        }
        output_run
    }
}
