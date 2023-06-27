use super::MergedRow;
use crate::common::fileops::read_file;
use crate::common::numerical::MultiFieldCmp;
use crate::query::concrete_types::ConcreteType;
use crate::query::tuple_table::TableErrors::{InvalidColumn, MissingFields};
use crate::schema::schema::Field;
use crate::schema::types::Type;
use crate::{FieldId, AQUADIR, AQUA_TMP_DIR};
use rand::{random, thread_rng, Rng};
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env::current_exe;
use std::fmt::{Display, Formatter, Pointer};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
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

type TableHeaders = HashMap<FieldId, (usize, Type)>;
type Column = ConcreteType;
type Row = Vec<Column>;
type RowMap = HashMap<FieldId, Option<Vec<u8>>>;
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

// const TMP_DIR: &str = "tests/db/db_tmp";

impl Iterator for TupleTableIter {
    type Item = MergedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.table.data.is_empty() {
            if self.table.segments.is_empty() {
                return None;
            }
            self.table.load_segment(0);
        }
        if self.next_row == self.table.num_rows as usize {
            if self.next_segment >= self.table.segments.len() {
                return None;
            }
            self.table.load_segment(self.next_segment);
            if self.table.data.is_empty() {
                return None;
            }
            self.next_segment += 1;
            self.next_row = 0;
        }
        let row = self.table.data[self.next_row].clone();
        self.next_row += 1;
        Some(self.row_to_merged_row(row))
    }
}

#[derive(Debug)]
pub struct TupleTableIter {
    table: TupleTable,
    next_row: usize,
    next_segment: usize,
    index_map: HashMap<usize, FieldId>,
}
impl TupleTableIter {
    fn new(table: TupleTable) -> Self {
        let index_map = table
            .index_type_map
            .iter()
            .map(|(k, (idx, _))| (*idx, k.clone()))
            .collect();
        Self {
            table,
            next_row: 0,
            next_segment: 1,
            index_map,
        }
    }
    pub fn step_back(&mut self) {
        if self.next_row != 0 {
            self.next_row -= 1;
        } else {
            self.table.load_segment(self.next_segment - 2);
            self.next_segment -= 1;
        }
    }
    fn row_to_merged_row(&self, row: Row) -> MergedRow {
        row.into_iter()
            .enumerate()
            .map(|(idx, col)| (self.index_map.get(&idx).unwrap().clone(), col.to_bytes()))
            .collect()
    }
}

#[derive(Debug)]
pub struct TupleTable {
    name: String,
    num_rows: u32,
    num_cols: u32,
    index_type_map: TableHeaders,
    data: Vec<Row>,
    segments: Vec<File>,
    current_segment: Option<usize>,
    max_data_size: usize,
    current_memory_use: usize,
}

impl IntoIterator for TupleTable {
    type Item = MergedRow;
    type IntoIter = TupleTableIter;

    fn into_iter(self) -> Self::IntoIter {
        TupleTableIter::new(self)
    }
}

impl TupleTable {
    pub fn get_headers(&self) -> &TableHeaders {
        &self.index_type_map
    }
    pub fn new(name: &str, headers: HashMap<FieldId, Type>, max_memory: usize) -> Self {
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
            current_segment: None,
            index_type_map: table_headers,
            max_data_size: max_memory,
            current_memory_use: 0,
        }
    }

    fn purge_mem_disk(&mut self) {
        let mut rng = thread_rng();
        let tmp_dir = AQUA_TMP_DIR();
        let path = format!("tmp{}{}", self.name, rng.gen::<u64>());
        let path = tmp_dir.join(path.as_str());
        let mut segment = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path.clone())
            .unwrap();
        let mut current_segment_data = std::mem::take(&mut self.data);

        bincode::encode_into_std_write(
            current_segment_data,
            &mut segment,
            bincode::config::standard(),
        )
        .unwrap();
        let segment = File::options().read(true).write(true).open(path).unwrap();
        self.segments.push(segment);
        self.num_rows = 0;
        self.current_memory_use = 0;
        self.current_segment = None;
    }

    pub fn add_row_map(&mut self, row_map: RowMap) -> InsertResult {
        if self.current_memory_use >= self.max_data_size {
            self.purge_mem_disk();
        }
        if row_map.len() != self.num_cols as usize {
            return Err(MissingFields);
        }
        let mut row = vec![Column::default(); self.num_cols as usize];
        for (field_name, data) in row_map.into_iter() {
            match self.index_type_map.get(&field_name) {
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

    fn load_segment(&mut self, segment: usize) {
        let file = &mut self.segments[segment];
        self.current_memory_use = file.metadata().unwrap().len() as usize;
        let mut data = vec![];
        file.read_to_end(&mut data).unwrap();
        file.rewind();
        self.data = bincode::decode_from_slice(&data, bincode::config::standard())
            .unwrap()
            .0;
        self.num_rows = self.data.len() as u32;
        self.current_segment = Some(segment);
    }
    pub fn print_all(&mut self) {
        for row in self.data.iter() {
            println!("{}", RowPrint(row));
        }
        let just_printed_idx = if self.current_segment.is_none() {
            self.purge_mem_disk();
            self.segments.len() - 1
        } else {
            self.current_segment.unwrap()
        };
        let rest: Vec<usize> = (0..self.segments.len())
            .filter(|idx| *idx != just_printed_idx)
            .collect();
        for seg in rest {
            self.load_segment(seg);
            for row in self.data.iter() {
                println!("{}", RowPrint(row));
            }
        }
    }

    pub fn print_sort_asc(&mut self) {
        self.print_all()
    }

    fn get_cols_at(row: &Row, at: &[usize]) -> Row {
        at.iter().map(|idx| row[*idx].clone()).collect()
    }

    pub fn sort_single(&mut self, sort_key: &FieldId, desc: bool) {
        self.sort(&[sort_key.clone()], &[desc])
    }

    pub fn sort(&mut self, sort_keys: &[FieldId], desc: &[bool]) {
        let key_indexes = sort_keys
            .iter()
            .map(|sort_key| self.index_type_map.get(sort_key).unwrap().0)
            .collect::<Vec<_>>();
        if self.segments.is_empty() {
            self.data.sort_unstable_by(|r1, r2| {
                let first = Self::get_cols_at(r1, &key_indexes);
                let second = Self::get_cols_at(r2, &key_indexes);
                (&*first).multi_cmp(&second, desc)
            });
        } else {
            self.external_merge(key_indexes.clone(), desc)
        }
    }

    fn external_merge(&mut self, key_index: Vec<usize>, desc: &[bool]) {
        self.num_rows = 0;
        self.current_memory_use = 0;
        let mut runs = vec![];
        self.purge_mem_disk();
        let mut disk_segments = mem::take(&mut self.segments);
        while !disk_segments.is_empty() {
            let mut cutoff = disk_segments.len() as isize - 2;
            if cutoff < 1 {
                cutoff = 0;
            }
            let mut run = SortingRun::init(
                disk_segments.split_off(cutoff as usize),
                key_index.clone(),
                desc,
            );
            runs.push(run);
        }

        while runs.len() > 1 {
            runs = runs
                .chunks_mut(2)
                .map(|run| SortingRun::merge(run, desc))
                .collect();
        }
        let mut sorted = runs.remove(0);
        self.segments = (mem::take(&mut sorted.segments))
            .into_iter()
            .collect::<Vec<File>>();

        self.purge_mem_disk();
    }
}

#[derive(Debug)]
struct SortingRun {
    key_indexes: Vec<usize>,
    segments: VecDeque<File>,
    current_data: VecDeque<Row>,
    current_seg: Option<File>,
}
impl SortingRun {
    fn new(key_indexes: Vec<usize>) -> Self {
        Self {
            key_indexes,
            segments: VecDeque::new(),
            current_data: VecDeque::new(),
            current_seg: None,
        }
    }
    // fn memory_purge_run(
    //     table_name: &str,
    //     key_indexes: Vec<usize>,
    //     mut current_data: Vec<Row>,
    //     mut disk_seg: File,
    // ) -> Self {
    //     let mut rng = thread_rng();
    //     let config = bincode::config::standard();
    //     let mut disk_data = vec![];
    //     disk_seg.read_to_end(&mut disk_data).unwrap();
    //     disk_seg.rewind();
    //     let mut disk_data: Vec<Row> = bincode::decode_from_slice(&disk_data, config).unwrap().0;
    //     current_data.append(&mut disk_data);
    //     current_data.sort_unstable_by(|r1, r2| r1[key_index].cmp(&r2[key_index]));
    //     let len = current_data.len();
    //     let mut dst = bincode::encode_to_vec(&current_data[..len / 2], config).unwrap();
    //     let tmp_dir = AQUA_TMP_DIR();
    //     let path = format!("tmp{}{}", table_name, rng.gen::<u64>());
    //     let path = tmp_dir.join(path.as_str());
    //     println!("{:?}", path.clone());
    //     let mut segment = File::options()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(path.clone())
    //         .unwrap();
    //     segment.write_all(&dst).unwrap();
    //     segment.rewind();
    //     dst.clear();
    //     dst = bincode::encode_to_vec(&current_data[len / 2..], config).unwrap();
    //     disk_seg.write_all(&dst).unwrap();
    //     disk_seg.rewind();
    //     Self {
    //         key_index,
    //         segments: VecDeque::from([segment, disk_seg]),
    //         current_data: VecDeque::new(),
    //         current_seg: None,
    //     }
    // }
    fn init(mut segments: Vec<File>, key_indexes: Vec<usize>, desc: &[bool]) -> Self {
        let config = bincode::config::standard();
        let mut data: Vec<Row> = vec![];
        let mut disk_buff = vec![];
        for mut seg in &mut segments {
            seg.read_to_end(&mut disk_buff).unwrap();
            seg.rewind();
            data.append(&mut bincode::decode_from_slice(&disk_buff, config).unwrap().0);
            disk_buff.clear();
        }
        data.sort_unstable_by(|r1, r2| {
            let first = TupleTable::get_cols_at(r1, &key_indexes);
            let second = TupleTable::get_cols_at(r2, &key_indexes);
            (&*first).multi_cmp(&second, desc)
        });
        let mut run_segments = VecDeque::new();
        let part_size = data.len() / segments.len();
        let mut data_iter = data.chunks(part_size);
        for mut seg_file in segments {
            let mem_buff = bincode::encode_to_vec(data_iter.next().unwrap(), config).unwrap();
            seg_file.write_all(&mem_buff).unwrap();
            seg_file.rewind();
            run_segments.push_back(seg_file);
        }
        Self {
            key_indexes,
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

        let mut disk_buff = vec![];
        seg.read_to_end(&mut disk_buff);
        seg.rewind();
        let v = seg.metadata().unwrap();
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
            let tmp_dir = AQUA_TMP_DIR();
            let path = format!("tmp{random}");
            let path = tmp_dir.join(path.as_str());
            let mut segment = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .unwrap();
            segment.write_all(&mem_buff).unwrap();
            segment.rewind();
            self.segments.push_back(segment);
            self.current_data.clear();
        }
    }

    fn finalize(&mut self, random: u64) {
        let mem_buff =
            bincode::encode_to_vec(&self.current_data, bincode::config::standard()).unwrap();
        let tmp_dir = AQUA_TMP_DIR();
        let path = format!("tmp{random}");
        let path = tmp_dir.join(path.as_str());
        let mut segment = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        segment.write_all(&mem_buff).unwrap();
        segment.rewind();
        self.segments.push_back(segment);
        self.current_data.clear();
    }

    fn merge(mut runs: &mut [SortingRun], desc: &[bool]) -> Self {
        let mut rng = thread_rng();
        let sort_keys = runs[0].key_indexes.clone();
        if runs.len() == 1 {
            return SortingRun {
                current_data: VecDeque::new(),
                current_seg: None,
                key_indexes: sort_keys,
                segments: mem::take(&mut runs[0].segments),
            };
        }
        let mut removed = HashSet::new();
        let mut output_run = SortingRun {
            current_data: VecDeque::new(),
            current_seg: None,
            key_indexes: sort_keys.clone(),
            segments: VecDeque::new(),
        };
        for run in runs.iter_mut() {
            run.load_segment();
        }
        let max_rows = runs.last().unwrap().current_data.len();
        let runs_length = runs.len();
        while removed.len() != runs_length {
            let (idx, least_run) = runs
                .iter_mut()
                .enumerate()
                .filter(|(idx, _)| !removed.contains(idx))
                .min_by(|(_, run1), (_, run2)| {
                    let r1 = run1.peek_row();
                    let r2 = run2.peek_row();
                    let first = TupleTable::get_cols_at(r1, &sort_keys);
                    let second = TupleTable::get_cols_at(r2, &sort_keys);
                    (&*first).multi_cmp(&second, desc)
                    // let order = run1.peek_row()[sort_key].cmp(&run2.peek_row()[sort_key]);
                    // if !desc {
                    //     order
                    // } else {
                    //     order.reverse()
                    // }
                })
                .unwrap();
            let (row, seg) = least_run.get_row();
            if seg.is_some() && least_run.has_more() {
                least_run.load_segment();
            }
            output_run.add_row(max_rows, row, rng.gen());
            if least_run.has_no_more() {
                removed.insert(idx);
            }
        }
        output_run.finalize(rng.gen());
        output_run
    }
    // fn print_run(&mut self){
    //     println!("RUN PRINT");
    //     let config = bincode::config::standard();
    //     let sort_key = self.key_index;
    //     for seg in &mut self.segments{
    //         let mut disk_buff = vec![];
    //         let mut data : Vec<Row> = Vec::new();
    //         seg.read_to_end(&mut disk_buff);
    //         seg.rewind();
    //         let v = seg.metadata().unwrap();
    //         data.append(&mut bincode::decode_from_slice(&disk_buff, config).unwrap().0);
    //         for v in &data{
    //             println!("{}",v[sort_key]);
    //         }
    //     }
    //     println!("RUN FINISH");
    // }
    // fn split_into_two(self) -> (Self, Self) {
    //     let Self {
    //         key_index,
    //         mut segments,
    //         current_data,
    //         current_seg,
    //     } = self;
    //     let split2 = segments.split_off(segments.len() / 2);
    //     (
    //         Self {
    //             key_index,
    //             segments,
    //             current_data: VecDeque::new(),
    //             current_seg: None,
    //         },
    //         Self {
    //             key_index,
    //             segments: split2,
    //             current_data: VecDeque::new(),
    //             current_seg: None,
    //         },
    //     )
    // }
}
