use ext_sort::{buffer::mem::MemoryLimitedBufferBuilder, ExternalSorterBuilder};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::io::{BufWriter, Write, BufReader, Read, self};
use std::path::Path;
use clap::Parser;
use rayon::prelude::*;
use std::fs::File;
use std::time::{Duration, Instant};
use deepsize::DeepSizeOf;


const NONCE_SIZE: usize = 4;
const HASH_SIZE: usize = 32;
const RECORD_SIZE: usize = NONCE_SIZE + HASH_SIZE;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    nonce: [u8; NONCE_SIZE],
    hash: [u8; HASH_SIZE],
    position: usize,
    offset: usize
}

impl Record {
    fn new(nonce: [u8; NONCE_SIZE], hash: [u8; HASH_SIZE], position: usize, offset: usize) -> Self {
        Record { hash, nonce, position, offset }
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for Record {}

impl DeepSizeOf for Record {
    fn deep_size_of(&self) -> usize {
        self.nonce.deep_size_of() + self.hash.deep_size_of()
    }

    fn deep_size_of_children(&self, _: &mut deepsize::Context) -> usize {
        0
    }

}

type Table = Vec<Record>;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    // Specify K size
    #[arg(short)]
    k: u32,

    // Filename
    #[arg(short, long)]
    filename: String,

    /// Specify the maximum amount of memory to be used in MB
    #[arg(short, long)]
    memory_size: i64,

    /// Specify the maximum amount of memory to be used in MB
    #[arg(short, long, default_value_t=7)]
    table_count: i64,

    // Verify
    #[arg(short, long, default_value_t = false)]
    verify: bool
}

fn compute_matching_function(nonce: &[u8; NONCE_SIZE]) -> [u8; 32] {
    let hash_result: [u8; 32] = blake3::hash(nonce).as_bytes()[0..32].try_into().unwrap();
    let mut hasher = blake3::Hasher::new();
    hasher.update(&hash_result);
    let hash_output: [u8; 32] = hasher.finalize().as_bytes()[0..32].try_into().unwrap();
    return hash_output

}

fn forward_propagation(mut tables: Vec<Table>, table_count: i64, n_iterations: i64) -> Vec<Table> {
    println!("Generating Hashes...");
    let new_entries: Vec<Record> = (0..(n_iterations as u32))
        .into_par_iter() 
        .map(|i| {
            let nonce = i.to_le_bytes(); 
            let matching_value = compute_matching_function(&nonce); 
            let record = Record::new(nonce, matching_value, i as usize, i as usize);
            record
        })
        .collect(); 

    tables[0].extend(new_entries);

    for table_id in 1..table_count as usize {
        let new_entries: Vec<Record> = tables[table_id - 1]
            .par_iter() 
            .enumerate()
            .map(|(position,entry)| {
                let matching_value = compute_matching_function(&entry.nonce);
                let record = Record::new(entry.nonce,matching_value, position, position);
                record
            })
            .collect(); 

        tables[table_id].extend(new_entries);
    }

    tables
}

fn read_table_from_file(file_path: &str) -> Vec<Record> {
    let file = File::open(file_path).expect("Couldn't open file");
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();

    let mut nonce = [0u8; NONCE_SIZE];
    let mut hash = [0u8; HASH_SIZE];

    while let Ok(_) = reader.read_exact(&mut nonce) {
        if let Ok(_) = reader.read_exact(&mut hash) {
            let mut position_bytes = [0u8; std::mem::size_of::<usize>()];
            let mut offset_bytes = [0u8; std::mem::size_of::<usize>()];

            if reader.read_exact(&mut position_bytes).is_ok() && reader.read_exact(&mut offset_bytes).is_ok() {
                let position = usize::from_le_bytes(position_bytes);
                let offset = usize::from_le_bytes(offset_bytes);
                
                let record = Record::new(nonce, hash, position, offset);
                records.push(record);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    records
}



fn collation_match(current: &Record, previous: &Record) -> bool {
    let hash_match = current.hash[..8] == previous.hash[..8];
    // let nonce_match = current.nonce == previous.nonce;
    let position_match = current.position - previous.position < 10; 
    let offset_match = current.offset - previous.offset < 10;       

    hash_match && position_match && offset_match
}


fn write_table_to_file(records: &Vec<Record>, file_path: &str) {
    let file = File::create(file_path).expect("Couldn't create file");
    let mut writer = BufWriter::new(file);

    for entry in records {
        writer.write_all(&entry.hash).expect("Unable to write matching value");
        writer.write_all(&entry.nonce).expect("Unable to write nonce");
    }
    writer.flush().unwrap();
}




fn apply_backpropagation(table_count: i64) {
    
    let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();

    for table_id in (1..table_count).rev() {
        println!("Comparing Table {:?} and Table {:?}", table_id, table_id - 1);

        pool.install(|| {
            let prev_table_file = format!("table_{}.bin", table_id - 1);
            let current_table_file = format!("table_{}.bin", table_id);

            let prev_table = read_table_from_file(&prev_table_file);
            let mut current_table = read_table_from_file(&current_table_file);

            let prev_table_map: HashMap<[u8; NONCE_SIZE], Record> = prev_table.iter()
                .map(|entry| (entry.nonce, entry.clone())) 
                .collect();

            let mut new_entries: Vec<Record> = Vec::new();

            for entry in &current_table {
                if let Some(prev_entry) = prev_table_map.get(&entry.nonce) {
                    let collation_condition = collation_match(entry, prev_entry);

                    if collation_condition {
                        new_entries.push(Record::new(entry.nonce, prev_entry.hash, entry.position, prev_entry.offset));
                    }
                }
            }

            current_table = new_entries;

            println!("Table {}: Kept {} entries", table_id, current_table.len());

            current_table.sort_by(|a, b| a.position.cmp(&b.position)); 

            write_table_to_file(&current_table, &current_table_file);
        });
    }

    

}



fn sort_table(table_id: usize, memory_limit_in_bytes: i64, _filename: &String) {
    println!("Sorting Hashes...");
    let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
    let filename: String = format!("hashes_{}_{}.bin", _filename, table_id);
    let sorted_filename = format!("table_{}.bin", table_id);
    pool.install(|| {
        let input_file = File::open(&filename).unwrap();
        let mut input_reader = BufReader::new(input_file);
        let mut output_writer = BufWriter::new(File::create(&sorted_filename).unwrap());

        
        let sorter: ext_sort::ExternalSorter<Record, io::Error, MemoryLimitedBufferBuilder> = ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new("./"))
            .with_buffer(MemoryLimitedBufferBuilder::new(memory_limit_in_bytes as u64))
            .build()
            .unwrap();

        let mut records = Vec::new();
        let mut nonce = [0u8; 4];
        let mut hash = [0u8; 32];
        let mut position = 0;
        let mut offset = 0;

        while let Ok(_) = input_reader.read_exact(&mut nonce) {
            input_reader.read_exact(&mut hash).unwrap();
            let record = Record::new(nonce, hash, position, offset);
            records.push(Ok(record));
            position += 1;
            offset += 1; 
        }

        let sorted = sorter.sort(records.into_iter()).unwrap();

        for result in sorted {
            match result {
                Ok(record) => {
                    output_writer.write_all(&record.hash).unwrap();
                    output_writer.write_all(&record.nonce).unwrap();
                }
                Err(e) => {
                    eprintln!("Error during sorting: {}", e);
                }
            }
        }

        output_writer.flush().unwrap();
    });

}

fn write_to_table(table_id: usize, tables: &Vec<Table>, _filename: &String) {
    println!("Writing Hashes...");
    let filename = format!("hashes_{}_{}.bin", _filename, table_id);
    let file = File::options().append(true).create(true).write(true).open(filename).expect("Unable to create file");
    let mut writer = BufWriter::new(file);

    let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
    pool.install(|| {
        for entry in &tables[table_id] {
            writer.write_all(&entry.hash).expect("Unable to write matching value");
            writer.write_all(&entry.nonce).expect("Unable to write nonce");
        }
    })
}




fn main() {
    let cli = Cli::parse();
    let k = cli.k;
    let filename = cli.filename;
    let table_count = cli.table_count;

    let memory_limit: i64 = cli.memory_size;
    let memory_limit_in_bytes: i64 = memory_limit * (2i64.pow(20));
    let mut total_iterations = 2i64.pow(k)*(RECORD_SIZE as i64)/(memory_limit_in_bytes/(table_count as i64));
    if total_iterations == 0 {
        total_iterations = 1;
    }
    let num_entries = 2i64.pow(k)/total_iterations;
    
    let mut tables: Vec<Table> = vec![Vec::new(); table_count as usize];
    

    let mut table_duration = Duration::from_millis(0);
    let mut backward_duration = Duration::from_millis(0);
    let mut sort_duration = Duration::from_millis(0);
    let mut file_duration = Duration::from_millis(0);
    

    println!("Memory Limit: {:?} MB", memory_limit);
    println!("Total Number of Iterations: {:?}", total_iterations);
    println!("Entries in each Iteration: {:?}", num_entries);



    //let records = generate_hashes(k);
    for i in 0..total_iterations {
        println!("Iteration {:?}", i+1);
        let _table_start = Instant::now();
        tables = forward_propagation( tables, table_count, num_entries);
        let _table_end = _table_start.elapsed();
        table_duration = table_duration + _table_end;
        
        let _file_start = Instant::now();
        for table_id in 0..table_count {
            println!("Writing Table {:?}", table_id);
            write_to_table(table_id as usize, &tables, &filename);
        }
        let _file_end = _file_start.elapsed();
        file_duration = file_duration + _file_end;
    }

    let _sort_start = Instant::now();
    for table_id in 0..table_count {
        sort_table(table_id as usize, memory_limit_in_bytes, &filename);
    }
    let _sort_end = _sort_start.elapsed();
    sort_duration = sort_duration + _sort_end;

    let _backward_start = Instant::now();
    apply_backpropagation(table_count);
    let _backward_end = _backward_start.elapsed();
    backward_duration = backward_duration + _backward_end;

    let total_duration = table_duration + sort_duration + backward_duration + file_duration;

    println!("k={:?} Table_Count={:?} Forward_Propagation={:?} Sorting_Duration={:?} Backward_Propagation={:?} Writing_Duration={:?} Total_Duration={:?}", k, table_count, table_duration, sort_duration, backward_duration, file_duration, total_duration);

}