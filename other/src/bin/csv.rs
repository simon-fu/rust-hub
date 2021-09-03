


use std::{collections::HashMap, fs::File, path::Path};

use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use csv::StringRecord;

const TIME_FMT:&str = "%Y-%m-%d %H:%M:%S.%3f";

// fn test_time_fmt() -> Result<()> {
//     let time_str = "2021-08-30 21:48:32.327";
//     let no_timezone = NaiveDateTime::parse_from_str(time_str, TIME_FMT).context(format!("{}", time_str))?;
//     println!("NaiveDateTime=[{:?}]", no_timezone); 
//     println!("NaiveDateTime timestamp_millis=[{}]", no_timezone.timestamp_millis());

//     let nsecs_i64 = 1_000_000 * (no_timezone.timestamp_millis()%1000);
//     let nsecs_u32 = nsecs_i64 as u32;
//     let rtime = NaiveDateTime::from_timestamp(no_timezone.timestamp_millis()/1000, nsecs_u32);
//     println!("rtime=[{}], nsecs_i64={}, nsecs_u32={}", rtime.format("%Y-%m-%d %H:%M:%S.%3f"), nsecs_i64, nsecs_u32);  
//     Ok(())
// }

#[derive(Clone)]
struct Line {
    time: NaiveDateTime,
    record: StringRecord,
}

// fn print_csv_file(filename: &str) -> Result<()> {
//     let file = File::open(&Path::new(filename)).context(format!("couldn't open {}", filename))?;
//     let mut rdr = csv::Reader::from_reader(file);
//     for result in rdr.records() {
//         let record = result?;
//         println!("{:?}", record);
//     }
//     Ok(())
// }

// fn print_lines(vec: &Vec<Line>) {
//     for line in vec {
//         println!("{:?}", line.record);
//     }
// }

fn read_csv_file_to_map(filename: &str) -> Result<HashMap<String, Line>> {
    let time_index = 0;
    let mid_index = 1;
    let mut map : HashMap<String, Line> = HashMap::new();
    let file = File::open(&Path::new(filename)).context(format!("couldn't open {}", filename))?;
    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.records() {
        let record = result?;
        let time_str = record.get(time_index).unwrap();
        let time = NaiveDateTime::parse_from_str(time_str, TIME_FMT).context(format!("{}", time_str))?;
        map.insert(record.get(mid_index).unwrap().to_string(), Line{time, record});
    }

    Ok(map)
}

fn filte_map_by_time_range(
    input: HashMap<String, Line>, 
    time_start: &NaiveDateTime, 
    time_end: &NaiveDateTime) -> Vec<Line> {

    let mut output : Vec<Line> = Vec::new();
    for (_k, v) in input {
        if v.time >= *time_start && v.time <= *time_end {
            output.push(v);
        }
    }
    output
}

fn write_vec_to_csv_file(vec: &Vec<Line>, filename: &str) -> Result<()> {
    let mut wtr = csv::Writer::from_path(filename)?;
    for line in vec {
        wtr.write_record(&line.record)?;
    }
    wtr.flush()?;
    Ok(())
}

pub fn run() -> Result<()> {
    let sender_in_filename = "/Users/simon/Downloads/sender-user-data.csv";
    let recver_in_filename = "/Users/simon/Downloads/receiver-user-data.csv";
    
    let sender_out_filename = "/Users/simon/Downloads/sender-out.csv";
    let recver_out_filename = "/Users/simon/Downloads/receiver-out.csv";

    let time_range_start = NaiveDateTime::parse_from_str("2021-09-02 16:00:00.000", TIME_FMT)?;
    let time_range_end = NaiveDateTime::parse_from_str("2021-09-02 18:00:00.000", TIME_FMT)?;

    // print_csv_file(sender_in_filename)?;

    let mut send_map_out : HashMap<String, Line> = HashMap::new();
    let mut recv_map_out : HashMap<String, Line> = HashMap::new();
    {
        let send_map_in = read_csv_file_to_map(sender_in_filename)?;
        let mut recv_map_in = read_csv_file_to_map(recver_in_filename)?;

        println!("read sender [{}], lines {}", sender_in_filename, send_map_in.len());
        println!("read recver [{}], lines {}", recver_in_filename, recv_map_in.len());

        for (k, v) in send_map_in {
            if let Some(item) = recv_map_in.remove(&k) {
                send_map_out.insert(k.clone(), v);
                recv_map_out.insert(k, item);
            }
        }
    }

    let mut send_out = filte_map_by_time_range(send_map_out, &time_range_start, &time_range_end);
    let mut recv_out = filte_map_by_time_range(recv_map_out, &time_range_start, &time_range_end);

    send_out.sort_by(|a, b| a.time.cmp(&b.time));
    recv_out.sort_by(|a, b| a.time.cmp(&b.time));


    // print_lines(&send_out);

    write_vec_to_csv_file(&send_out, sender_out_filename)?;
    write_vec_to_csv_file(&recv_out, recver_out_filename)?;    

    println!("write sender [{}], lines {}", sender_in_filename, send_out.len());
    println!("write recver [{}], lines {}", recver_in_filename, recv_out.len());

    Ok(())
}


fn main() -> Result<()> {
    run()
}
