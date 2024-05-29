use std::error::Error;
use std::fs::File;
use std::process;

use chrono::{DateTime, Utc};
use csv::{ReaderBuilder, WriterBuilder};

fn main() {
    if let Err(err) = process_csv() {
        println!("error running process_csv: {}", err);
        process::exit(1);
    }
}

fn process_csv() -> Result<(), Box<dyn Error>> {
    // Open the file
    let file_path = "race_data.csv";
    let file = File::open(file_path)?;

    // Create a CSV reader
    let mut rdr = ReaderBuilder::new().from_reader(file);

    // Create a CSV writer for the output file
    let output_file = File::create("updated_race_data.csv")?;
    let mut wtr = WriterBuilder::new().from_writer(output_file);

    // Get the headers
    let headers = rdr.headers()?.clone();
    let mut new_headers = headers.clone();
    new_headers.push_field("time_delta");

    wtr.write_record(&new_headers)?;

    // Initialize previous timestamp
    let mut previous_timestamp: Option<DateTime<Utc>> = None;

    // Iterate over records
    for result in rdr.records() {
        let record = result?;
        let mut new_record = record.clone();

        // Parse the timestamp
        let timestamp_str = record.get(headers.iter().position(|h| h == "timestamp").unwrap()).unwrap();
        let timestamp: DateTime<Utc> = timestamp_str.parse()?;

        // Calculate the time difference
        let time_delta = if let Some(prev) = previous_timestamp {
            timestamp.signed_duration_since(prev).num_milliseconds().to_string()
        } else {
            String::new()
        };

        // Update the previous timestamp
        previous_timestamp = Some(timestamp);

        // Add the time_delta to the new record
        new_record.push_field(&time_delta);

        // Write the record to the output file
        wtr.write_record(&new_record)?;
    }

    // Flush the writer
    wtr.flush()?;

    Ok(())
}
