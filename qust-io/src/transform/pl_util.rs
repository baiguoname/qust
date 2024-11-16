use polars::prelude::*;
use polars::error::PolarsResult;
use qust::prelude::v32;

pub fn vec_to_dataframe(data: &[v32], index: Option<Vec<chrono::NaiveDateTime>>) -> PolarsResult<DataFrame> {
    let num_cols = data[0].len();

    let mut columns: Vec<Series> = Vec::with_capacity(num_cols);
    if let Some(data) = index {
        columns.push(Series::new("index".into(), data));
    }

    for col_idx in 0..num_cols {
        let column_data: Vec<f32> = data.iter().map(|row| row[col_idx]).collect();
        columns.push(Series::new((&format!("column_{}", col_idx)).into(), column_data));
    }

    let df = DataFrame::new(columns)?;
    
    Ok(df)
}