use std::io::Write;

use arrow::array::{Array, RecordBatch};
use comfy_table::{ContentArrangement, Table};

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Default)]
pub enum Format {
    #[default]
    Table,
    Csv,
    Json,
}

/// Resolve which format to use. An explicit `--format` always wins; otherwise
/// infer from the `--output` file extension (`.csv` → CSV, `.json`/`.jsonl`/
/// `.ndjson` → JSON), falling back to the terminal table. This keeps
/// `--output amostra.csv` from silently writing an ASCII table into a `.csv`.
pub fn resolve_format(explicit: Option<Format>, output: Option<&std::path::Path>) -> Format {
    if let Some(f) = explicit {
        return f;
    }
    match output
        .and_then(|p| p.extension())
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .as_deref()
    {
        Some("csv") => Format::Csv,
        Some("json" | "jsonl" | "ndjson") => Format::Json,
        _ => Format::Table,
    }
}

pub fn write(format: Format, batches: &[RecordBatch], writer: &mut dyn Write) -> Result<()> {
    match format {
        Format::Table => write_table(batches, writer),
        Format::Csv => write_csv(batches, writer),
        Format::Json => write_json(batches, writer),
    }
}

fn write_table(batches: &[RecordBatch], writer: &mut dyn Write) -> Result<()> {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    if let Some(first) = batches.first() {
        let header: Vec<String> = first
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        table.set_header(header);
    }

    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|col| format_cell(batch.column(col), row))
                .collect();
            table.add_row(cells);
        }
    }

    writeln!(writer, "{table}")?;
    Ok(())
}

fn write_json(batches: &[RecordBatch], writer: &mut dyn Write) -> Result<()> {
    let mut w = arrow::json::LineDelimitedWriter::new(writer);
    for batch in batches {
        w.write(batch)?;
    }
    w.finish()?;
    Ok(())
}

fn write_csv(batches: &[RecordBatch], writer: &mut dyn Write) -> Result<()> {
    let mut w = arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(writer);
    for batch in batches {
        w.write(batch)?;
    }
    Ok(())
}

fn format_cell(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "".to_string();
    }
    arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nome", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["foo", "bar"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn resolve_format_infers_from_extension() {
        use std::path::Path;
        // Explicit format always wins.
        assert_eq!(
            resolve_format(Some(Format::Table), Some(Path::new("x.csv"))),
            Format::Table
        );
        // Inference by extension when no explicit format.
        assert_eq!(resolve_format(None, Some(Path::new("amostra.csv"))), Format::Csv);
        assert_eq!(resolve_format(None, Some(Path::new("out.JSON"))), Format::Json);
        assert_eq!(resolve_format(None, Some(Path::new("out.jsonl"))), Format::Json);
        // Unknown extension or no output → table.
        assert_eq!(resolve_format(None, Some(Path::new("out.txt"))), Format::Table);
        assert_eq!(resolve_format(None, None), Format::Table);
    }

    #[test]
    fn csv_format_emits_csv() {
        let mut buf = Vec::new();
        write(Format::Csv, &[sample_batch()], &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines[0], "id,nome");
        assert_eq!(lines[1], "1,foo");
        assert_eq!(lines[2], "2,bar");
    }

    #[test]
    fn json_format_emits_jsonl() {
        let mut buf = Vec::new();
        write(Format::Json, &[sample_batch()], &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        let v0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v0["id"], 1);
        assert_eq!(v0["nome"], "foo");
    }

    #[test]
    fn table_format_contains_headers_and_rows() {
        let mut buf = Vec::new();
        write(Format::Table, &[sample_batch()], &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("id"), "expected header 'id' in: {s}");
        assert!(s.contains("nome"));
        assert!(s.contains("foo"));
        assert!(s.contains("bar"));
        assert!(s.contains('1'));
        assert!(s.contains('2'));
    }
}
