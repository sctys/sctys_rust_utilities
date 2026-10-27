//! Filename mechanics with caller-owned templates and names.
use chrono::{DateTime, Utc};

pub const MODULE_ARCHIVE_TEMPLATE: &str = "{module}_{file_date_from}_{file_date_to}.tar.bz2";
pub const SOURCE_ARCHIVE_TEMPLATE: &str = "{source_module}_{file_date_from}_{file_date_to}.tar.bz2";
pub const TEMP_SOURCE_DATA_TEMPLATE: &str = "{prefix}temp.parquet";
pub const PARQUET_EXTENSION: &str = "parquet";

/// Preserves the applications' digit-aware identifier conversion byte for byte.
pub fn digit_snake_case(input: &str) -> String {
    let chars: Vec<_> = input.chars().collect();
    let mut output = String::new();
    for (index, &ch) in chars.iter().enumerate() {
        if ch.is_uppercase() {
            if index > 0 && !chars[index - 1].is_uppercase() && !chars[index - 1].is_ascii_digit() {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push(ch);
        }
        if ch.is_ascii_digit() && chars.get(index + 1).is_some_and(|next| next.is_uppercase()) {
            output.push('_');
        }
    }
    output
}

pub fn logger_name(project: &str, source: &str) -> String {
    format!("{project}.{source}")
}

pub fn is_temp_file(name: &str) -> bool {
    [".tmp", ".swp", ".part"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
        || ["~", ".#"].iter().any(|prefix| name.starts_with(prefix))
}

pub fn is_parquet_file(name: &str) -> bool {
    name.rsplit_once('.')
        .is_some_and(|(_, extension)| extension == PARQUET_EXTENSION)
}

/// Replaces only declared placeholders, preserving application-owned templates.
pub fn archive_name(
    template: &str,
    module: &str,
    from: &DateTime<Utc>,
    to: &DateTime<Utc>,
) -> String {
    template
        .replace("{module}", module)
        .replace("{source_module}", module)
        .replace("{file_date_from}", &from.format("%Y%m%d").to_string())
        .replace("{file_date_to}", &to.format("%Y%m%d").to_string())
}

pub fn dated_name(date: &DateTime<Utc>, name: &str) -> String {
    format!("{}_{name}", date.format("%Y%m%d"))
}

pub fn dataset_name(name: &str, extension: &str) -> String {
    format!("{name}.{extension}")
}

pub fn prefixed_name(template: &str, prefix: &str) -> String {
    template.replace("{prefix}", prefix)
}

/// Legacy module directories are named by their serialized enum representation.
pub fn legacy_name(value: &impl serde::Serialize) -> Result<String, serde_json::Error> {
    serde_json::to_string(value).map(|name| name.replace('"', ""))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_legacy_identifiers() {
        for (input, expected) in [
            ("Eightbo", "eightbo"),
            ("Trading212Price", "trading212_price"),
            ("DatePage", "date_page"),
            ("ETFPrice", "etfprice"),
            ("A1B2C", "a1_b2_c"),
            ("", ""),
        ] {
            assert_eq!(digit_snake_case(input), expected);
        }
    }
    #[test]
    fn test_filename_fixtures() {
        let from = DateTime::from_timestamp(0, 0).unwrap();
        let to = from + chrono::Duration::days(1);
        for placeholder in ["module", "source_module"] {
            assert_eq!(
                archive_name(
                    &format!("{{{placeholder}}}_{{file_date_from}}_{{file_date_to}}.tar.bz2"),
                    "source_date",
                    &from,
                    &to
                ),
                "source_date_19700101_19700102.tar.bz2"
            );
        }
        assert_eq!(
            dated_name(&from, "source.parquet"),
            "19700101_source.parquet"
        );
        assert_eq!(dataset_name("source", "parquet"), "source.parquet");
        assert_eq!(
            prefixed_name("{prefix}temp.parquet", "src_"),
            "src_temp.parquet"
        );
        assert_eq!(logger_name("project", "source"), "project.source");
        for name in ["a.tmp", "a.swp", "a.part", "~a", ".#a"] {
            assert!(is_temp_file(name));
        }
        assert!(!is_temp_file("part.parquet"));
    }
}
