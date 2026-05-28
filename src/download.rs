use scraper::{Html, Selector};

use crate::schema::Period;
use crate::{Error, Result};

pub fn parse_period_listing(html: &str) -> Vec<Period> {
    let doc = Html::parse_document(html);
    let sel = Selector::parse("a").unwrap();
    doc.select(&sel)
        .filter_map(|a| {
            let href = a.value().attr("href")?.trim_end_matches('/');
            href.parse::<Period>().ok()
        })
        .collect()
}

pub fn latest_period(html: &str) -> Result<Period> {
    parse_period_listing(html)
        .into_iter()
        .max()
        .ok_or_else(|| Error::MissingData("no period in listing".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
<html><body>
<a href="2024-12/">2024-12/</a>
<a href="2025-01/">2025-01/</a>
<a href="2026-04/">2026-04/</a>
<a href="readme.txt">readme.txt</a>
</body></html>
"#;

    #[test]
    fn parses_yyyy_mm_anchors() {
        let mut periods = parse_period_listing(SAMPLE);
        periods.sort();
        let strs: Vec<String> = periods.iter().map(|p| p.to_string()).collect();
        assert_eq!(strs, vec!["2024-12", "2025-01", "2026-04"]);
    }

    #[test]
    fn ignores_non_period_anchors() {
        let html = r#"<a href="foo">x</a><a href="2026-05/">2026-05/</a>"#;
        let periods = parse_period_listing(html);
        assert_eq!(periods.len(), 1);
        assert_eq!(periods[0].to_string(), "2026-05");
    }
}
