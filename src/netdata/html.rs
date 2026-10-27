use scraper::Selector;
use std::fmt::Display;

/// Shared helpers for working with HTML selectors.
pub struct HtmlTag {}

impl HtmlTag {
    /// Parses a CSS selector, preserving the error message used by downstream
    /// scraper implementations.
    pub fn parse_selector(tag: &str) -> Selector {
        Selector::parse(tag)
            .unwrap_or_else(|error| panic!("{}", selector_parse_error_message(tag, error)))
    }
}

pub fn selector_parse_error_message(tag: &str, error: impl Display) -> String {
    format!("Unable to parse the tag {tag}. {error}")
}

#[cfg(test)]
mod tests {
    use super::HtmlTag;
    use scraper::Html;

    #[test]
    fn test_parse_selector_extracts_nested_text_and_attributes() {
        let document = Html::parse_document(
            r#"<article><a class="result" href="/item">Hello <b>world</b></a></article>"#,
        );
        let selector = HtmlTag::parse_selector("article > a.result");
        let element = document.select(&selector).next().unwrap();

        assert_eq!(element.value().attr("href"), Some("/item"));
        assert_eq!(element.text().collect::<String>(), "Hello world");
        assert_eq!(document.select(&selector).count(), 1);
    }

    #[test]
    fn test_parse_selector_supports_has_and_is() {
        let document = Html::parse_fragment(
            r#"<article><a class="result">Match</a></article><article>Other</article>"#,
        );
        let selector = HtmlTag::parse_selector("article:has(a:is(.result, .featured))");

        assert_eq!(document.select(&selector).count(), 1);
    }

    #[test]
    #[should_panic(expected = "Unable to parse the tag a[.")]
    fn test_parse_selector_preserves_invalid_selector_context() {
        HtmlTag::parse_selector("a[");
    }
}
