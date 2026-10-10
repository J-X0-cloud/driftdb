//! Tokenizer for the query language.

use crate::error::{Error, Result};
use crate::query::time::unit_nanos;

/// Kinds of tokens.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// Bare identifier or keyword (keywords are matched case-insensitively
    /// by the parser).
    Ident(String),
    /// Double-quoted identifier, never treated as a keyword.
    QuotedIdent(String),
    /// Single-quoted string literal.
    Str(String),
    /// Integer literal.
    Int(i64),
    /// Floating point literal.
    Float(f64),
    /// Duration literal in nanoseconds (`5m`, `1h30m`).
    Duration(i64),
    /// Regular expression literal `/.../`.
    Regex(String),
    /// `,`
    Comma,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `*`
    Star,
    /// `=`
    Eq,
    /// `!=` or `<>`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `=~`
    RegexMatch,
    /// `!~`
    RegexNotMatch,
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `;`
    Semicolon,
    /// End of input.
    Eof,
}

/// A token and the byte offset where it starts.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    /// What was read.
    pub kind: TokenKind,
    /// Byte offset into the source.
    pub offset: usize,
}

fn err(offset: usize, message: impl Into<String>) -> Error {
    Error::Parse {
        offset,
        message: message.into(),
    }
}

/// Split `src` into tokens, ending with [`TokenKind::Eof`].
pub fn tokenize(src: &str) -> Result<Vec<Token>> {
    let bytes = src.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            // Line comment.
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        let two = |a: u8, b: u8| c == a && bytes.get(i + 1) == Some(&b);
        let (kind, len) = if two(b'!', b'=') || two(b'<', b'>') {
            (TokenKind::NotEq, 2)
        } else if two(b'<', b'=') {
            (TokenKind::LtEq, 2)
        } else if two(b'>', b'=') {
            (TokenKind::GtEq, 2)
        } else if two(b'=', b'~') {
            (TokenKind::RegexMatch, 2)
        } else if two(b'!', b'~') {
            (TokenKind::RegexNotMatch, 2)
        } else {
            match c {
                b',' => (TokenKind::Comma, 1),
                b'(' => (TokenKind::LParen, 1),
                b')' => (TokenKind::RParen, 1),
                b'*' => (TokenKind::Star, 1),
                b'=' => (TokenKind::Eq, 1),
                b'<' => (TokenKind::Lt, 1),
                b'>' => (TokenKind::Gt, 1),
                b'+' => (TokenKind::Plus, 1),
                b'-' => (TokenKind::Minus, 1),
                b';' => (TokenKind::Semicolon, 1),
                b'\'' => {
                    let (s, len) = read_quoted(src, i, '\'')?;
                    (TokenKind::Str(s), len)
                }
                b'"' => {
                    let (s, len) = read_quoted(src, i, '"')?;
                    if s.is_empty() {
                        return Err(err(i, "empty quoted identifier"));
                    }
                    (TokenKind::QuotedIdent(s), len)
                }
                b'/' => {
                    let (s, len) = read_quoted(src, i, '/')?;
                    (TokenKind::Regex(s), len)
                }
                b'0'..=b'9' | b'.' => read_number(src, i)?,
                c if c.is_ascii_alphabetic() || c == b'_' => {
                    let len = bytes[i..]
                        .iter()
                        .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
                        .count();
                    (TokenKind::Ident(src[i..i + len].to_string()), len)
                }
                _ => {
                    let ch = src[i..].chars().next().unwrap_or('?');
                    return Err(err(i, format!("unexpected character {ch:?}")));
                }
            }
        };
        tokens.push(Token {
            kind,
            offset: start,
        });
        i += len;
    }
    tokens.push(Token {
        kind: TokenKind::Eof,
        offset: src.len(),
    });
    Ok(tokens)
}

/// Read a literal delimited by `quote`, honouring backslash escapes of the
/// delimiter and of backslash itself. For regexes, other escapes are kept
/// verbatim so the regex engine sees them.
fn read_quoted(src: &str, start: usize, quote: char) -> Result<(String, usize)> {
    let mut out = String::new();
    let mut chars = src[start + 1..].char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some((_, next)) if next == quote => out.push(next),
                Some((_, '\\')) if quote != '/' => out.push('\\'),
                Some((_, next)) => {
                    out.push('\\');
                    out.push(next);
                }
                None => break,
            },
            c if c == quote => return Ok((out, i + 2)),
            c => out.push(c),
        }
    }
    Err(err(start, format!("unterminated literal starting with {quote}")))
}

fn read_number(src: &str, start: usize) -> Result<(TokenKind, usize)> {
    let bytes = src.as_bytes();
    let mut i = start;
    let mut is_float = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i < bytes.len() && bytes[i] == b'.' {
        is_float = true;
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    if i < bytes.len() && matches!(bytes[i], b'e' | b'E') {
        let mut j = i + 1;
        if j < bytes.len() && matches!(bytes[j], b'+' | b'-') {
            j += 1;
        }
        if j < bytes.len() && bytes[j].is_ascii_digit() {
            is_float = true;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            i = j;
        }
    }
    let text = &src[start..i];
    if text == "." {
        return Err(err(start, "unexpected '.'"));
    }
    // A unit suffix directly after an integer makes a duration; durations
    // may chain several components (`1h30m`).
    if !is_float && i < bytes.len() && (bytes[i].is_ascii_alphabetic() || src[i..].starts_with('µ')) {
        let mut j = i;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || !bytes[j].is_ascii()) {
            j += 1;
        }
        let literal = &src[start..j];
        return match crate::query::time::parse_duration(literal) {
            Some(nanos) => Ok((TokenKind::Duration(nanos), j - start)),
            None => {
                let unit: String = src[i..j].chars().take_while(|c| !c.is_ascii_digit()).collect();
                if unit_nanos(&unit).is_none() {
                    Err(err(start, format!("invalid duration unit {unit:?} in {literal:?}")))
                } else {
                    Err(err(start, format!("invalid duration {literal:?}")))
                }
            }
        };
    }
    if is_float {
        let v: f64 = text
            .parse()
            .map_err(|_| err(start, format!("invalid number {text:?}")))?;
        Ok((TokenKind::Float(v), i - start))
    } else {
        let v: i64 = text
            .parse()
            .map_err(|_| err(start, format!("integer {text} is out of range")))?;
        Ok((TokenKind::Int(v), i - start))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use TokenKind as T;

    fn kinds(src: &str) -> Vec<TokenKind> {
        tokenize(src).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn basic_select() {
        assert_eq!(
            kinds("SELECT mean(v) FROM cpu WHERE host != 'a' AND time >= now() - 5m"),
            vec![
                T::Ident("SELECT".into()),
                T::Ident("mean".into()),
                T::LParen,
                T::Ident("v".into()),
                T::RParen,
                T::Ident("FROM".into()),
                T::Ident("cpu".into()),
                T::Ident("WHERE".into()),
                T::Ident("host".into()),
                T::NotEq,
                T::Str("a".into()),
                T::Ident("AND".into()),
                T::Ident("time".into()),
                T::GtEq,
                T::Ident("now".into()),
                T::LParen,
                T::RParen,
                T::Minus,
                T::Duration(300_000_000_000),
                T::Eof,
            ]
        );
    }

    #[test]
    fn numbers_and_durations() {
        assert_eq!(
            kinds("1 2.5 .5 1e3 7e-1 1h30m 250ms 10us"),
            vec![
                T::Int(1),
                T::Float(2.5),
                T::Float(0.5),
                T::Float(1000.0),
                T::Float(0.7),
                T::Duration(5_400_000_000_000),
                T::Duration(250_000_000),
                T::Duration(10_000),
                T::Eof
            ]
        );
    }

    #[test]
    fn quoting_and_regex() {
        assert_eq!(
            kinds(r#""host name" = 'it\'s' =~ /^web\d+\/x$/ <> !~"#),
            vec![
                T::QuotedIdent("host name".into()),
                T::Eq,
                T::Str("it's".into()),
                T::RegexMatch,
                T::Regex(r"^web\d+/x$".into()),
                T::NotEq,
                T::RegexNotMatch,
                T::Eof
            ]
        );
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(kinds("a -- comment\n b"), vec![T::Ident("a".into()), T::Ident("b".into()), T::Eof]);
    }

    #[test]
    fn errors_carry_offsets() {
        let e = tokenize("SELECT 'open").unwrap_err();
        assert!(matches!(e, Error::Parse { offset: 7, .. }), "{e}");
        assert!(tokenize("x = 5q").is_err());
        assert!(tokenize("x # y").is_err());
        assert!(tokenize("99999999999999999999").is_err());
        assert!(tokenize("\"\"").is_err());
        assert!(tokenize(" . ").is_err());
    }
}
