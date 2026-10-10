//! Recursive-descent parser for the query language.
//!
//! ```text
//! statement   := [EXPLAIN] select | show
//! select      := SELECT projection {, projection} FROM name
//!                [WHERE expr] [GROUP BY group {, group}] [FILL ( fill )]
//!                [ORDER BY time (ASC|DESC)] [LIMIT int] [;]
//! projection  := * | name [AS name] | func ( name [, number] ) [AS name]
//! group       := time ( duration [, duration] ) | name | *
//! fill        := null | none | previous | linear | number
//! expr        := and { OR and }
//! and         := term { AND term }
//! term        := ( expr ) | time cmp timeexpr | name tagop literal
//! timeexpr    := (now() | 'rfc3339' | int | duration) { (+|-) duration }
//! show        := SHOW MEASUREMENTS
//!              | SHOW TAG KEYS [FROM name]
//!              | SHOW TAG VALUES [FROM name] WITH KEY = name
//!              | SHOW FIELD KEYS [FROM name]
//!              | SHOW SERIES [FROM name] [WHERE expr]
//! ```

use crate::error::{Error, Result};
use crate::query::ast::*;
use crate::query::lexer::{tokenize, Token, TokenKind};
use crate::query::time::parse_rfc3339;

/// Parse a single statement.
pub fn parse(src: &str) -> Result<Statement> {
    let mut p = Parser {
        tokens: tokenize(src)?,
        pos: 0,
    };
    let stmt = p.statement()?;
    p.eat(&TokenKind::Semicolon);
    if !p.at(&TokenKind::Eof) {
        return Err(p.error("unexpected input after end of statement"));
    }
    Ok(stmt)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn peek_kind(&self) -> &TokenKind {
        &self.peek().kind
    }

    fn next(&mut self) -> Token {
        let t = self.peek().clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn at(&self, kind: &TokenKind) -> bool {
        self.peek_kind() == kind
    }

    fn eat(&mut self, kind: &TokenKind) -> bool {
        if self.at(kind) {
            self.next();
            true
        } else {
            false
        }
    }

    fn error(&self, message: impl Into<String>) -> Error {
        Error::Parse {
            offset: self.peek().offset,
            message: message.into(),
        }
    }

    fn describe(kind: &TokenKind) -> String {
        match kind {
            TokenKind::Eof => "end of input".into(),
            TokenKind::Ident(s) => format!("{s:?}"),
            other => format!("{other:?}"),
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<()> {
        if self.eat(&kind) {
            Ok(())
        } else {
            Err(self.error(format!(
                "expected {}, found {}",
                Self::describe(&kind),
                Self::describe(self.peek_kind())
            )))
        }
    }

    fn at_keyword(&self, kw: &str) -> bool {
        matches!(self.peek_kind(), TokenKind::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        if self.at_keyword(kw) {
            self.next();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<()> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(self.error(format!(
                "expected {kw}, found {}",
                Self::describe(self.peek_kind())
            )))
        }
    }

    /// An identifier: bare (if not a reserved keyword) or double-quoted.
    fn name(&mut self, what: &str) -> Result<String> {
        match self.peek_kind().clone() {
            TokenKind::QuotedIdent(s) => {
                self.next();
                Ok(s)
            }
            TokenKind::Ident(s) if !is_reserved(&s) => {
                self.next();
                Ok(s)
            }
            other => Err(self.error(format!("expected {what}, found {}", Self::describe(&other)))),
        }
    }

    fn statement(&mut self) -> Result<Statement> {
        if self.eat_keyword("EXPLAIN") {
            self.expect_keyword("SELECT")?;
            return Ok(Statement::Explain(self.select()?));
        }
        if self.eat_keyword("SELECT") {
            return Ok(Statement::Select(self.select()?));
        }
        if self.eat_keyword("SHOW") {
            return self.show();
        }
        Err(self.error("expected SELECT, EXPLAIN or SHOW"))
    }

    fn optional_from(&mut self) -> Result<Option<String>> {
        if self.eat_keyword("FROM") {
            Ok(Some(self.name("measurement name")?))
        } else {
            Ok(None)
        }
    }

    fn show(&mut self) -> Result<Statement> {
        if self.eat_keyword("MEASUREMENTS") {
            return Ok(Statement::ShowMeasurements);
        }
        if self.eat_keyword("TAG") {
            if self.eat_keyword("KEYS") {
                return Ok(Statement::ShowTagKeys {
                    from: self.optional_from()?,
                });
            }
            self.expect_keyword("VALUES")?;
            let from = self.optional_from()?;
            self.expect_keyword("WITH")?;
            self.expect_keyword("KEY")?;
            self.expect(TokenKind::Eq)?;
            let key = self.name("tag key")?;
            return Ok(Statement::ShowTagValues { from, key });
        }
        if self.eat_keyword("FIELD") {
            self.expect_keyword("KEYS")?;
            return Ok(Statement::ShowFieldKeys {
                from: self.optional_from()?,
            });
        }
        if self.eat_keyword("SERIES") {
            let from = self.optional_from()?;
            let condition = if self.eat_keyword("WHERE") {
                let expr = self.expr()?;
                if expr.mentions_time() {
                    return Err(Error::Parse {
                        offset: 0,
                        message: "SHOW SERIES does not support time conditions".into(),
                    });
                }
                Some(expr)
            } else {
                None
            };
            return Ok(Statement::ShowSeries { from, condition });
        }
        Err(self.error("expected MEASUREMENTS, TAG, FIELD or SERIES after SHOW"))
    }

    fn select(&mut self) -> Result<SelectStatement> {
        let mut projections = vec![self.projection()?];
        while self.eat(&TokenKind::Comma) {
            projections.push(self.projection()?);
        }
        self.expect_keyword("FROM")?;
        let from = self.name("measurement name")?;
        let condition = if self.eat_keyword("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        let group_by = if self.eat_keyword("GROUP") {
            self.expect_keyword("BY")?;
            self.group_by()?
        } else {
            GroupBy::default()
        };
        let fill = if self.eat_keyword("FILL") {
            self.fill()?
        } else {
            Fill::default()
        };
        let mut descending = false;
        if self.eat_keyword("ORDER") {
            self.expect_keyword("BY")?;
            self.expect_keyword("time")?;
            if self.eat_keyword("DESC") {
                descending = true;
            } else {
                self.eat_keyword("ASC");
            }
        }
        let limit = if self.eat_keyword("LIMIT") {
            match self.next().kind {
                TokenKind::Int(n) if n >= 0 => Some(n as usize),
                _ => return Err(self.error("LIMIT expects a non-negative integer")),
            }
        } else {
            None
        };
        Ok(SelectStatement {
            projections,
            from,
            condition,
            group_by,
            fill,
            descending,
            limit,
        })
    }

    fn alias(&mut self) -> Result<Option<String>> {
        if self.eat_keyword("AS") {
            Ok(Some(self.name("alias")?))
        } else {
            Ok(None)
        }
    }

    fn projection(&mut self) -> Result<Projection> {
        if self.eat(&TokenKind::Star) {
            return Ok(Projection::Wildcard);
        }
        let offset = self.peek().offset;
        let name = self.name("field or function")?;
        if !self.eat(&TokenKind::LParen) {
            if name.eq_ignore_ascii_case("time") {
                return Err(Error::Parse {
                    offset,
                    message: "time is always returned and cannot be selected explicitly".into(),
                });
            }
            let alias = self.alias()?;
            return Ok(Projection::Field { name, alias });
        }
        let field = self.name("field name")?;
        let func = if name.eq_ignore_ascii_case("percentile") {
            self.expect(TokenKind::Comma)?;
            let p = match self.next().kind {
                TokenKind::Int(v) => v as f64,
                TokenKind::Float(v) => v,
                _ => return Err(self.error("percentile expects a numeric argument")),
            };
            if !(0.0..=100.0).contains(&p) {
                return Err(Error::Parse {
                    offset,
                    message: format!("percentile {p} is outside 0..=100"),
                });
            }
            AggFunc::Percentile(p)
        } else {
            AggFunc::from_name(&name).ok_or_else(|| Error::Parse {
                offset,
                message: format!("unknown function {name:?}"),
            })?
        };
        self.expect(TokenKind::RParen)?;
        let alias = self.alias()?;
        Ok(Projection::Aggregate { func, field, alias })
    }

    fn group_by(&mut self) -> Result<GroupBy> {
        let mut g = GroupBy::default();
        loop {
            if self.eat(&TokenKind::Star) {
                g.all_tags = true;
            } else if self.at_keyword("time")
                && self.tokens.get(self.pos + 1).map(|t| &t.kind) == Some(&TokenKind::LParen)
            {
                self.next();
                self.next();
                if g.time.is_some() {
                    return Err(self.error("GROUP BY time() given twice"));
                }
                let interval = self.duration("interval")?;
                if interval <= 0 {
                    return Err(self.error("GROUP BY time() interval must be positive"));
                }
                let offset = if self.eat(&TokenKind::Comma) {
                    let negative = self.eat(&TokenKind::Minus);
                    let d = self.duration("offset")?;
                    if negative {
                        -d
                    } else {
                        d
                    }
                } else {
                    0
                };
                self.expect(TokenKind::RParen)?;
                g.time = Some(TimeWindow {
                    interval,
                    offset: offset.rem_euclid(interval),
                });
            } else {
                let tag = self.name("tag key")?;
                if !g.tags.contains(&tag) {
                    g.tags.push(tag);
                }
            }
            if !self.eat(&TokenKind::Comma) {
                return Ok(g);
            }
        }
    }

    fn duration(&mut self, what: &str) -> Result<i64> {
        match self.next().kind {
            TokenKind::Duration(d) => Ok(d),
            _ => Err(self.error(format!("expected a duration for {what}"))),
        }
    }

    fn fill(&mut self) -> Result<Fill> {
        self.expect(TokenKind::LParen)?;
        let negative = self.eat(&TokenKind::Minus);
        let fill = match self.next().kind {
            TokenKind::Ident(s) if !negative => match s.to_ascii_lowercase().as_str() {
                "null" => Fill::Null,
                "none" => Fill::None,
                "previous" => Fill::Previous,
                "linear" => Fill::Linear,
                other => return Err(self.error(format!("unknown fill option {other:?}"))),
            },
            TokenKind::Int(v) => Fill::Value(if negative { -(v as f64) } else { v as f64 }),
            TokenKind::Float(v) => Fill::Value(if negative { -v } else { v }),
            _ => return Err(self.error("expected a fill option")),
        };
        self.expect(TokenKind::RParen)?;
        Ok(fill)
    }

    fn expr(&mut self) -> Result<Expr> {
        let mut lhs = self.and_expr()?;
        while self.eat_keyword("OR") {
            let rhs = self.and_expr()?;
            lhs = Expr::Or(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn and_expr(&mut self) -> Result<Expr> {
        let mut lhs = self.term()?;
        while self.eat_keyword("AND") {
            let rhs = self.term()?;
            lhs = Expr::And(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn term(&mut self) -> Result<Expr> {
        if self.eat(&TokenKind::LParen) {
            let e = self.expr()?;
            self.expect(TokenKind::RParen)?;
            return Ok(e);
        }
        if self.at_keyword("time") {
            self.next();
            let op = match self.next().kind {
                TokenKind::Eq => CmpOp::Eq,
                TokenKind::NotEq => CmpOp::NotEq,
                TokenKind::Lt => CmpOp::Lt,
                TokenKind::LtEq => CmpOp::LtEq,
                TokenKind::Gt => CmpOp::Gt,
                TokenKind::GtEq => CmpOp::GtEq,
                _ => return Err(self.error("expected a comparison operator after time")),
            };
            let value = self.time_expr()?;
            return Ok(Expr::Time { op, value });
        }
        let key = self.name("tag key")?;
        let tok = self.next();
        let op = match tok.kind {
            TokenKind::Eq => TagOp::Eq,
            TokenKind::NotEq => TagOp::NotEq,
            TokenKind::RegexMatch => TagOp::Regex,
            TokenKind::RegexNotMatch => TagOp::NotRegex,
            TokenKind::Lt | TokenKind::LtEq | TokenKind::Gt | TokenKind::GtEq => {
                return Err(Error::Parse {
                    offset: tok.offset,
                    message: format!("tag {key:?} can only be compared with =, !=, =~ or !~"),
                })
            }
            _ => {
                return Err(Error::Parse {
                    offset: tok.offset,
                    message: format!("expected an operator after {key:?}"),
                })
            }
        };
        let value_tok = self.next();
        let value = match (op, value_tok.kind) {
            (TagOp::Eq | TagOp::NotEq, TokenKind::Str(s)) => s,
            (TagOp::Regex | TagOp::NotRegex, TokenKind::Regex(r)) => {
                regex::Regex::new(&r).map_err(|e| Error::Parse {
                    offset: value_tok.offset,
                    message: format!("invalid regex: {e}"),
                })?;
                r
            }
            (TagOp::Eq | TagOp::NotEq, _) => {
                return Err(Error::Parse {
                    offset: value_tok.offset,
                    message: format!("tag {key:?} must be compared with a 'single-quoted' string"),
                })
            }
            _ => {
                return Err(Error::Parse {
                    offset: value_tok.offset,
                    message: "regex operators expect a /regex/ literal".into(),
                })
            }
        };
        Ok(Expr::Tag { key, op, value })
    }

    fn time_expr(&mut self) -> Result<TimeExpr> {
        let tok = self.next();
        let base = match tok.kind {
            TokenKind::Ident(s) if s.eq_ignore_ascii_case("now") => {
                self.expect(TokenKind::LParen)?;
                self.expect(TokenKind::RParen)?;
                TimeBase::Now
            }
            TokenKind::Str(s) => {
                TimeBase::Absolute(parse_rfc3339(&s).ok_or_else(|| Error::Parse {
                    offset: tok.offset,
                    message: format!("invalid RFC 3339 timestamp {s:?}"),
                })?)
            }
            TokenKind::Int(ns) => TimeBase::Absolute(ns),
            TokenKind::Duration(ns) => TimeBase::Absolute(ns),
            TokenKind::Minus => match self.next().kind {
                TokenKind::Int(ns) => TimeBase::Absolute(-ns),
                TokenKind::Duration(ns) => TimeBase::Absolute(-ns),
                _ => return Err(self.error("expected a number after '-'")),
            },
            _ => {
                return Err(Error::Parse {
                    offset: tok.offset,
                    message: "expected now(), a timestamp string or an integer".into(),
                })
            }
        };
        let mut offset: i64 = 0;
        loop {
            let sign = if self.eat(&TokenKind::Plus) {
                1
            } else if self.eat(&TokenKind::Minus) {
                -1
            } else {
                break;
            };
            let d = self.duration("time arithmetic")?;
            offset = offset
                .checked_add(sign * d)
                .ok_or_else(|| self.error("time offset overflows"))?;
        }
        Ok(TimeExpr { base, offset })
    }
}

/// Keywords that cannot be used as bare identifiers.
fn is_reserved(s: &str) -> bool {
    const RESERVED: &[&str] = &[
        "select", "from", "where", "group", "by", "fill", "order", "limit", "and", "or", "as",
        "show", "explain", "with", "asc", "desc",
    ];
    RESERVED.iter().any(|k| s.eq_ignore_ascii_case(k))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select(src: &str) -> SelectStatement {
        match parse(src).unwrap() {
            Statement::Select(s) => s,
            other => panic!("expected select, got {other:?}"),
        }
    }

    #[test]
    fn full_select() {
        let s = select(
            "select mean(usage) as avg, percentile(usage, 99.5), max(usage) from cpu \
             where (host = 'a' or host =~ /^web/) and time >= now() - 1h and time < now() \
             group by time(5m, 1m), region fill(previous) order by time desc limit 10;",
        );
        assert_eq!(s.from, "cpu");
        assert_eq!(s.projections.len(), 3);
        assert_eq!(s.projections[0].output_name(), "avg");
        assert_eq!(s.projections[1].output_name(), "percentile");
        assert!(matches!(
            s.projections[1],
            Projection::Aggregate {
                func: AggFunc::Percentile(p),
                ..
            } if p == 99.5
        ));
        assert_eq!(
            s.group_by.time,
            Some(TimeWindow {
                interval: 300_000_000_000,
                offset: 60_000_000_000
            })
        );
        assert_eq!(s.group_by.tags, vec!["region"]);
        assert_eq!(s.fill, Fill::Previous);
        assert!(s.descending);
        assert_eq!(s.limit, Some(10));
        assert_eq!(
            s.condition.unwrap().to_string(),
            "(((host = 'a' OR host =~ /^web/) AND time >= now() - 1h) AND time < now())"
        );
    }

    #[test]
    fn raw_select_with_wildcard_and_quoted_names() {
        let s = select(r#"SELECT *, "user time" AS ut FROM "my cpu" WHERE "dc name" != 'x y'"#);
        assert_eq!(s.projections[0], Projection::Wildcard);
        assert_eq!(s.projections[1].output_name(), "ut");
        assert_eq!(s.from, "my cpu");
        assert_eq!(s.fill, Fill::Null);
        assert!(!s.descending);
    }

    #[test]
    fn time_literals() {
        let s = select(
            "SELECT v FROM m WHERE time > '2023-11-14T22:13:20Z' + 1s AND time <= 1700000100s",
        );
        let Some(Expr::And(a, b)) = s.condition else {
            panic!()
        };
        assert_eq!(
            *a,
            Expr::Time {
                op: CmpOp::Gt,
                value: TimeExpr {
                    base: TimeBase::Absolute(1_700_000_000_000_000_000),
                    offset: 1_000_000_000
                }
            }
        );
        assert_eq!(
            *b,
            Expr::Time {
                op: CmpOp::LtEq,
                value: TimeExpr {
                    base: TimeBase::Absolute(1_700_000_100_000_000_000),
                    offset: 0
                }
            }
        );
    }

    #[test]
    fn fill_variants_and_group_star() {
        assert_eq!(select("SELECT count(v) FROM m FILL(none)").fill, Fill::None);
        assert_eq!(
            select("SELECT count(v) FROM m FILL(-1.5)").fill,
            Fill::Value(-1.5)
        );
        assert_eq!(
            select("SELECT count(v) FROM m FILL(0)").fill,
            Fill::Value(0.0)
        );
        assert_eq!(
            select("SELECT count(v) FROM m FILL(linear)").fill,
            Fill::Linear
        );
        let g = select("SELECT count(v) FROM m GROUP BY *, time(1h, -15m)").group_by;
        assert!(g.all_tags);
        assert_eq!(g.time.unwrap().offset, 45 * 60_000_000_000);
    }

    #[test]
    fn show_statements() {
        assert_eq!(
            parse("SHOW MEASUREMENTS").unwrap(),
            Statement::ShowMeasurements
        );
        assert_eq!(
            parse("show tag keys from cpu").unwrap(),
            Statement::ShowTagKeys {
                from: Some("cpu".into())
            }
        );
        assert_eq!(
            parse("SHOW TAG VALUES WITH KEY = host").unwrap(),
            Statement::ShowTagValues {
                from: None,
                key: "host".into()
            }
        );
        assert_eq!(
            parse("SHOW FIELD KEYS").unwrap(),
            Statement::ShowFieldKeys { from: None }
        );
        assert!(matches!(
            parse("SHOW SERIES FROM cpu WHERE host = 'a'").unwrap(),
            Statement::ShowSeries {
                condition: Some(_),
                ..
            }
        ));
        assert!(matches!(
            parse("EXPLAIN SELECT v FROM m").unwrap(),
            Statement::Explain(_)
        ));
    }

    #[test]
    fn syntax_errors() {
        for bad in [
            "",
            "SELECT",
            "SELECT FROM m",
            "SELECT v m",
            "SELECT v FROM",
            "SELECT v FROM select",
            "SELECT time FROM m",
            "SELECT nope(v) FROM m",
            "SELECT percentile(v) FROM m",
            "SELECT percentile(v, 101) FROM m",
            "SELECT v FROM m WHERE host = 5",
            "SELECT v FROM m WHERE host > 'a'",
            "SELECT v FROM m WHERE host =~ 'a'",
            "SELECT v FROM m WHERE host =~ /(/",
            "SELECT v FROM m WHERE time > 'yesterday'",
            "SELECT v FROM m WHERE time > now() - 5",
            "SELECT v FROM m GROUP BY time(0s)",
            "SELECT v FROM m GROUP BY time(1m), time(2m)",
            "SELECT v FROM m FILL(sideways)",
            "SELECT v FROM m ORDER BY host",
            "SELECT v FROM m LIMIT -1",
            "SELECT v FROM m extra",
            "SHOW TAG VALUES FROM m",
            "SHOW SERIES WHERE time > 0",
            "SHOW NOTHING",
            "DELETE FROM m",
        ] {
            assert!(parse(bad).is_err(), "expected {bad:?} to fail");
        }
    }

    #[test]
    fn parse_errors_point_at_the_problem() {
        match parse("SELECT v FROM m WHERE host = 5").unwrap_err() {
            Error::Parse { offset, .. } => assert_eq!(offset, 29),
            other => panic!("{other}"),
        }
    }
}
