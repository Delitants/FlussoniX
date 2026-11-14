use serde_json::{Map, Value, json};

const MAX_TOKENS: usize = 8192;
const MAX_STATEMENTS: usize = 4096;

#[derive(Debug)]
pub(crate) struct TextError {
    pub error: &'static str,
    pub detail: String,
    pub line: usize,
    pub col: usize,
    pub config: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    Word(String),
    Open,
    Close,
    Semi,
    Eof,
}

#[derive(Clone, Debug)]
struct Token {
    kind: Kind,
    line: usize,
    col: usize,
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
    line: usize,
    col: usize,
    tokens: usize,
    statements: usize,
    pending: Option<Token>,
    result: Map<String, Value>,
}

pub(crate) fn parse_text(input: &str) -> Result<Value, TextError> {
    Parser {
        input,
        offset: 0,
        line: 1,
        col: 1,
        tokens: 0,
        statements: 0,
        pending: None,
        result: Map::new(),
    }
    .parse()
}

impl Parser<'_> {
    fn error(&self, code: &'static str, detail: &str, line: usize, col: usize) -> TextError {
        TextError {
            error: code,
            detail: detail.into(),
            line,
            col,
            config: Value::Object(self.result.clone()),
        }
    }

    fn current(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.current()?;
        self.offset += c.len_utf8();
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    fn skip_space_and_comments(&mut self) {
        loop {
            while self.current().is_some_and(char::is_whitespace) {
                self.advance();
            }
            let rest = &self.input[self.offset..];
            if rest.starts_with('#') || rest.starts_with("//") {
                while self.current().is_some_and(|c| c != '\n') {
                    self.advance();
                }
            } else {
                break;
            }
        }
    }

    fn token(&mut self) -> Result<Token, TextError> {
        if let Some(token) = self.pending.take() {
            return Ok(token);
        }
        self.skip_space_and_comments();
        let (line, col) = (self.line, self.col);
        if self.tokens == MAX_TOKENS {
            return Err(self.error(
                "invalid_config",
                "text configuration has too many tokens",
                line,
                col,
            ));
        }
        self.tokens += 1;
        let kind = match self.current() {
            None => Kind::Eof,
            Some('{') => {
                self.advance();
                Kind::Open
            }
            Some('}') => {
                self.advance();
                Kind::Close
            }
            Some(';') => {
                self.advance();
                Kind::Semi
            }
            Some('"') => {
                self.advance();
                let mut word = String::new();
                loop {
                    match self.advance() {
                        Some('"') => break,
                        Some('\\') => match self.advance() {
                            Some('"') => word.push('"'),
                            Some('\\') => word.push('\\'),
                            Some('n') => word.push('\n'),
                            Some('r') => word.push('\r'),
                            Some('t') => word.push('\t'),
                            _ => {
                                return Err(self.error(
                                    "invalid_config",
                                    "invalid quoted value",
                                    line,
                                    col,
                                ));
                            }
                        },
                        Some(c) => word.push(c),
                        None => {
                            return Err(self.error(
                                "invalid_config",
                                "unterminated quoted value",
                                line,
                                col,
                            ));
                        }
                    }
                }
                Kind::Word(word)
            }
            Some(_) => {
                let start = self.offset;
                while let Some(c) = self.current() {
                    if c.is_whitespace() || matches!(c, '{' | '}' | ';' | '"') {
                        break;
                    }
                    self.advance();
                }
                Kind::Word(self.input[start..self.offset].into())
            }
        };
        Ok(Token { kind, line, col })
    }

    fn peek(&mut self) -> Result<Token, TextError> {
        let token = self.token()?;
        self.pending = Some(token.clone());
        Ok(token)
    }

    fn word(&mut self) -> Result<String, TextError> {
        let token = self.token()?;
        match token.kind {
            Kind::Word(word) => Ok(word),
            _ => Err(self.error("invalid_config", "expected a value", token.line, token.col)),
        }
    }

    fn expect(&mut self, kind: Kind, detail: &str) -> Result<(), TextError> {
        let token = self.token()?;
        if token.kind == kind {
            Ok(())
        } else {
            Err(self.error("invalid_config", detail, token.line, token.col))
        }
    }

    fn count_statement(&mut self, line: usize, col: usize) -> Result<(), TextError> {
        if self.statements == MAX_STATEMENTS {
            return Err(self.error(
                "invalid_config",
                "text configuration has too many statements",
                line,
                col,
            ));
        }
        self.statements += 1;
        Ok(())
    }

    fn parse(mut self) -> Result<Value, TextError> {
        loop {
            let command = self.token()?;
            let Kind::Word(name) = command.kind else {
                if command.kind == Kind::Eof {
                    return Ok(Value::Object(self.result));
                }
                return Err(self.error(
                    "invalid_config",
                    "expected a stream or template block",
                    command.line,
                    command.col,
                ));
            };
            self.count_statement(command.line, command.col)?;
            let collection = match name.as_str() {
                "stream" => "streams",
                "template" => "templates",
                "http" | "https" | "cluster" | "source" | "peer" | "transcoder" => {
                    return Err(self.error(
                        "invalid_config",
                        "this text directive is not supported; use JSON configuration",
                        command.line,
                        command.col,
                    ));
                }
                _ => {
                    return Err(self.error(
                        "unknown_command",
                        "unknown configuration command",
                        command.line,
                        command.col,
                    ));
                }
            };
            let block_name = self.word()?;
            self.expect(Kind::Open, "expected '{' after block name")?;
            let mut block = Map::new();
            block.insert("name".into(), json!(block_name));
            let mut inputs = Vec::new();
            loop {
                let directive = self.token()?;
                match directive.kind {
                    Kind::Close => break,
                    Kind::Eof => {
                        return Err(self.error(
                            "invalid_config",
                            "unterminated configuration block",
                            directive.line,
                            directive.col,
                        ));
                    }
                    Kind::Word(key) => {
                        self.count_statement(directive.line, directive.col)?;
                        match key.as_str() {
                            "input" => {
                                let url = self.word()?;
                                self.expect(Kind::Semi, "expected ';' after input")?;
                                inputs.push(json!({"url":url}));
                            }
                            "title" | "comment" | "template"
                                if collection == "streams" || key != "template" =>
                            {
                                if block.contains_key(&key) {
                                    return Err(self.error(
                                        "invalid_config",
                                        "duplicate scalar directive",
                                        directive.line,
                                        directive.col,
                                    ));
                                }
                                let value = self.word()?;
                                self.expect(Kind::Semi, "expected ';' after value")?;
                                block.insert(key, json!(value));
                            }
                            "static" | "disabled" => {
                                if block.contains_key(&key) {
                                    return Err(self.error(
                                        "invalid_config",
                                        "duplicate flag directive",
                                        directive.line,
                                        directive.col,
                                    ));
                                }
                                self.expect(Kind::Semi, "expected ';' after flag")?;
                                block.insert(key, json!(true));
                            }
                            _ if matches!(
                                key.as_str(),
                                "transcoder"
                                    | "push"
                                    | "on_play"
                                    | "on_publish"
                                    | "password"
                                    | "template"
                            ) =>
                            {
                                return Err(self.error(
                                    "invalid_config",
                                    "unsupported text directive; use JSON configuration",
                                    directive.line,
                                    directive.col,
                                ));
                            }
                            _ => {
                                return Err(self.error(
                                    "unknown_command",
                                    "unknown configuration command",
                                    directive.line,
                                    directive.col,
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(self.error(
                            "invalid_config",
                            "expected a directive or '}'",
                            directive.line,
                            directive.col,
                        ));
                    }
                }
            }
            if !inputs.is_empty() {
                block.insert("inputs".into(), Value::Array(inputs));
            }
            self.result
                .entry(collection)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .push(Value::Object(block));
            if self.peek()?.kind == Kind::Semi {
                self.token()?;
            }
        }
    }
}
