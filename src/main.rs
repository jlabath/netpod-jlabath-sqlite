use anyhow::{anyhow, Result};
use bendy::encoding::ToBencode;
use rusqlite::{types::Value as SqlValue, Connection, Result as SqlResult};
use serde_json::Value as JsonValue;
use std::env;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process;
use std::sync::{Arc, RwLock};
use std::thread;
mod impls;
mod util;

#[derive(Debug, PartialEq)]
pub enum Op {
    Describe,
    Invoke,
}

impl Op {
    fn from_str(s: &str) -> Result<Op, String> {
        match s {
            "describe" => Ok(Op::Describe),
            "invoke" => Ok(Op::Invoke),
            _ => Err(format!("Invalid operation: {}", s)),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Request {
    op: Op,
    id: Option<String>,
    var: Option<String>,
    args: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct Var {
    name: String,
}

#[derive(PartialEq, Debug)]
pub struct Namespace {
    name: String,
    vars: Vec<Var>,
}

#[derive(PartialEq, Debug)]
pub struct DescribeResponse {
    format: String,
    namespaces: Vec<Namespace>,
}

#[derive(Debug, PartialEq)]
pub enum Status {
    Done,
    Error,
}

impl Status {
    fn as_str(&self) -> &str {
        match self {
            Self::Done => "done",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ErrorResponse {
    id: Option<String>,
    status: Status,
    ex_message: String,
    //ex_data: Option<String>,
}

fn err_response(id: Option<String>, err: anyhow::Error) -> Response {
    Response::Error(ErrorResponse {
        id,
        status: Status::Error,
        ex_message: err.to_string(),
    })
}

#[derive(PartialEq, Debug)]
pub struct InvokeResponse {
    id: String,
    status: Status,
    value: Vec<u8>,
}

#[derive(Debug)]
pub enum Response {
    Describe(DescribeResponse),
    Invoke(InvokeResponse),
    Error(ErrorResponse),
}

fn handle_describe() -> Result<Response> {
    let q_var = Var {
        name: "query".to_string(),
    };
    let exec_var = Var {
        name: "exec".to_string(),
    };
    let append_var = Var {
        name: "append".to_string(),
    };
    let ns = Namespace {
        name: "netpod.jlabath.sqlite".to_string(),
        vars: vec![q_var, exec_var, append_var],
    };
    let r = DescribeResponse {
        format: "json".to_string(),
        namespaces: vec![ns],
    };
    Ok(Response::Describe(r))
}

fn do_query(db_name: Arc<Result<String>>, lock: Arc<RwLock<()>>, args: String) -> Result<Vec<u8>> {
    let decoded_args: Vec<String> = serde_json::from_str(&args)?;
    let query = decoded_args.first().ok_or(anyhow!("no query arg given"))?;
    let name = match &*db_name {
        Ok(name) => Ok(name),
        Err(e) => Err(anyhow::Error::msg(e.to_string())),
    }?;

    let _guard = lock.read().map_err(|e| anyhow::Error::msg(e.to_string()))?;
    let conn = Connection::open(name)?;
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query([])?;
    let mut col_count: usize = 1000000;
    let mut response: Vec<JsonValue> = Vec::new();

    while let Some(row) = rows.next()? {
        let mut cur_row: Vec<JsonValue> = Vec::new();
        for idx in 0..col_count {
            let rv: SqlResult<SqlValue> = row.get(idx);
            match rv {
                Ok(v) => {
                    cur_row.push(util::sql_to_json(v));
                }
                Err(rusqlite::Error::InvalidColumnIndex(idx)) => {
                    //println!("got the invalid col index");
                    col_count = idx;
                    break; //the for loop
                }
                Err(e) => {
                    return Err(e.into());
                }
            };
        }
        let json_row = JsonValue::Array(cur_row);
        response.push(json_row);
    }

    Ok(JsonValue::Array(response).to_string().into_bytes())
}

fn do_exec(db_name: Arc<Result<String>>, lock: Arc<RwLock<()>>, args: String) -> Result<Vec<u8>> {
    let decoded_args: Vec<String> = serde_json::from_str(&args)?;
    let sql = decoded_args.first().ok_or(anyhow!("no sql arg given"))?;
    let name = match &*db_name {
        Ok(name) => Ok(name),
        Err(e) => Err(anyhow::Error::msg(e.to_string())),
    }?;
    let _guard = lock
        .write()
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    let conn = Connection::open(name)?;
    let mut stmt = conn.prepare(sql)?;
    let row_changed = stmt.execute(())?;
    Ok(JsonValue::Number(row_changed.into())
        .to_string()
        .into_bytes())
}

fn invoke_response(id: String, value: Vec<u8>) -> Response {
    let r = InvokeResponse {
        id,
        status: Status::Done,
        value,
    };
    Response::Invoke(r)
}

fn handle_invoke(
    db_name: Arc<Result<String>>,
    lock: Arc<RwLock<()>>,
    req: Request,
) -> Result<Response> {
    let var_name = req.var.unwrap_or("no var in request".to_string());
    match var_name.as_str() {
        "netpod.jlabath.sqlite/query" => {
            match do_query(db_name, lock, req.args.unwrap_or("".to_string())) {
                Ok(v) => Ok(invoke_response(req.id.unwrap_or("".to_string()), v)),
                Err(e) => Ok(err_response(req.id, e)),
            }
        }
        "netpod.jlabath.sqlite/exec" => {
            match do_exec(db_name, lock, req.args.unwrap_or("".to_string())) {
                Ok(v) => Ok(invoke_response(req.id.unwrap_or("".to_string()), v)),
                Err(e) => Ok(err_response(req.id, e)),
            }
        }
        unknown_var => Err(anyhow!("no function for var {}", unknown_var)),
    }
}

fn handle_request(
    db_name: Arc<Result<String>>,
    lock: Arc<RwLock<()>>,
    req: Request,
) -> Result<Response> {
    match req.op {
        Op::Describe => handle_describe(),
        Op::Invoke => handle_invoke(db_name, lock, req),
    }
}

fn read_request(mut stream: &UnixStream) -> Result<Request> {
    let mut buffer = [0; 1024 * 2];
    let mut data = Vec::new();
    let req: Option<Request>;

    loop {
        let bytes_read = stream.read(&mut buffer)?;

        if bytes_read == 0 {
            req = Some(util::decode_request(&data)?);
            break; // End of stream reached
        }

        // Append the read data
        data.extend_from_slice(&buffer[..bytes_read]);

        match util::decode_request(&data) {
            Ok(r) => {
                req = Some(r);
                break;
            }
            Err(_e) => continue,
        }
    }

    req.ok_or(anyhow!("request is None"))
}

fn handle_client(db_name: Arc<Result<String>>, lock: Arc<RwLock<()>>, mut stream: UnixStream) {
    match read_request(&stream) {
        Ok(req) => {
            let response = handle_request(db_name, lock, req);
            match response {
                Ok(response) => match response.to_bencode() {
                    Ok(buf) => {
                        if let Err(err) = stream.write_all(&buf) {
                            eprintln!("writing out stream failed {}", err);
                        }
                    }
                    Err(err) => {
                        let er = err_response(None, anyhow::Error::msg(err.to_string()));
                        if let Ok(e_buf) = er.to_bencode() {
                            if let Err(err) = stream.write_all(&e_buf) {
                                eprintln!("failed writing out err stream {}", err);
                            }
                        }
                    }
                },
                Err(e) => {
                    eprintln!("handle_request failed with `{}`", e);
                    let er = err_response(None, e);
                    match er.to_bencode() {
                        Ok(e_buf) => {
                            if let Err(err) = stream.write_all(&e_buf) {
                                eprintln!("failed writing out stream {}", err);
                            }
                        }
                        Err(err) => {
                            eprintln!("trouble encoding error response {}", err);
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("trouble reading from the stream {}", e);
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Expected exactly one argument!");
        process::exit(1);
    }

    let socket_path = &args[1];

    // Remove existing socket file if it exists
    let _ = std::fs::remove_file(socket_path);

    // Bind to the Unix socket
    let listener = UnixListener::bind(socket_path)?;
    eprintln!("netpod.jlabath.sqlite server listening on {}", socket_path);

    //rw lock for write operations
    let lock = Arc::new(RwLock::new(()));
    let db_name = Arc::new(env::var("SQLITE_DB").map_err(|e| anyhow!("SQLITE_DB {}", e)));

    // Accept connections in a loop
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a new thread for each client
                let lock = lock.clone();
                let db_name = db_name.clone();
                thread::spawn(move || handle_client(db_name, lock, stream));
            }
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
    Ok(())
}
