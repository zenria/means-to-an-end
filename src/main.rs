use std::{
    collections::BTreeMap,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
};

use clap::Parser;

#[derive(Parser)]
struct Args {
    /// bind the service to this tcp port, default 5555
    #[arg(short, long, default_value = "5555")]
    port: u16,
}

fn main() {
    let args = Args::parse();
    let s = format!("0.0.0.0:{}", args.port)
        .parse::<SocketAddr>()
        .unwrap();
    println!("Listening to {s}");
    let listener = TcpListener::bind(s).unwrap();
    for incoming in listener.incoming() {
        match incoming {
            Ok(incoming) => {
                thread::spawn(|| echo(incoming));
            }

            Err(e) => eprintln!("error {e}"),
        }
    }
}

enum Query {
    Insert { timestamp: i32, value: i32 },
    Query { min_ts: i32, max_ts: i32 },
    Unknown,
}

impl Query {
    fn process(self, stream: &mut TcpStream, db: &mut BTreeMap<i32, i32>) {
        let peer_addr = stream.peer_addr().unwrap();
        match self {
            Query::Insert { timestamp, value } => {
                println!("{peer_addr} - insert {timestamp} => {value}");
                db.insert(timestamp, value);
            }
            Query::Query { min_ts, max_ts } => {
                // compute mean
                let mut count = 0;
                let mut total: i64 = 0;
                if min_ts <= max_ts {
                    db.range(min_ts..=max_ts).for_each(|(_k, v)| {
                        count += 1;
                        total += *v as i64;
                    });
                }
                let mean = if count > 0 { total / count } else { 0 };
                // convert back to i32
                let mean = mean as i32;
                println!("{peer_addr} - query {min_ts},{max_ts} => {mean}");

                stream
                    .write_all(&mean.to_be_bytes())
                    .expect("Cannot write to socket!");
            }
            Query::Unknown => {}
        }
    }
}

fn echo(mut stream: TcpStream) {
    let mut db: BTreeMap<i32, i32> = BTreeMap::new();

    let peer_addr = stream.peer_addr().unwrap();

    println!("{peer_addr} - connected!");

    let mut buf = [0u8; 9];
    loop {
        match stream.read_exact(&mut buf) {
            Ok(_) => {
                let query = buf[0] as char;
                let first = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
                let second = i32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
                let message = match query {
                    'I' => Query::Insert {
                        timestamp: first,
                        value: second,
                    },
                    'Q' => Query::Query {
                        min_ts: first,
                        max_ts: second,
                    },
                    _ => Query::Unknown,
                };
                message.process(&mut stream, &mut db);
            }
            Err(e) => {
                if e.kind() != ErrorKind::UnexpectedEof {
                    eprintln!("{peer_addr} - en error occured reading socket {e}");
                }
                break;
            }
        }
    }
    println!("{peer_addr} - connection ended.");
}
