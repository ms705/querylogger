extern crate msql_srv;
extern crate mysql;
extern crate mysql_common as myc;
extern crate nom_sql;

use std::error::Error;
use std::thread;
use std::net;
use std::io;

use msql_srv::{Column, ErrorKind, MysqlIntermediary, MysqlShim, ParamParser, QueryResultWriter,
               StatementMetaWriter};

struct MysqlBackend {
    conn: mysql::Conn,
}

impl MysqlShim<net::TcpStream> for MysqlBackend {
    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<net::TcpStream>,
    ) -> io::Result<()> {
        unimplemented!()
    }

    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser,
        results: QueryResultWriter<net::TcpStream>,
    ) -> io::Result<()> {
        unimplemented!()
    }

    fn on_close(&mut self, _: u32) {}

    fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<net::TcpStream>,
    ) -> io::Result<()> {
        match nom_sql::parse_query(&format!("{};", query)) {
            Ok(_) => print!("OK: {}", query),
            Err(_) => print!("FAIL: {}", query),
        }

        match self.conn.query(query) {
            Ok(mut mres) => {
                let schema: Vec<_> = mres.columns_ref()
                    .iter()
                    .map(|c| Column {
                        table: c.table_str().to_string(),
                        column: c.name_str().to_string(),
                        coltype: c.column_type(),
                        colflags: c.flags(),
                    })
                    .collect();

                let rows: Vec<_> = mres.by_ref().collect();
                if rows.len() > 0 || query.to_lowercase().starts_with("select")
                    || query.to_lowercase().starts_with("show") {
                    println!(" -> Ok({} rows)", rows.len());

                    let mut writer = results.start(schema.as_slice())?;
                    for r in rows {
                        writer.write_row(r.unwrap().unwrap())?;
                    }
                    writer.finish()
                } else {
                    println!(" -> Ok({} affected rows)", mres.affected_rows());
                    return results.completed(mres.affected_rows(), mres.last_insert_id());
                }
            }
            Err(e) => {
                println!(" -> Err({:?}", e);
                results.error(ErrorKind::ER_UNKNOWN_ERROR, e.description().as_bytes())
            }
        }
    }
}

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();

    let port = listener.local_addr().unwrap().port();
    println!("listening on {}", port);
    let jh = thread::spawn(move || {
        while let Ok((s, _)) = listener.accept() {
            let mut db = MysqlBackend {
                conn: mysql::Conn::new("mysql://127.0.0.1:3306").unwrap(),
            };

            MysqlIntermediary::run_on_tcp(db, s);
        }
    });

    jh.join().unwrap();
}
