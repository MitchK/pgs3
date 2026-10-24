use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pgs3: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let mut listen = None;
    let mut database_url = None;
    let mut max_object_bytes = env_usize(
        &["PGS3_MAX_OBJECT_BYTES", "PGSTORE_MAX_OBJECT_BYTES"],
        256 * 1024 * 1024,
    )?;
    let pool_size = env_usize(&["PGS3_POOL_SIZE", "PGSTORE_POOL_SIZE"], 16)?;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            "--listen" => listen = Some(next_value(&mut args, "--listen")?),
            "--database-url" => database_url = Some(next_value(&mut args, "--database-url")?),
            "--max-object-bytes" => {
                max_object_bytes = next_value(&mut args, "--max-object-bytes")?
                    .parse()
                    .map_err(|_| "--max-object-bytes must be an integer".to_string())?;
            }
            other => return Err(format!("unknown argument {other}; try --help")),
        }
    }
    let listen = listen.ok_or("--listen is required, for example --listen 127.0.0.1:9741")?;
    let database_url = database_url.ok_or(
        "--database-url is required, for example --database-url postgres://pgstore:pgstore@127.0.0.1/pgstore",
    )?;
    let hard_cap = 1024 * 1024 * 1024 - 1;
    if max_object_bytes == 0 || max_object_bytes > hard_cap {
        return Err(format!(
            "max object size must be between 1 and {hard_cap} bytes (postgres bytea limit)"
        ));
    }
    let listen: SocketAddr = listen
        .parse()
        .map_err(|_| format!("--listen {listen} is not an ip:port"))?;
    let store = pgs3::Store::connect(&database_url, pool_size)
        .await
        .map_err(|error| error.to_string())?;
    let app = pgs3::router(store, max_object_bytes);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| format!("bind {listen}: {error}"))?;
    let bound = listener
        .local_addr()
        .map_err(|error| format!("local address: {error}"))?;
    eprintln!("pgs3 listening on http://{bound}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|error| format!("server stopped: {error}"))?;
    Ok(())
}

fn print_help() {
    eprintln!(
        "\
pgs3 is an S3-compatible server with Postgres as the store.

Usage:
  pgs3 --listen ADDR --database-url URL [--max-object-bytes N]

  --listen ADDR          Address to bind, for example 127.0.0.1:9741
  --database-url URL     Postgres connection string

Environment:
  PGS3_MAX_OBJECT_BYTES      Default 268435456, maximum 1073741823
  PGS3_POOL_SIZE             Default 16

Connect to the Postgres primary. A lagging replica breaks read-after-write."
    );
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn env_usize(names: &[&str], default: usize) -> Result<usize, String> {
    let Some(name) = names.iter().find(|name| env::var(name).is_ok()) else {
        return Ok(default);
    };
    match env::var(name) {
        Ok(value) if value.is_empty() => Ok(default),
        Ok(value) => value
            .parse()
            .map_err(|_| format!("{name} must be an integer")),
        Err(_) => Ok(default),
    }
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
