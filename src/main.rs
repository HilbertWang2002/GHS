mod cli;
mod daemon;
mod mcp;

use anyhow::Result;
use clap::{Parser, Subcommand};
use ghs::api::Address;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Submit, manage and compare local experiments", version)]
struct Ghs {
    #[command(flatten)]
    address: Address,
    /// 以 JSON 输出客户端命令的完整结果，适合脚本处理
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the experiment scheduler.
    Daemon(daemon::Options),
    /// Expose the scheduler's tools over MCP stdio.
    Mcp,
    #[command(flatten)]
    Client(cli::Action),
    /// Execute a daemon-reserved attempt in its own Linux session.
    #[command(hide = true)]
    Runner { directory: PathBuf },
    /// Deliver a persisted T3 wake event in a separate process.
    #[command(hide = true)]
    WakeDeliver { directory: PathBuf },
}

fn main() -> Result<()> {
    let Ghs {
        address,
        command,
        json,
    } = Ghs::parse();
    match command {
        Command::Daemon(options) => daemon::run(address, options),
        Command::Mcp => mcp::run(&address.socket()?),
        Command::Client(action) => cli::run(action, &address.socket()?, json),
        Command::Runner { directory } => ghs::process::run(&directory),
        Command::WakeDeliver { directory } => ghs::wake::deliver(&directory),
    }
}
