//! `cairn-bench-gen`: writes synthetic attributes and a takedown schedule as CSV.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use anyhow::Context;
use cairn_bench_gen::{Correlation, GenConfig, Generator, takedown_schedule};
use clap::{Parser, Subcommand};
use std::io::{BufWriter, Write};

#[derive(Parser)]
#[command(
    name = "cairn-bench-gen",
    about = "Synthetic attributes and workloads for Cairn benchmarks"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write per-item attributes as CSV.
    Attributes {
        /// Number of items.
        #[arg(long, default_value_t = 1_000_000)]
        n: u64,
        /// Seed.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// `random` or `clustered`.
        #[arg(long, default_value = "random")]
        correlation: String,
        /// Number of synthetic clusters (when no labels file is given).
        #[arg(long, default_value_t = 1000)]
        clusters: u32,
        /// Optional file of little-endian u32 cluster labels, one per item.
        #[arg(long)]
        labels: Option<std::path::PathBuf>,
        /// Output CSV path.
        #[arg(long)]
        out: std::path::PathBuf,
    },
    /// Write a takedown schedule as CSV.
    Takedowns {
        /// Number of items.
        #[arg(long, default_value_t = 1_000_000)]
        n: u64,
        /// Seed.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Percent of items deleted per minute.
        #[arg(long, default_value_t = 0.5)]
        percent_per_minute: f64,
        /// Duration in minutes.
        #[arg(long, default_value_t = 10)]
        minutes: u64,
        /// Output CSV path.
        #[arg(long)]
        out: std::path::PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    match Cli::parse().cmd {
        Cmd::Attributes {
            n,
            seed,
            correlation,
            clusters,
            labels,
            out,
        } => {
            let correlation = match correlation.as_str() {
                "random" => Correlation::Random,
                "clustered" => Correlation::Clustered,
                other => anyhow::bail!("unknown correlation {other:?}"),
            };
            let labels: Option<Vec<u32>> = match labels {
                None => None,
                Some(p) => {
                    let bytes =
                        std::fs::read(&p).with_context(|| format!("reading {}", p.display()))?;
                    anyhow::ensure!(
                        bytes.len() == 4 * n as usize,
                        "labels file must hold {n} u32 values"
                    );
                    Some(
                        bytes
                            .chunks_exact(4)
                            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                            .collect(),
                    )
                }
            };
            let g = Generator::new(GenConfig {
                seed,
                n,
                correlation,
                clusters,
                ..GenConfig::default()
            });
            let mut w = BufWriter::new(std::fs::File::create(&out)?);
            writeln!(
                w,
                "id,cluster,flag_50,flag_10,flag_1,flag_01,date,channel,rights,speaker"
            )?;
            for id in 0..n {
                let a = g.attributes(id, labels.as_ref().map(|l| l[id as usize]));
                writeln!(
                    w,
                    "{},{},{},{},{},{},{},{},{},{}",
                    a.id,
                    a.cluster,
                    u8::from(a.flag_50),
                    u8::from(a.flag_10),
                    u8::from(a.flag_1),
                    u8::from(a.flag_01),
                    a.date,
                    a.channel,
                    a.rights.as_str(),
                    a.speaker
                )?;
            }
            w.flush()?;
        }
        Cmd::Takedowns {
            n,
            seed,
            percent_per_minute,
            minutes,
            out,
        } => {
            let mut w = BufWriter::new(std::fs::File::create(&out)?);
            writeln!(w, "at_ms,id")?;
            for t in takedown_schedule(seed, n, percent_per_minute, minutes) {
                writeln!(w, "{},{}", t.at_ms, t.id)?;
            }
            w.flush()?;
        }
    }
    Ok(())
}
