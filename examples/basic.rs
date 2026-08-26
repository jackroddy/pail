//! A pipeline end to end: one command on its own, a serial step, a batch, some
//! rust, and two sinks.
//!
//! `cargo run -p pipeline --example basic`

use pail::{Closure, Cmd, PipelineBuilder, Progress, Step, Table};

fn main() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join("pipeline-basic");
    std::fs::remove_dir_all(&dir).ok();
    let data = dir.join("data.txt");
    let table = dir.join("runs.tbl");

    PipelineBuilder::new()
        // a bare Cmd is a step of one
        .step(
            Cmd::new("/bin/sh")
                .name("make-data")
                .arg("-c", "seq 1 2000")
                .stdout_to(&data),
        )
        // one after another
        .step(
            Step::serial([
                Cmd::new("/usr/bin/wc").name("lines").flag("-l").path(&data),
                Cmd::new("/usr/bin/wc").name("bytes").flag("-c").path(&data),
            ])
            .name("count"),
        )
        // three at a time: the step's wall clock comes out near a third of the
        // summed user time
        .step(
            Step::batched(
                3,
                [
                    Cmd::new("/bin/sh")
                        .name("burn-1")
                        .arg("-c", "seq 1 5000000 > /dev/null"),
                    Cmd::new("/bin/sh")
                        .name("burn-2")
                        .arg("-c", "seq 1 10000000 > /dev/null"),
                    Cmd::new("/bin/sh")
                        .name("burn-3")
                        .arg("-c", "seq 1 15000000 > /dev/null"),
                ],
            )
            .name("burn"),
        )
        // rust in place of a command. only the wall clock gets measured, so the
        // cpu, memory and argv columns come out empty
        .step(
            Step::from_closures([
                Closure::new("count-lines", move || {
                    let lines = std::fs::read_to_string(&data)?.lines().count();
                    anyhow::ensure!(lines == 2000, "expected 2000 lines, found {lines}");
                    Ok(())
                }),
                Closure::new("wait", || {
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    Ok(())
                }),
            ])
            .name("check"),
        )
        .sink(Progress::new())
        .sink(Table::new(&table))
        .build()?
        .run()?;

    println!("\n{}", table.display());
    print!("{}", std::fs::read_to_string(&table)?);

    Ok(())
}
