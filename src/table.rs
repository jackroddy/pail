//! A sink that writes the summary table.
//!
//! Two kinds of row, told apart by the first column. A step row carries the
//! step's name and its wall clock; the command rows under it carry a `|` or `||`
//! instead of a name, and their own numbers. A step of one command collapses to
//! a single row, since the two would otherwise say the same thing twice.
//!
//! A block is built when its step finishes, because a column's width is not
//! known until the last cell in it has arrived. [`Mode`] decides what happens
//! to it then.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::Context;

use crate::execute::{Status, Timing};
use crate::fmt::{bytes, cpu_pct, dash, secs};
use crate::item::Item;
use crate::sink::Sink;
use crate::step::{Step, Strategy};

/// Marks a command that ran after the one above it.
const SERIAL: &str = "|";
/// Marks a command that ran alongside the others in its step.
const BATCH: &str = "||";

/// The columns every row ends with, after whatever fields and tags the run
/// carries.
const METRICS: [&str; 8] = [
    "wall(s)", "user(s)", "sys(s)", "cpu(%)", "max_rss", "exit", "status", "argv",
];

/// Whether every block gets its own header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Headers {
    /// One header at the top of the file. Its widths are the floor for every
    /// block, so blocks line up with it and with each other until some value
    /// turns out wider than the label above it.
    Once,
    /// A header on every block, so each block reads on its own.
    Each,
}

/// How the table is laid out and when it reaches the file.
///
/// The combinations that make no sense cannot be written down: there is nothing
/// to decide about headers when the file holds one block, and ragged columns
/// force a header on every block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Hold everything back and write one table with every row padded alike.
    /// Nothing reaches the file until the run is over.
    Whole,
    /// Write each step's block as that step finishes. Every block carries the
    /// same columns, padded to its own contents. The default, with
    /// [`Headers::Once`].
    Blocks { headers: Headers },
    /// Write each step's block as that step finishes, each carrying only the
    /// columns its own commands use.
    Ragged,
}

/// Writes a table of everything the pipeline ran.
#[derive(Debug)]
pub struct Table {
    path: PathBuf,
    mode: Mode,
    /// The columns every block carries, worked out before anything runs so blocks
    /// agree whatever order fields turn up in. Unused by [`Mode::Ragged`], which
    /// asks each step instead.
    columns: Columns,
    /// Every row so far, for [`Mode::Whole`].
    rows: Vec<Vec<Cell>>,
    /// Widths every block starts from, worked out before the run from everything
    /// already known: names, fields, tags, argv. Only the numbers are missing,
    /// and their headings are wider than they usually are. `None` for the modes
    /// that do not share widths between blocks.
    floor: Option<Vec<usize>>,
    text: String,
}

impl Default for Mode {
    fn default() -> Mode {
        Mode::Blocks {
            headers: Headers::Once,
        }
    }
}

impl Table {
    pub fn new(path: impl Into<PathBuf>) -> Table {
        Table {
            path: path.into(),
            mode: Mode::default(),
            columns: Columns::default(),
            rows: Vec::new(),
            floor: None,
            text: String::new(),
        }
    }

    pub fn mode(mut self, mode: Mode) -> Table {
        self.mode = mode;
        self
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&self.path, &self.text)
            .with_context(|| format!("failed to write {}", self.path.display()))
    }
}

impl Sink for Table {
    fn start(&mut self, steps: &[Step]) -> anyhow::Result<()> {
        self.columns = Columns::of(steps);

        // Ragged blocks are meant to differ, and Whole renders in one go, so
        // neither has anything to share.
        self.floor = match self.mode {
            Mode::Ragged | Mode::Whole => None,
            _ => Some(self.columns.measure(steps)),
        };

        self.text.clear();
        if matches!(
            self.mode,
            Mode::Blocks {
                headers: Headers::Once
            }
        ) {
            self.text = render(&self.columns.header(), &[], true, self.floor.as_deref());
        }
        self.flush()
    }

    // no `record`: a step's rows are built from the step itself once it is done,
    // which keeps them in the order the commands were declared rather than the
    // order a batch happened to finish them in

    fn step_done(&mut self, step: &Step) -> anyhow::Result<()> {
        let columns = match self.mode {
            Mode::Ragged => Columns::of(std::slice::from_ref(step)),
            _ => self.columns.clone(),
        };

        let mut rows = columns.block(step);

        let show_header = match self.mode {
            Mode::Whole => {
                self.rows.append(&mut rows);
                return Ok(());
            }
            Mode::Blocks {
                headers: Headers::Once,
            } => false,
            _ => true,
        };

        self.text.push_str(&render(
            &columns.header(),
            &rows,
            show_header,
            self.floor.as_deref(),
        ));
        self.flush()
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        if self.mode == Mode::Whole {
            let rows = std::mem::take(&mut self.rows);
            self.text = render(&self.columns.header(), &rows, true, None);
            self.flush()?;
        }
        Ok(())
    }
}

/// The field keys and tags a block carries, in front of [`METRICS`].
///
/// Which ones there are depends on the commands, so every part of a block —
/// the header, the step line, each command line — has to agree about them. They
/// live here rather than being handed to each in turn.
#[derive(Clone, Debug, Default)]
struct Columns {
    keys: Vec<String>,
    tags: Vec<String>,
    /// Whether anything in the run asks to be pinned. A run where nothing does
    /// gets no cpus column at all, rather than one of nothing but dashes.
    ///
    /// This asks what was requested and not where anything landed, because the
    /// columns are settled before the run and nothing has landed anywhere yet.
    cpus: bool,
}

impl Columns {
    /// The keys and tags these steps carry, each in sorted order. Commands and
    /// closures share the columns, since they share the table.
    fn of(steps: &[Step]) -> Columns {
        let mut keys = BTreeSet::new();
        let mut tags = BTreeSet::new();
        let mut cpus = false;

        for item in steps.iter().flat_map(Step::items) {
            keys.extend(item.fields().keys().cloned());
            tags.extend(item.tags().iter().cloned());
            cpus |= item.cores() > 0;
        }

        Columns {
            keys: keys.into_iter().collect(),
            tags: tags.into_iter().collect(),
            cpus,
        }
    }

    fn header(&self) -> Vec<String> {
        let mut header: Vec<String> = vec!["step".into(), "cmd".into()];
        header.extend(self.keys.iter().cloned());
        header.extend(self.tags.iter().cloned());
        header.extend(METRICS.iter().map(|s| s.to_string()));
        if self.cpus {
            header.insert(header.len() - 1, "cpus".into());
        }
        header
    }

    /// Slots the cpus cell into a row that is otherwise finished, second to
    /// last. It sits there rather than out with the fields because the width
    /// reserved for it is only a guess, and everything a wider one shifts along
    /// is then just argv, which is last and unpadded anyway.
    fn put_cpus(&self, cells: &mut Vec<Cell>, cpus: Option<&[usize]>) {
        if !self.cpus {
            return;
        }

        // empty rather than absent means it asked for cpus and never got as far
        // as holding any, which reads the same as having none to report
        let text = match cpus {
            Some([]) | None => dash(),
            Some(cpus) => crate::cpu::list(cpus),
        };
        cells.insert(cells.len() - 1, Cell::left(text));
    }

    /// One step's rows: its own line, then a line per command.
    fn block(&self, step: &Step) -> Vec<Vec<Cell>> {
        let mut rows = Vec::new();

        // a step of one would just repeat itself, so it gets no line of its own
        // and keeps the first column instead, with its command filling in the rest
        let first = if step.items().count() == 1 {
            Cell::left(step.label())
        } else {
            rows.push(self.step_row(step));
            Cell::right(match step.strategy() {
                Some(Strategy::Batched { .. }) => BATCH,
                _ => SERIAL,
            })
        };

        for item in step.items() {
            rows.push(self.row(first.clone(), item));
        }
        rows
    }

    /// How wide each column has to be for every block to fit under one header.
    ///
    /// Everything but the numbers is already known before the run, and the
    /// headings above the numbers are wider than the numbers usually are.
    fn measure(&self, steps: &[Step]) -> Vec<usize> {
        let header = self.header();
        let mut widths = vec![0; header.len()];

        widen(&mut widths, &head_cells(&header));
        for step in steps {
            for row in self.block(step) {
                widen(&mut widths, &row);
            }
        }

        // every cpus cell above still says `-`, since nothing has been given
        // any cpus yet. how many each command gets is settled though, so the
        // column can be sized for the widest of those rather than for a dash
        if self.cpus {
            let at = widths.len() - 2;
            let most = steps
                .iter()
                .flat_map(Step::items)
                .map(|item| crate::cpu::list_width(item.cores()))
                .max()
                .unwrap_or(0);
            widths[at] = widths[at].max(most);
        }

        widths
    }
}

/// What one row has to say about what something cost and how it went. Anything
/// left out prints as `-`, which is how a command that never started says it has
/// no numbers.
#[derive(Default)]
struct Metrics {
    wall_s: Option<f64>,
    user_s: Option<f64>,
    sys_s: Option<f64>,
    max_rss_kb: Option<i64>,
    exit: Option<i32>,
    status: Option<&'static str>,
    argv: Option<String>,
}

impl Metrics {
    /// The cells, in the order [`METRICS`] names them. A column added to one
    /// without the other does not compile.
    fn cells(self) -> [Cell; METRICS.len()] {
        let cpu = match (self.user_s, self.sys_s) {
            (Some(user), Some(sys)) => cpu_pct(user + sys, self.wall_s),
            _ => dash(),
        };

        [
            Cell::left(secs(self.wall_s)),
            Cell::left(secs(self.user_s)),
            Cell::left(secs(self.sys_s)),
            Cell::left(cpu),
            Cell::left(self.max_rss_kb.map(bytes).unwrap_or_else(dash)),
            Cell::left(self.exit.map(|e| e.to_string()).unwrap_or_else(dash)),
            Cell::left(self.status.unwrap_or("-")),
            Cell::left(self.argv.unwrap_or_else(dash)),
        ]
    }
}

impl Columns {
    /// The step's own line: measured wall clock, and its commands' CPU added up.
    fn step_row(&self, step: &Step) -> Vec<Cell> {
        let mut cells = vec![Cell::left(step.label()), Cell::left("-")];
        cells.extend(std::iter::repeat_n(
            Cell::left("-"),
            self.keys.len() + self.tags.len(),
        ));

        let timings: Vec<&Timing> = step
            .items()
            .filter_map(|item| item.status().timing())
            .collect();

        // exit, status and argv belong to commands; a count or a rollup here
        // would be a different quantity sharing a column
        let mut metrics = Metrics {
            wall_s: step.wall_s(),
            ..Metrics::default()
        };

        if !timings.is_empty() {
            // summed CPU against measured wall is what shows whether a batch
            // actually bought anything: a step that ran four at once reads about
            // four times what any one of them did
            //
            // a closure has no cpu figure, and a sum over only what we did
            // measure would read as the step's whole cost. std's Sum for Option
            // gives up on the total instead, which is the honest answer
            metrics.user_s = timings.iter().map(|t| t.user_s).sum();
            metrics.sys_s = timings.iter().map(|t| t.sys_s).sum();
            // the largest any one process got, which is not the same as the most
            // the step held at once — wait4 cannot tell us that
            // a max, unlike a sum, is not spoiled by one with no number at all
            metrics.max_rss_kb = timings.iter().filter_map(|t| t.max_rss_kb).max();
        }

        cells.extend(metrics.cells());
        // the cpus a whole step held is not the cpus any one command held, so
        // like exit and argv beside it, the step line leaves it alone
        self.put_cpus(&mut cells, None);
        cells
    }

    /// One command's line. `first` is the step name for a collapsed step of one,
    /// and a right-aligned `|` or `||` otherwise.
    /// One item's line, whichever kind it is. The columns a closure has no
    /// answer for come back `None` from [`Item`] and print as `-`.
    fn row(&self, first: Cell, item: Item<'_>) -> Vec<Cell> {
        let mut cells = vec![first, Cell::left(item.label())];
        cells.extend(self.key_cells(item.fields(), item.tags()));

        // two separate questions: what it cost, and how it went. one that could
        // not start has nothing to say about the first
        let t = item.status().timing();
        cells.extend(
            Metrics {
                wall_s: t.map(|t| t.wall_s),
                user_s: t.and_then(|t| t.user_s),
                sys_s: t.and_then(|t| t.sys_s),
                max_rss_kb: t.and_then(|t| t.max_rss_kb),
                exit: item.exit(),
                status: Some(status_word(item.status())),
                argv: item.line(),
            }
            .cells(),
        );
        self.put_cpus(&mut cells, item.cpus());
        cells
    }

    /// The field and tag cells, which every row carries the same way.
    fn key_cells(&self, fields: &BTreeMap<String, String>, tags: &BTreeSet<String>) -> Vec<Cell> {
        let mut cells: Vec<Cell> = self
            .keys
            .iter()
            .map(|k| Cell::left(fields.get(k).map(String::as_str).unwrap_or("-")))
            .collect();
        cells.extend(
            self.tags
                .iter()
                .map(|t| Cell::left(if tags.contains(t) { "x" } else { "-" })),
        );
        cells
    }
}

/// The status column. It answers one question — did this work — and leaves the
/// exit column to say what the command actually reported, which is also how
/// "never started" tells itself apart from "started and failed" without a word
/// of its own: there is no exit code beside it.
fn status_word(status: &Status) -> &'static str {
    match status {
        Status::NotRun => "-",
        Status::Skipped => "skip",
        Status::TimedOut(_) => "time",
        Status::Finished(t) if t.ok() => "ok",
        _ => "fail",
    }
}

/// A cell and which side its padding goes on.
#[derive(Clone, Debug)]
struct Cell {
    text: String,
    right: bool,
}

impl Cell {
    fn left(text: impl Into<String>) -> Cell {
        Cell {
            text: text.into(),
            right: false,
        }
    }

    fn right(text: impl Into<String>) -> Cell {
        Cell {
            text: text.into(),
            right: true,
        }
    }
}

/// Lay out the rows, every column padded to its widest cell, under a commented
/// header and dashed separator if `show_header` says so.
///
/// The header sets the width of every column whether it is printed or not. That
/// is what keeps blocks lined up under a header printed once at the top: they
/// share its widths as a floor, and only drift apart where a value is wider than
/// the label above it.
fn render(
    header: &[String],
    rows: &[Vec<Cell>],
    show_header: bool,
    floor: Option<&[usize]>,
) -> String {
    let head = head_cells(header);

    // a floor from a different set of columns is no floor at all
    let mut widths = match floor.filter(|floor| floor.len() == header.len()) {
        Some(floor) => floor.to_vec(),
        None => vec![0; header.len()],
    };
    widen(&mut widths, &head);
    for row in rows {
        widen(&mut widths, row);
    }

    let mut out = String::new();
    if show_header {
        let last = header.len() - 1;
        let mut sep: Vec<Cell> = widths.iter().map(|w| Cell::left("-".repeat(*w))).collect();
        sep[0] = Cell::left(format!("# {}", "-".repeat(widths[0].saturating_sub(2))));
        // argv is unpadded, so underline the label rather than the whole column
        sep[last] = Cell::left("-".repeat(header[last].chars().count()));

        write_row(&mut out, &head, &widths);
        write_row(&mut out, &sep, &widths);
    }
    for row in rows {
        write_row(&mut out, row, &widths);
    }
    out
}

/// The header as cells. The "# " marker is absorbed into the first column's
/// width, so the labels stay lined up over the data below them.
fn head_cells(header: &[String]) -> Vec<Cell> {
    let mut head: Vec<Cell> = header.iter().map(Cell::left).collect();
    head[0] = Cell::left(format!("# {}", header[0]));
    head
}

fn widen(widths: &mut [usize], cells: &[Cell]) {
    for (i, cell) in cells.iter().enumerate() {
        widths[i] = widths[i].max(cell.text.chars().count());
    }
}

fn write_row(out: &mut String, cells: &[Cell], widths: &[usize]) {
    let last = cells.len() - 1;
    for (i, cell) in cells.iter().enumerate() {
        if i == last {
            out.push_str(&cell.text);
            break;
        }
        let pad = " ".repeat(widths[i].saturating_sub(cell.text.chars().count()));
        if cell.right {
            out.push_str(&pad);
            out.push_str(&cell.text);
        } else {
            out.push_str(&cell.text);
            out.push_str(&pad);
        }
        out.push(' ');
    }
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::closure::Closure;
    use crate::cmd::{Cmd, Output};
    use crate::step::OnError;

    /// Fixed numbers so a rendered table is the same every time: 2.25s of CPU
    /// over 1.5s of wall is 150%, and 2048 KiB is 2.00MiB.
    fn timing(exit: i32) -> Timing {
        Timing {
            wall_s: 1.5,
            user_s: Some(2.0),
            sys_s: Some(0.25),
            max_rss_kb: Some(2048),
            exit,
        }
    }

    fn cmd(program: &str, name: &str) -> Cmd {
        Cmd::new(program)
            .name(name)
            .stdout(Output::Inherit)
            .stderr(Output::Inherit)
    }

    fn finish(step: &mut Step, index: usize) {
        step.index = Some(index);
        step.elapsed_s = Some(1.5);
        for cmd in step.cmds_mut() {
            cmd.status = Status::Finished(timing(0));
        }
        for closure in step.closures_mut() {
            // a closure only ever has a wall clock behind it
            closure.status = Status::Finished(Timing {
                user_s: None,
                sys_s: None,
                max_rss_kb: None,
                ..timing(0)
            });
        }
    }

    /// A step of one that collapses, then a batch of two carrying a field.
    fn steps() -> Vec<Step> {
        let mut setup = Step::serial([cmd("/mkdir", "mkdir")]).name("setup");
        finish(&mut setup, 1);

        let mut burn = Step::batched(
            2,
            [
                cmd("/a", "a").field("job", 1),
                cmd("/b", "b").field("job", 2),
            ],
        )
        .name("burn");
        finish(&mut burn, 2);

        vec![setup, burn]
    }

    /// A path of this test's own. Tests share a process and run at once, so a
    /// shared one would have them deleting each other's output.
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("pipeline-table-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        dir.join("runs.tbl")
    }

    /// Drive the sink the way a pipeline would, and give back what it wrote.
    fn write(name: &str, mode: Mode, steps: &[Step]) -> String {
        let path = scratch(name);

        let mut table = Table::new(&path).mode(mode);
        table.start(steps).unwrap();
        for step in steps {
            table.step_done(step).unwrap();
        }
        table.finish().unwrap();

        std::fs::read_to_string(&path).unwrap()
    }

    fn header_lines(text: &str) -> usize {
        text.lines().filter(|l| l.starts_with("# step")).count()
    }

    #[test]
    fn a_block_lays_out_under_its_header() {
        let expected = "\
# step     cmd   job wall(s) user(s) sys(s) cpu(%) max_rss exit status argv
# -------- ----- --- ------- ------- ------ ------ ------- ---- ------ ----
[1](setup) mkdir -   1.50    2.00    0.25   150%   2.00MiB 0    ok     /mkdir
[2](burn)  -     -   1.50    4.00    0.50   300%   2.00MiB -    -      -
        || a     1   1.50    2.00    0.25   150%   2.00MiB 0    ok     /a
        || b     2   1.50    2.00    0.25   150%   2.00MiB 0    ok     /b
";
        assert_eq!(write("golden", Mode::default(), &steps()), expected);
    }

    #[test]
    fn a_step_of_one_keeps_the_first_column_instead_of_a_line_of_its_own() {
        let text = write("collapse", Mode::default(), &steps());
        let rows: Vec<&str> = text.lines().skip(2).collect();

        assert_eq!(rows.len(), 4, "one collapsed step plus a step row and two commands");
        assert!(rows[0].starts_with("[1](setup) mkdir"), "{}", rows[0]);
        assert!(rows[1].starts_with("[2](burn)  -"), "{}", rows[1]);
    }

    #[test]
    fn a_batch_marks_its_commands_differently_from_a_serial_one() {
        let mut serial = Step::serial([cmd("/a", "a"), cmd("/b", "b")]).name("s");
        finish(&mut serial, 1);
        let mut batched = Step::batched(2, [cmd("/a", "a"), cmd("/b", "b")]).name("b");
        finish(&mut batched, 2);

        let text = write("markers", Mode::default(), &[serial, batched]);
        assert_eq!(text.matches(" | ").count(), 2, "{text}");
        assert_eq!(text.matches("|| ").count(), 2, "{text}");
    }

    #[test]
    fn a_step_that_never_ran_has_no_numbers_to_report() {
        let mut step = Step::serial([cmd("/a", "a"), cmd("/b", "b")]).name("s");
        step.index = Some(1);

        let text = write("never-ran", Mode::default(), &[step]);
        let step_row = text.lines().nth(2).unwrap();

        assert!(step_row.starts_with("[1](s)"), "{step_row}");
        assert!(
            !step_row.contains('%'),
            "no command finished, so there is no cpu figure: {step_row}"
        );
    }

    #[test]
    fn blocks_with_one_header_writes_it_before_anything_runs() {
        let path = scratch("early");

        let mut table = Table::new(&path).mode(Mode::default());
        table.start(&steps()).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(header_lines(&text), 1);
        assert_eq!(text.lines().count(), 2, "header and separator, no rows yet");
    }

    #[test]
    fn one_header_serves_every_block() {
        assert_eq!(header_lines(&write("one-header", Mode::default(), &steps())), 1);
    }

    #[test]
    fn a_header_on_every_block_means_one_per_step() {
        let steps = steps();
        let text = write(
            "each-header",
            Mode::Blocks {
                headers: Headers::Each,
            },
            &steps,
        );
        assert_eq!(header_lines(&text), steps.len());
    }

    #[test]
    fn whole_holds_everything_back_until_the_run_is_over() {
        let path = scratch("whole");
        let steps = steps();

        let mut table = Table::new(&path).mode(Mode::Whole);
        table.start(&steps).unwrap();
        for step in &steps {
            table.step_done(step).unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "",
            "nothing should reach the file before finish"
        );

        table.finish().unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(header_lines(&text), 1);
        assert_eq!(text.lines().count(), 6);
    }

    #[test]
    fn ragged_gives_each_block_only_the_columns_its_own_commands_use() {
        let text = write("ragged", Mode::Ragged, &steps());
        let heads: Vec<&str> = text.lines().filter(|l| l.starts_with("# step")).collect();

        assert_eq!(heads.len(), 2, "ragged blocks each need their own header");
        assert!(!heads[0].contains("job"), "setup has no fields: {}", heads[0]);
        assert!(heads[1].contains("job"), "burn does: {}", heads[1]);
    }

    #[test]
    fn every_block_shares_the_header_widths_as_a_floor() {
        // the second step's names are shorter, but its columns stay lined up
        // with the first block rather than closing up
        let text = write("floor", Mode::default(), &steps());
        let lines: Vec<&str> = text.lines().collect();

        let column_of = |line: &str, n: usize| line.match_indices("1.50").nth(n).map(|(i, _)| i);
        assert_eq!(column_of(lines[2], 0), column_of(lines[3], 0));
        assert_eq!(column_of(lines[2], 0), column_of(lines[4], 0));
    }

    #[test]
    fn a_value_wider_than_its_heading_widens_the_column() {
        let mut step = Step::serial([cmd("/x", "an-unusually-long-command-name")]).name("s");
        finish(&mut step, 1);

        let text = write("wide", Mode::default(), &[step]);
        let lines: Vec<&str> = text.lines().collect();
        let head = lines[0].find("cmd").unwrap();

        assert!(
            lines[2][head..].starts_with("an-unusually-long-command-name"),
            "{}",
            lines[2]
        );
        assert!(lines[2].contains("1.50"), "the numbers still follow: {}", lines[2]);
    }

    #[test]
    fn argv_is_the_last_column_and_never_padded() {
        let text = write("argv", Mode::default(), &steps());
        for line in text.lines().skip(2) {
            assert_eq!(line.trim_end(), line, "trailing pad on: {line:?}");
        }
        // the separator underlines the label rather than the whole column
        assert!(text.lines().nth(1).unwrap().ends_with("----"));
    }

    #[test]
    fn columns_are_sorted_and_asked_for_only_once() {
        let step = Step::serial([
            cmd("/a", "a").field("zed", 1).field("alpha", 2).tag("slow"),
            cmd("/b", "b").field("alpha", 3).tag("slow").tag("first"),
        ]);
        let columns = Columns::of(&[step]);

        assert_eq!(columns.keys, ["alpha", "zed"]);
        assert_eq!(columns.tags, ["first", "slow"]);
    }

    #[test]
    fn a_tag_reads_as_present_or_absent_rather_than_as_a_value() {
        let mut step = Step::serial([cmd("/a", "a").tag("setup"), cmd("/b", "b")]).name("s");
        finish(&mut step, 1);

        let text = write("tags", Mode::default(), &[step]);
        let lines: Vec<&str> = text.lines().collect();
        let at = lines[0].find("setup").unwrap();

        assert_eq!(&lines[3][at..at + 1], "x");
        assert_eq!(&lines[4][at..at + 1], "-");
    }

    #[test]
    fn a_row_with_nothing_measured_is_all_dashes() {
        let cells = Metrics::default().cells();
        assert_eq!(cells.len(), METRICS.len());
        for cell in &cells {
            assert_eq!(cell.text, "-");
        }
    }

    #[test]
    fn status_and_exit_answer_different_questions() {
        // a command that never started has no exit code beside its "fail", which
        // is how it tells itself apart from one that started and failed
        assert_eq!(status_word(&Status::NotRun), "-");
        assert_eq!(status_word(&Status::Skipped), "skip");
        assert_eq!(status_word(&Status::Failed("x".into())), "fail");
        assert_eq!(status_word(&Status::TimedOut(timing(143))), "time");
        assert_eq!(status_word(&Status::Finished(timing(0))), "ok");
        assert_eq!(status_word(&Status::Finished(timing(1))), "fail");
    }

    #[test]
    fn a_skipped_step_still_gets_a_row() {
        let mut step = Step::serial([cmd("/a", "a")])
            .name("never")
            .on_error(OnError::Continue);
        step.index = Some(1);
        step.cmds_mut()[0].status = Status::Skipped;

        let text = write("skipped", Mode::default(), &[step]);
        let row = text.lines().nth(2).unwrap();

        assert!(row.starts_with("[1](never) a"), "{row}");
        assert!(row.contains("skip"), "{row}");
    }

    #[test]
    fn a_step_of_one_closure_collapses_and_shares_its_columns_with_commands() {
        let mut setup = Step::serial([cmd("/mkdir", "mkdir").field("job", 1)]).name("setup");
        finish(&mut setup, 1);

        // `shard` is the closure's alone, so the column can only come from it;
        // `job` is shared, so the two have to agree about where it sits
        let mut check = Step::from_closures([Closure::new("verify", || Ok(()))
            .field("job", 2)
            .field("shard", 7)])
        .name("check");
        finish(&mut check, 2);

        // the closure row carries a wall clock and a verdict, and dashes every
        // column a thread has no answer for
        let expected = "\
# step     cmd    job shard wall(s) user(s) sys(s) cpu(%) max_rss exit status argv
# -------- ------ --- ----- ------- ------- ------ ------ ------- ---- ------ ----
[1](setup) mkdir  1   -     1.50    2.00    0.25   150%   2.00MiB 0    ok     /mkdir
[2](check) verify 2   7     1.50    -       -      -      -       -    ok     -
";

        assert_eq!(
            write("closure-collapse", Mode::default(), &[setup, check]),
            expected
        );
    }

    #[test]
    fn a_step_of_two_closures_gets_a_line_of_its_own() {
        let mut step = Step::from_closures([
            Closure::new("first", || Ok(())),
            Closure::new("second", || Ok(())),
        ])
        .name("check");
        finish(&mut step, 1);

        let text = write("closure-block", Mode::default(), &[step]);
        let rows: Vec<&str> = text.lines().skip(2).collect();

        assert_eq!(rows.len(), 3, "a step row and two closures: {rows:?}");
        assert!(rows[0].starts_with("[1](check)"), "{}", rows[0]);
        // serial, so the same marker a serial command step gets
        assert!(rows[1].trim_start().starts_with("| first"), "{}", rows[1]);
        assert!(rows[2].trim_start().starts_with("| second"), "{}", rows[2]);

        // the step's own row has a wall clock but no cpu to add up
        let step_row = rows[0];
        assert!(step_row.contains("1.50"), "{step_row}");
    }
}
