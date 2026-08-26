//! What a run is doing, as it does it.
//!
//! By default this keeps a block at the bottom of the terminal showing the step
//! that is going, what is running inside it and for how long, with the finished
//! lines scrolling above it in color. Turn the parts off and it becomes a plain
//! line per result, which is what a log wants.
//!
//! The spinning is done by a thread of its own: [`Sink`] has no tick, and it
//! should not — a tick carries no information. What the trait does give it is
//! [`item_start`](Sink::item_start), and knowing when something *began* is
//! enough to animate the rest without the pipeline calling in.
//!
//! This writes to stdout unless told otherwise: it is what the run produced,
//! not a note about it.

use std::io::{IsTerminal, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::execute::Status;
use crate::fmt::{bytes, dash};
use crate::item::Item;
use crate::sink::Sink;
use crate::step::Step;

/// How often the live block is redrawn. Fast enough to look like it is moving,
/// slow enough that a run pinned to every core does not notice.
const FRAME: Duration = Duration::from_millis(80);

/// How many running items to name before summing up the rest. A batch fifty
/// wide would otherwise push everything else off the screen.
const SHOWN: usize = 8;

/// Names are cut to this so a live line cannot wrap. A wrapped line takes up
/// two rows and the erase would leave half of it behind.
const NAME: usize = 32;

/// Erase the whole line and put the cursor back at the start of it.
const CLEAR: &str = "\r\x1b[2K";
/// Up one line, then erase that one too.
const UP: &str = "\x1b[1A\x1b[2K";
const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";

const GREEN: &str = "32";
const RED: &str = "31";
const YELLOW: &str = "33";
const CYAN: &str = "36";
const DIM: &str = "2";
const BOLD: &str = "1";

/// Whether to do something that only a terminal can make sense of.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum When {
    /// Only when writing to a terminal.
    #[default]
    Auto,
    Always,
    Never,
}

/// What goes in front of a finished line to say how it went.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Marks {
    /// `✓ ✗ ! ·`, and braille for the spinner.
    #[default]
    Unicode,
    /// `+ x ! -`, for a terminal that would make a mess of the above.
    Ascii,
    /// No mark at all. The verdict moves to the end of the line as a word, so
    /// that something still says a command worked.
    None,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Stream {
    #[default]
    Stdout,
    Stderr,
}

/// What a run is doing, printed as it happens.
///
/// ```no_run
/// # use pail::{Marks, PipelineBuilder, Progress, When};
/// // the default: live, in color, when there is a terminal to draw on
/// PipelineBuilder::new().sink(Progress::new());
///
/// // a plain line per result, for a log
/// PipelineBuilder::new().sink(
///     Progress::new()
///         .marks(Marks::None)
///         .color(When::Never)
///         .rewrite(When::Never),
/// );
/// ```
pub struct Progress {
    shared: Arc<Shared>,
    /// Held so the run can join it. `None` before the run starts and after it
    /// has been waited for.
    spinning: Option<JoinHandle<()>>,
    /// What was asked for. Turned into plain yes-or-no in `start`, which is the
    /// first point at which the stream is known to be worth asking about.
    asked: Asked,
}

#[derive(Clone, Copy, Default)]
struct Asked {
    marks: Marks,
    color: When,
    rewrite: When,
    stream: Stream,
}

struct Shared {
    state: Mutex<State>,
    /// Woken when the run is over, so the spinning thread stops at once rather
    /// than after one more frame of sleeping.
    changed: Condvar,
}

/// What one step looks like before it has run: enough to draw a live line for
/// it without waiting for its first result.
struct Planned {
    label: String,
    items: usize,
}

/// Something that has begun and not yet reported.
struct Running {
    /// Its place in the step, which is what tells it apart from another with
    /// the same name — two unnamed commands running one program share one.
    at: usize,
    name: String,
    since: Instant,
}

struct State {
    steps: Vec<Planned>,
    /// Which step is going. Set by `step_start` rather than counted off by
    /// `step_done`, so the label and the count below it always belong to the
    /// same step — advancing on the way out would leave the next step's name
    /// above the last one's tally until it started.
    at: usize,
    /// How many steps have started, which is where the next one goes.
    started: usize,
    done_here: usize,
    done_total: usize,
    total: usize,
    ok: usize,
    failed: usize,
    skipped: usize,
    /// Whether this step has had its name printed above its results yet.
    titled: bool,
    running: Vec<Running>,
    began: Instant,
    step_began: Instant,
    frame: usize,
    over: bool,
    /// Why the run was given up on, if it was.
    why: Option<String>,
    /// How many rows the live block last took up, so it can be erased.
    drawn: usize,
    marks: Marks,
    color: bool,
    rewrite: bool,
    stream: Stream,
}

impl Default for Progress {
    fn default() -> Progress {
        Progress::new()
    }
}

impl Progress {
    pub fn new() -> Progress {
        Progress {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    steps: Vec::new(),
                    at: 0,
                    started: 0,
                    done_here: 0,
                    done_total: 0,
                    total: 0,
                    ok: 0,
                    failed: 0,
                    skipped: 0,
                    titled: false,
                    running: Vec::new(),
                    began: Instant::now(),
                    step_began: Instant::now(),
                    frame: 0,
                    over: false,
                    why: None,
                    drawn: 0,
                    marks: Marks::default(),
                    color: false,
                    rewrite: false,
                    stream: Stream::default(),
                }),
                changed: Condvar::new(),
            }),
            spinning: None,
            asked: Asked::default(),
        }
    }

    pub fn marks(mut self, marks: Marks) -> Self {
        self.asked.marks = marks;
        self
    }

    /// Color is turned off by `NO_COLOR` or `TERM=dumb` whatever this says:
    /// asking for it in code is not a reason to override the reader.
    pub fn color(mut self, when: When) -> Self {
        self.asked.color = when;
        self
    }

    /// Whether to keep rewriting a block at the bottom of the screen. Off means
    /// one line per result and no spinner, which is what a redirected log wants.
    pub fn rewrite(mut self, when: When) -> Self {
        self.asked.rewrite = when;
        self
    }

    pub fn stream(mut self, stream: Stream) -> Self {
        self.asked.stream = stream;
        self
    }

    /// Stop the spinning thread and wait for it, so nothing draws over what is
    /// printed next. Doing it twice is harmless, which `Drop` relies on.
    fn stop(&mut self) {
        self.shared.state.lock().unwrap().over = true;
        self.shared.changed.notify_all();
        if let Some(spinning) = self.spinning.take() {
            let _ = spinning.join();
        }
    }
}

impl Sink for Progress {
    fn start(&mut self, steps: &[Step]) -> anyhow::Result<()> {
        let mut state = self.shared.state.lock().unwrap();

        // asked of the stream this actually writes to, so a redirect is seen
        let tty = match self.asked.stream {
            Stream::Stdout => std::io::stdout().is_terminal(),
            Stream::Stderr => std::io::stderr().is_terminal(),
        };
        let plain = std::env::var_os("NO_COLOR").is_some()
            || std::env::var_os("TERM").is_some_and(|term| term == "dumb");

        state.marks = self.asked.marks;
        state.stream = self.asked.stream;
        state.color = !plain && self.asked.color.yes(tty);
        state.rewrite = self.asked.rewrite.yes(tty);

        state.steps = steps
            .iter()
            .map(|step| Planned {
                label: step.label(),
                items: step.items().count(),
            })
            .collect();
        state.total = state.steps.iter().map(|s| s.items).sum();
        state.began = Instant::now();
        state.step_began = Instant::now();

        if state.rewrite {
            let text = HIDE_CURSOR.to_string();
            state.put(&text);
        }

        let opening = format!("{} steps, {} to run", state.steps.len(), state.total);
        let opening = state.paint(DIM, &opening);
        state.emit(&opening);
        let rewrite = state.rewrite;
        drop(state);

        // the pipeline never calls in to say time has passed, so the spinning
        // has to come from somewhere else. nothing to spin where nothing is
        // being rewritten, so nothing to run either
        if rewrite {
            let shared = Arc::clone(&self.shared);
            self.spinning = Some(std::thread::spawn(move || spin(&shared)));
        }

        Ok(())
    }

    fn step_start(&mut self, _step: &Step) -> anyhow::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        state.at = state.started;
        state.started += 1;
        state.step_began = Instant::now();
        state.done_here = 0;
        state.titled = false;
        state.running.clear();
        state.draw();
        Ok(())
    }

    fn item_start(&mut self, _step: &Step, at: usize, item: Item<'_>) -> anyhow::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        state.running.push(Running {
            at,
            name: item.label(),
            since: Instant::now(),
        });
        state.draw();
        Ok(())
    }

    fn item_done(&mut self, step: &Step, at: usize, item: Item<'_>) -> anyhow::Result<()> {
        let mut state = self.shared.state.lock().unwrap();

        // by position rather than by name: names repeat, positions do not
        if let Some(which) = state.running.iter().position(|r| r.at == at) {
            state.running.remove(which);
        }

        let name = item.label();
        match item.status() {
            Status::Skipped | Status::NotRun => state.skipped += 1,
            status if status.failed() => state.failed += 1,
            _ => state.ok += 1,
        }
        state.done_here += 1;
        state.done_total += 1;

        if !state.titled {
            state.titled = true;
            let title = state.paint(BOLD, &step.label());
            state.emit(&title);
        }

        let line = state.line(&name, item.status());
        state.emit(&line);

        // only if it is still there — a failure that said nothing has had its
        // file cleaned up already
        if item.status().failed()
            && let Some(path) = item.stderr_path()
            && path.exists()
        {
            let line = state.paint(DIM, &format!("      {}", path.display()));
            state.emit(&line);
        }

        Ok(())
    }

    fn step_done(&mut self, _step: &Step) -> anyhow::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        // `at` stays put: until the next step starts, the step that just ended
        // is still the one worth showing, sitting at its full count
        state.running.clear();
        state.draw();
        Ok(())
    }

    fn abandoned(&mut self, why: &str) -> anyhow::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        state.why = Some(why.to_string());
        let line = state.paint(RED, &format!("  giving up: {why}"));
        state.emit(&line);
        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.stop();

        let mut state = self.shared.state.lock().unwrap();
        state.wipe();

        let counts = format!(
            "{} ok, {} failed, {} skipped in {}",
            state.ok,
            state.failed,
            state.skipped,
            span(state.began.elapsed())
        );
        let color = if state.failed > 0 || state.why.is_some() {
            RED
        } else {
            GREEN
        };
        let summary = state.paint(color, &counts);
        state.emit(&summary);

        Ok(())
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        // a run that never reached `finish` — a panic on the way out, say —
        // would otherwise leave the terminal with no cursor in it
        self.stop();
        if let Ok(mut state) = self.shared.state.lock() {
            state.wipe();
        }
    }
}

impl When {
    fn yes(self, tty: bool) -> bool {
        match self {
            When::Auto => tty,
            When::Always => true,
            When::Never => false,
        }
    }
}

impl Marks {
    /// The mark for a status, and the color to paint it.
    fn of(self, status: &Status) -> Option<(&'static str, &'static str)> {
        let (unicode, ascii, color) = match status {
            Status::NotRun | Status::Skipped => ("·", ".", DIM),
            Status::TimedOut(_) => ("!", "!", YELLOW),
            status if status.failed() => ("✗", "x", RED),
            _ => ("✓", "+", GREEN),
        };

        match self {
            Marks::None => None,
            Marks::Unicode => Some((unicode, color)),
            Marks::Ascii => Some((ascii, color)),
        }
    }

    fn spinner(self, frame: usize) -> &'static str {
        const BRAILLE: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        const BARS: [&str; 4] = ["|", "/", "-", "\\"];
        match self {
            Marks::Unicode => BRAILLE[frame % BRAILLE.len()],
            _ => BARS[frame % BARS.len()],
        }
    }

    /// A name short enough that the line it sits on cannot wrap.
    fn cut(self, name: &str) -> String {
        if name.chars().count() <= NAME {
            return name.to_string();
        }
        match self {
            Marks::Unicode => name.chars().take(NAME - 1).chain(['…']).collect(),
            _ => name.chars().take(NAME - 3).chain(['.', '.', '.']).collect(),
        }
    }
}

/// Redraw the live block until the run says it is over.
fn spin(shared: &Shared) {
    loop {
        let state = shared.state.lock().unwrap();
        let (mut state, _) = shared
            .changed
            .wait_timeout_while(state, FRAME, |state| !state.over)
            .unwrap();

        if state.over {
            return;
        }
        state.frame += 1;
        state.draw();
    }
}

impl State {
    fn paint(&self, code: &str, text: &str) -> String {
        match self.color {
            true => format!("\x1b[{code}m{text}\x1b[0m"),
            false => text.to_string(),
        }
    }

    /// A permanent line, printed above the live block.
    fn emit(&mut self, line: &str) {
        let mut text = String::new();
        self.erase_into(&mut text);
        text.push_str(line.trim_end());
        text.push('\n');
        self.live_into(&mut text);
        self.put(&text);
    }

    /// Redraw the live block in place.
    fn draw(&mut self) {
        if !self.rewrite {
            return;
        }
        let mut text = String::new();
        self.erase_into(&mut text);
        self.live_into(&mut text);
        self.put(&text);
    }

    /// Take the live block away and put the cursor back.
    fn wipe(&mut self) {
        if !self.rewrite {
            return;
        }
        let mut text = String::new();
        self.erase_into(&mut text);
        text.push_str(SHOW_CURSOR);
        self.put(&text);
    }

    /// Flushed rather than left to the buffer, because a stream that is not a
    /// terminal holds its output back in blocks, and a benchmark that runs for
    /// an hour is worth being able to watch through a pipe.
    fn put(&self, text: &str) {
        let mut out: Box<dyn Write> = match self.stream {
            Stream::Stdout => Box::new(std::io::stdout().lock()),
            Stream::Stderr => Box::new(std::io::stderr().lock()),
        };
        let _ = out.write_all(text.as_bytes());
        let _ = out.flush();
    }

    /// Wind back over however many rows the block last took, clearing each.
    fn erase_into(&mut self, text: &mut String) {
        if !self.rewrite {
            return;
        }
        text.push_str(CLEAR);
        for _ in 1..self.drawn {
            text.push_str(UP);
        }
        self.drawn = 0;
    }

    fn live_into(&mut self, text: &mut String) {
        if !self.rewrite {
            return;
        }
        let lines = self.live();
        text.push_str(&lines.join("\n"));
        self.drawn = lines.len();
    }

    /// The block that keeps being rewritten: the step that is going, then a
    /// line for each thing running inside it.
    fn live(&self) -> Vec<String> {
        let Some(step) = self.steps.get(self.at) else {
            return Vec::new();
        };
        if self.over {
            return Vec::new();
        }

        let spinner = self.marks.spinner(self.frame);
        let overall = self.paint(
            DIM,
            &format!(
                "· {}/{} total  {}",
                self.done_total,
                self.total,
                span(self.began.elapsed())
            ),
        );

        let mut lines = vec![format!(
            "{} {}  {}/{}  {}  {overall}",
            self.paint(CYAN, spinner),
            self.paint(BOLD, &step.label),
            self.done_here,
            step.items,
            span(self.step_began.elapsed())
        )];

        for running in self.running.iter().take(SHOWN) {
            lines.push(format!(
                "   {} {:<NAME$}  {}",
                self.paint(CYAN, spinner),
                self.marks.cut(&running.name),
                self.paint(DIM, &span(running.since.elapsed()))
            ));
        }
        if self.running.len() > SHOWN {
            let rest = format!("   ... and {} more", self.running.len() - SHOWN);
            lines.push(self.paint(DIM, &rest));
        }

        lines
    }

    /// One finished thing, and how it went.
    fn line(&self, name: &str, status: &Status) -> String {
        let mark = self.marks.of(status);

        let verdict = match status {
            Status::TimedOut(_) => "timed out".to_string(),
            // with no mark in front, something still has to say it worked
            _ => match status.timing() {
                Some(t) if t.ok() => match mark {
                    Some(_) => String::new(),
                    None => "ok".to_string(),
                },
                Some(t) => format!("exit {}", t.exit),
                None => String::new(),
            },
        };

        let detail = match (status, status.timing()) {
            (Status::NotRun, _) => "not run".to_string(),
            (Status::Skipped, _) => "skipped".to_string(),
            (Status::Failed(why), _) => why.clone(),
            (_, Some(t)) => format!(
                "{:>8.2}s {:>9}  {verdict}",
                t.wall_s,
                t.max_rss_kb.map(bytes).unwrap_or_else(dash)
            ),
            (_, None) => dash(),
        };

        let dimmed = matches!(status, Status::NotRun | Status::Skipped);
        let name = match dimmed {
            true => self.paint(DIM, &format!("{name:<NAME$}")),
            false => format!("{name:<NAME$}"),
        };

        match mark {
            Some((mark, color)) => format!("  {} {name} {detail}", self.paint(color, mark)),
            None => format!("  {name} {detail}"),
        }
    }
}

/// A duration a reader can take in at a glance, rather than to two places.
fn span(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 60.0 {
        return format!("{s:.1}s");
    }
    let mins = (s / 60.0).floor();
    format!("{}m{:04.1}s", mins as u64, s - mins * 60.0)
}
