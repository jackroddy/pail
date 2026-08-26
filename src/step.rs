use crate::closure::Closure;
use crate::cmd::Cmd;
use crate::execute::Status;
use crate::item::Item;
use crate::label;

/// How far a failed command reaches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnError {
    /// Run the rest of this step's commands anyway.
    Continue,
    /// Skip the rest of this step. The pipeline carries on to the next one.
    Skip,
    /// Skip the rest of this step, skip every step after it, and fail the run.
    #[default]
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    Serial,
    Batched { jobs: usize },
}

/// What a step holds. Commands and closures are run and reported differently
/// enough to be kept apart: only commands have a strategy to run under, cores
/// to ask for, or an argv to print.
#[derive(Debug)]
pub(crate) enum Items {
    Cmds {
        cmds: Vec<Cmd>,
        strategy: Strategy,
        /// How many cores each of these asks for, unless it asked for itself.
        cores: Option<usize>,
    },
    /// Serial by definition, for now: nothing here spawns a thread.
    Closures(Vec<Closure>),
}

#[derive(Debug)]
pub struct Step {
    pub(crate) name: Option<String>,
    pub(crate) index: Option<usize>,
    pub(crate) on_error: OnError,
    pub(crate) elapsed_s: Option<f64>,
    pub(crate) items: Items,
}

impl Step {
    pub fn serial(cmds: impl IntoIterator<Item = Cmd>) -> Self {
        Step::of(Items::Cmds {
            cmds: cmds.into_iter().collect(),
            strategy: Strategy::Serial,
            cores: None,
        })
    }

    pub fn batched(jobs: usize, cmds: impl IntoIterator<Item = Cmd>) -> Self {
        Step::of(Items::Cmds {
            cmds: cmds.into_iter().collect(),
            strategy: Strategy::Batched { jobs: jobs.max(1) },
            cores: None,
        })
    }

    /// One closure after another. There is no batched form: a closure runs on
    /// the thread that reached it.
    pub fn from_closures(closures: impl IntoIterator<Item = Closure>) -> Self {
        Step::of(Items::Closures(closures.into_iter().collect()))
    }

    fn of(items: Items) -> Self {
        Step {
            name: None,
            index: None,
            on_error: OnError::default(),
            elapsed_s: None,
            items,
        }
    }

    /// Pin each of this step's commands to `cores` physical cores.
    ///
    /// Per command, not per step: a batch of four with `cores(2)` asks for eight
    /// cores. If the machine cannot spare that many at once, the commands that
    /// cannot be placed wait for the ones that can.
    pub fn cores(mut self, cores: usize) -> Self {
        // a closure asks for none, so there is nothing here to set
        if let Items::Cmds { cores: c, .. } = &mut self.items {
            *c = Some(cores);
        }
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// What to call this step. Empty until the pipeline is built, which is when
    /// a step learns its number.
    pub fn label(&self) -> String {
        match self.index {
            Some(index) => label::label(index, self.name.as_deref()),
            None => self.name.clone().unwrap_or_default(),
        }
    }

    pub fn cmds(&self) -> &[Cmd] {
        match &self.items {
            Items::Cmds { cmds, .. } => cmds,
            Items::Closures(_) => &[],
        }
    }

    pub fn closures(&self) -> &[Closure] {
        match &self.items {
            Items::Cmds { .. } => &[],
            Items::Closures(closures) => closures,
        }
    }

    /// Everything this step holds, in the order it was given, whichever kind it
    /// holds. This is what a sink sees.
    pub fn items(&self) -> impl Iterator<Item = Item<'_>> {
        self.cmds()
            .iter()
            .map(Item::Cmd)
            .chain(self.closures().iter().map(Item::Closure))
    }

    pub fn wall_s(&self) -> Option<f64> {
        self.elapsed_s
    }

    /// How this step's commands run. `None` for a step of closures, which has
    /// no commands to run either way.
    pub fn strategy(&self) -> Option<Strategy> {
        match &self.items {
            Items::Cmds { strategy, .. } => Some(*strategy),
            Items::Closures(_) => None,
        }
    }

    /// How many of this step's commands can be running at once.
    pub fn width(&self) -> usize {
        match self.strategy() {
            Some(Strategy::Batched { jobs }) => jobs,
            _ => 1,
        }
    }

    pub(crate) fn cmds_mut(&mut self) -> &mut [Cmd] {
        match &mut self.items {
            Items::Cmds { cmds, .. } => cmds,
            Items::Closures(_) => &mut [],
        }
    }

    pub(crate) fn closures_mut(&mut self) -> &mut [Closure] {
        match &mut self.items {
            Items::Cmds { .. } => &mut [],
            Items::Closures(closures) => closures,
        }
    }

    /// Whether the rest of this step's commands are worth running.
    pub(crate) fn skips(&self) -> bool {
        self.on_error != OnError::Continue && self.failed().is_some()
    }

    /// What to say about the thing that ends the run, if this step holds one.
    ///
    /// A command names the line you could paste to see it again; a closure has
    /// no such line, so it goes by its name alone.
    pub(crate) fn aborts(&self) -> Option<String> {
        (self.on_error == OnError::Abort)
            .then(|| self.failed())
            .flatten()
    }

    /// The first thing in this step that failed, described.
    fn failed(&self) -> Option<String> {
        match &self.items {
            Items::Cmds { cmds, .. } => cmds
                .iter()
                .find(|c| c.status().failed())
                .map(|c| format!("{} failed: {}", c.label(), c.line())),
            // a closure has no stderr file to go and read, so what it said when
            // it failed is only ever going to be here
            Items::Closures(closures) => {
                closures.iter().find(|c| c.status().failed()).map(|c| {
                    match c.status() {
                        Status::Failed(why) => format!("{} failed: {why}", c.label()),
                        // a failure with no words of its own
                        _ => format!("{} failed", c.label()),
                    }
                })
            }
        }
    }
}

impl From<Cmd> for Step {
    fn from(cmd: Cmd) -> Step {
        Step::serial([cmd])
    }
}

impl From<Closure> for Step {
    fn from(closure: Closure) -> Step {
        Step::from_closures([closure])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::{Status, Timing};

    fn timing(exit: i32) -> Timing {
        Timing {
            wall_s: 1.0,
            user_s: Some(1.0),
            sys_s: Some(0.0),
            max_rss_kb: Some(1024),
            exit,
        }
    }

    /// Two commands, the second having gone however `outcome` says.
    fn step(on_error: OnError, outcome: Status) -> Step {
        let mut step = Step::serial([Cmd::new("/a"), Cmd::new("/b")]).on_error(on_error);
        step.cmds_mut()[0].status = Status::Finished(timing(0));
        step.cmds_mut()[1].status = outcome;
        step
    }

    #[test]
    fn nothing_failed_so_nothing_reaches_anywhere() {
        for on_error in [OnError::Continue, OnError::Skip, OnError::Abort] {
            let step = step(on_error, Status::Finished(timing(0)));
            assert!(!step.skips(), "{on_error:?} skipped a clean step");
            assert!(step.aborts().is_none(), "{on_error:?} aborted a clean step");
        }
    }

    #[test]
    fn continue_lets_a_failure_pass() {
        let step = step(OnError::Continue, Status::Finished(timing(1)));
        assert!(!step.skips());
        assert!(step.aborts().is_none());
    }

    #[test]
    fn skip_stops_the_step_and_stops_there() {
        let step = step(OnError::Skip, Status::Finished(timing(1)));
        assert!(step.skips());
        assert!(step.aborts().is_none());
    }

    #[test]
    fn abort_stops_the_step_and_names_what_did_it() {
        let step = step(OnError::Abort, Status::Finished(timing(1)));
        assert!(step.skips());
        let why = step.aborts().expect("a failing command should abort");
        assert!(why.starts_with("b failed: "), "{why}");
    }

    #[test]
    fn abort_is_the_default() {
        assert_eq!(OnError::default(), OnError::Abort);
    }

    #[test]
    fn a_command_that_never_ran_is_not_a_failure() {
        for outcome in [Status::NotRun, Status::Skipped] {
            let step = step(OnError::Abort, outcome);
            assert!(!step.skips());
            assert!(step.aborts().is_none());
        }
    }

    #[test]
    fn every_way_of_failing_counts() {
        for outcome in [
            Status::Failed("could not spawn".into()),
            Status::TimedOut(timing(143)),
            Status::Finished(timing(1)),
        ] {
            let step = step(OnError::Abort, outcome);
            assert!(step.skips());
            assert!(step.aborts().is_some());
        }
    }

    #[test]
    fn a_batch_always_has_a_worker() {
        // zero jobs would mean no workers at all, which hangs rather than
        // finishing empty
        assert_eq!(Step::batched(0, [Cmd::new("/a")]).width(), 1);
        assert_eq!(Step::batched(4, [Cmd::new("/a")]).width(), 4);
        assert_eq!(Step::serial([Cmd::new("/a")]).width(), 1);
    }
}
