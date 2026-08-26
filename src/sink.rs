//! Where results go.
//!
//! A [`Pipeline`](crate::Pipeline) knows nothing about output. It runs
//! commands and announces what happened to whatever sinks were registered on
//! it. Printing progress and writing the summary table are both just sinks.
//!
//! # What a sink is promised
//!
//! Everything arrives on one thread, in order, so a sink needs no locking of
//! its own. The calls nest the way the run does:
//!
//! - [`start`](Sink::start) once, before anything runs.
//! - Then, for every step in turn: [`step_start`](Sink::step_start), the items
//!   inside it, then [`step_done`](Sink::step_done). Every step gets both, in
//!   that order, including the ones a failed run never reached — those simply
//!   get them one after the other with skipped items between.
//! - [`abandoned`](Sink::abandoned) if the run is being given up on, once.
//! - [`finish`](Sink::finish) once, however it ended.
//!
//! Within a step, each item is announced to [`item_done`](Sink::item_done)
//! **exactly once**, and never as [`Status::NotRun`] — by the time you see one
//! it has finished, failed to start, or been skipped.
//!
//! An item that actually ran also gets one [`item_start`](Sink::item_start)
//! before that. Not every item does: one that a stopping step never reached
//! goes straight to `item_done` as skipped. So `item_start` implies an
//! `item_done` will follow, but not the other way round.
//!
//! Both carry the item's position in its step, which is what tells two of them
//! apart when they share a name — [`label`](crate::Item::label) is not unique,
//! since an unnamed command goes by the program it runs.
//!
//! [`Status::NotRun`]: crate::Status

use crate::item::Item;
use crate::step::Step;

/// Something that wants to hear what the pipeline did.
///
/// Every method does nothing by default, so an implementation only writes the
/// ones it cares about. Returning `Err` from any of them stops the run: a sink
/// that cannot write is worth abandoning a long benchmark over.
pub trait Sink {
    /// Everything the pipeline is about to run, before any of it has.
    ///
    /// This is where a sink that needs to know the shape of the whole run — the
    /// full set of field keys, say — works it out.
    fn start(&mut self, steps: &[Step]) -> anyhow::Result<()> {
        let _ = steps;
        Ok(())
    }

    /// This step is the pipeline's current concern.
    fn step_start(&mut self, step: &Step) -> anyhow::Result<()> {
        let _ = step;
        Ok(())
    }

    /// One item is running: it holds whatever cores it asked for, and the
    /// process is about to be spawned or the closure about to be called.
    ///
    /// Time measured from here matches the wall clock that eventually gets
    /// recorded, because whatever waiting there was for cores has already
    /// happened. `at` is its position in the step.
    fn item_start(&mut self, step: &Step, at: usize, item: Item<'_>) -> anyhow::Result<()> {
        let _ = (step, at, item);
        Ok(())
    }

    /// One item reached its final state. `step` is the one holding it, for the
    /// name and whether its commands ran together, and `at` is its position in
    /// that step.
    fn item_done(&mut self, step: &Step, at: usize, item: Item<'_>) -> anyhow::Result<()> {
        let _ = (step, at, item);
        Ok(())
    }

    /// Every item in `step` has been announced, and its wall clock is final.
    fn step_done(&mut self, step: &Step) -> anyhow::Result<()> {
        let _ = step;
        Ok(())
    }

    /// The run is being given up on, and why. The steps after this one still
    /// report their items as skipped, and [`finish`](Sink::finish) still
    /// follows.
    fn abandoned(&mut self, why: &str) -> anyhow::Result<()> {
        let _ = why;
        Ok(())
    }

    /// The pipeline is over, however it ended.
    fn finish(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
