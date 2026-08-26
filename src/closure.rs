//! A closure the pipeline runs as a step of its own.

use std::collections::{BTreeMap, BTreeSet};

use crate::cmd::Value;
use crate::execute::Status;

/// `Send` because it may be run from a worker thread, and `'static` because a
/// [`Closure`] has no lifetime to hang it on — captures are owned, or shared
/// through an `Arc`. `FnOnce`, so values can be moved in and back out.
pub(crate) type Call = Box<dyn FnOnce() -> anyhow::Result<()> + Send>;

/// Rust to run in place of a command.
///
/// Only its wall clock is measured. A thread has no `wait4` to ask, so there is
/// no cpu time and no peak memory, and a closure takes neither a timeout nor a
/// core count — a thread can be neither killed on a deadline nor pinned.
pub struct Closure {
    pub(crate) name: String,
    pub(crate) fields: BTreeMap<String, String>,
    pub(crate) tags: BTreeSet<String>,
    pub(crate) status: Status,
    /// `None` once it has been run, which is the only time it can be.
    pub(crate) f: Option<Call>,
}

impl std::fmt::Debug for Closure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Closure")
            .field("name", &self.name)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl Closure {
    /// The name is not optional the way a command's is: there is no program
    /// behind it to fall back on.
    pub fn new(
        name: impl Into<String>,
        f: impl FnOnce() -> anyhow::Result<()> + Send + 'static,
    ) -> Closure {
        Closure {
            name: name.into(),
            fields: BTreeMap::new(),
            tags: BTreeSet::new(),
            status: Status::NotRun,
            f: Some(Box::new(f)),
        }
    }

    pub fn field(mut self, key: impl Into<String>, value: impl Value) -> Self {
        self.fields.insert(key.into(), value.render());
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.insert(tag.into());
        self
    }

    pub fn label(&self) -> &str {
        &self.name
    }

    pub fn status(&self) -> &Status {
        &self.status
    }

    pub fn fields(&self) -> &BTreeMap<String, String> {
        &self.fields
    }

    pub fn tags(&self) -> &BTreeSet<String> {
        &self.tags
    }
}
