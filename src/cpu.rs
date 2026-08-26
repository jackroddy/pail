//! Which CPUs a command is allowed to run on.
//!
//! A command asks for a number of cores and never says which. [`Cores`] keeps
//! track of what is free, hands out that many, and takes them back when the
//! command is done. The pinning happens in the child itself, between the fork
//! and the exec.

use std::sync::{Condvar, Mutex};

/// The cores a pipeline has to hand out, and which of them are in use.
///
/// The pool holds one logical CPU per physical core. Two logical CPUs on one
/// physical core are not two cores — they share the execution units, so a
/// command given both would get somewhere around 1.3 cores' worth of work done
/// while the table claimed it had 2. Only the first of each sibling group is
/// ever handed out and the rest sit idle, which is why a 32-CPU machine with
/// hyperthreading has 24 of these rather than 32.
#[derive(Debug, Default)]
pub(crate) struct Cores {
    pool: Vec<usize>,
    /// The cpus currently leased out, and something to wait on for one to come
    /// back. A command that cannot be placed yet sleeps here rather than
    /// spinning, which would burn a core to wait for a core.
    taken: Mutex<Vec<usize>>,
    freed: Condvar,
}

/// Cores held for as long as one command needs them.
///
/// Handing them back is [`Drop`]'s job rather than the caller's, so a command
/// that failed to spawn, timed out, or panicked releases its cores the same way
/// one that finished does.
pub(crate) struct Lease<'a> {
    cores: &'a Cores,
    cpus: Vec<usize>,
}

impl Cores {
    pub(crate) fn read() -> Cores {
        let mut pool = Vec::new();
        let mut spoken_for = Vec::new();

        for cpu in allowed() {
            if spoken_for.contains(&cpu) {
                continue;
            }
            spoken_for.extend(siblings(cpu));
            pool.push(cpu);
        }

        Cores {
            pool,
            taken: Mutex::new(Vec::new()),
            freed: Condvar::new(),
        }
    }

    /// A pool of exactly these cpus, so the handing-out can be exercised without
    /// depending on what the machine happens to have.
    #[cfg(test)]
    pub(crate) fn with_pool(pool: Vec<usize>) -> Cores {
        Cores {
            pool,
            taken: Mutex::new(Vec::new()),
            freed: Condvar::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.pool.len()
    }

    /// Take `size` cores, waiting for them if they are not free yet.
    ///
    /// `None` for a command that asked for none, and for one that gave up
    /// waiting because the run is stopping — neither is going to be pinned.
    /// A request larger than the whole machine would wait forever, which is why
    /// the pipeline refuses to build one.
    pub(crate) fn acquire(&self, size: usize, abandon: &dyn Fn() -> bool) -> Option<Lease<'_>> {
        if size == 0 || size > self.pool.len() {
            return None;
        }

        let mut taken = self.taken.lock().unwrap();
        loop {
            if abandon() {
                return None;
            }
            if let Some(lease) = self.grab(&mut taken, size) {
                return Some(lease);
            }
            taken = self.freed.wait(taken).unwrap();
        }
    }

    /// Take `size` cores if they are free right now, and never wait. For
    /// working out what a run would look like without running it.
    pub(crate) fn try_acquire(&self, size: usize) -> Option<Lease<'_>> {
        if size == 0 || size > self.pool.len() {
            return None;
        }
        self.grab(&mut self.taken.lock().unwrap(), size)
    }

    fn grab(&self, taken: &mut Vec<usize>, size: usize) -> Option<Lease<'_>> {
        // lowest first, so a run with the machine to itself places its commands
        // the same way every time
        let free: Vec<usize> = self
            .pool
            .iter()
            .filter(|cpu| !taken.contains(cpu))
            .take(size)
            .copied()
            .collect();

        if free.len() < size {
            return None;
        }

        taken.extend(&free);
        Some(Lease {
            cores: self,
            cpus: free,
        })
    }

    /// Wake anything waiting on cores that are never coming, so a stopping run
    /// does not leave workers parked.
    pub(crate) fn wake(&self) {
        self.freed.notify_all();
    }

    fn release(&self, cpus: &[usize]) {
        let mut taken = self.taken.lock().unwrap();
        taken.retain(|cpu| !cpus.contains(cpu));
        drop(taken);
        self.freed.notify_all();
    }
}

impl Lease<'_> {
    pub(crate) fn cpus(&self) -> &[usize] {
        &self.cpus
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.cores.release(&self.cpus);
    }
}

/// The affinity mask for `cpus`, built while there is still a whole program to
/// build it in. What installs it runs after the fork, where about the only
/// thing left that is safe to do is make one syscall.
///
/// A cpu at or past `CPU_SETSIZE` would write outside the set, so it is dropped
/// rather than trusted. `allowed` cannot produce one, and this is what keeps
/// that true if a pool ever comes from somewhere else.
#[cfg(target_os = "linux")]
pub(crate) fn mask(cpus: &[usize]) -> libc::cpu_set_t {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for cpu in cpus.iter().filter(|cpu| **cpu < libc::CPU_SETSIZE as usize) {
        unsafe { libc::CPU_SET(*cpu, &mut set) };
    }
    set
}

/// A cpu list the way the kernel writes one: `0`, `0,2`, empty for nothing.
pub(crate) fn list(cpus: &[usize]) -> String {
    let parts: Vec<String> = cpus.iter().map(|cpu| cpu.to_string()).collect();
    parts.join(",")
}

/// How wide the list for `cores` cpus comes out if every one of them is a
/// single digit: one digit each, and a comma between them.
///
/// Room to reserve before anything has run and we know which cpus they are.
/// The low guess on purpose — a cpu past the first ten grows the column past
/// this, and nothing is reserved for a width most runs will not use.
pub(crate) fn list_width(cores: usize) -> usize {
    match cores {
        0 => 0,
        cores => 2 * cores - 1,
    }
}

/// The CPUs this process may run on, which is not the same as every CPU the
/// machine has — a benchmark started under `taskset` gets a smaller set, and
/// handing out anything outside it would pin commands nowhere.
#[cfg(target_os = "linux")]
fn allowed() -> Vec<usize> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
    if rc != 0 {
        return Vec::new();
    }

    (0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .collect()
}

/// Every CPU the machine has. There is no `taskset` here to ask for a
/// smaller set, so this is the honest answer rather than an approximation.
#[cfg(not(target_os = "linux"))]
fn allowed() -> Vec<usize> {
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    (0..cpus).collect()
}

/// Every logical CPU sharing a physical core with this one, itself included.
/// A CPU whose topology we cannot read is treated as a core of its own, which
/// is the reading that under-promises.
fn siblings(cpu: usize) -> Vec<usize> {
    let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
    match std::fs::read_to_string(path) {
        Ok(text) => parse_list(&text),
        Err(_) => vec![cpu],
    }
}

/// A kernel cpulist: `0-1`, `16`, `0-3,8-11`.
fn parse_list(text: &str) -> Vec<usize> {
    let mut out = Vec::new();

    for part in text.trim().split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((lo, hi)) => {
                if let (Ok(lo), Ok(hi)) = (lo.parse::<usize>(), hi.parse::<usize>()) {
                    out.extend(lo..=hi);
                }
            }
            None => out.extend(part.parse::<usize>()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parse_list_reads_what_the_kernel_writes() {
        assert_eq!(parse_list("16"), vec![16]);
        assert_eq!(parse_list("0-1"), vec![0, 1]);
        assert_eq!(parse_list("0-3,8-11"), vec![0, 1, 2, 3, 8, 9, 10, 11]);
        assert_eq!(parse_list("0,4,8"), vec![0, 4, 8]);
        // sysfs files come with one
        assert_eq!(parse_list("2-3\n"), vec![2, 3]);
    }

    #[test]
    fn parse_list_gives_up_on_a_part_it_cannot_read_rather_than_the_whole_line() {
        assert_eq!(parse_list(""), Vec::<usize>::new());
        assert_eq!(parse_list("\n"), Vec::<usize>::new());
        assert_eq!(parse_list("junk"), Vec::<usize>::new());
        assert_eq!(parse_list("0,junk,2"), vec![0, 2]);
        // a range that runs backwards yields nothing, not a panic
        assert_eq!(parse_list("5-2"), Vec::<usize>::new());
    }

    #[test]
    fn cores_go_out_lowest_first() {
        let cores = Cores::with_pool(vec![0, 2, 4, 6]);
        let lease = cores.acquire(2, &|| false).expect("two of four");
        assert_eq!(lease.cpus(), [0, 2]);
    }

    #[test]
    fn two_leases_never_share_a_core() {
        let cores = Cores::with_pool(vec![0, 2, 4, 6]);
        let first = cores.acquire(2, &|| false).expect("two of four");
        let second = cores.acquire(2, &|| false).expect("the other two");

        assert_eq!(first.cpus(), [0, 2]);
        assert_eq!(second.cpus(), [4, 6]);
        assert!(cores.try_acquire(1).is_none(), "pool should be empty");
    }

    #[test]
    fn dropping_a_lease_hands_the_cores_back() {
        let cores = Cores::with_pool(vec![0, 2, 4, 6]);
        {
            let _all = cores.acquire(4, &|| false).expect("the whole pool");
            assert!(cores.try_acquire(1).is_none(), "nothing should be left");
        }
        assert_eq!(
            cores.try_acquire(4).map(|l| l.cpus().to_vec()),
            Some(vec![0, 2, 4, 6]),
            "the whole pool should be back"
        );
    }

    #[test]
    fn packing_leaves_no_gap() {
        let cores = Cores::with_pool(vec![0, 2, 4, 6, 8, 10]);
        let small = cores.acquire(1, &|| false).expect("one");
        let big = cores.acquire(3, &|| false).expect("three");

        assert_eq!(small.cpus(), [0]);
        assert_eq!(big.cpus(), [2, 4, 6], "should start right after the small one");
    }

    #[test]
    fn asking_for_more_than_exists_is_refused_rather_than_waited_on() {
        let cores = Cores::with_pool(vec![0, 2]);
        assert!(cores.acquire(3, &|| false).is_none());
        assert!(cores.try_acquire(3).is_none());
    }

    #[test]
    fn asking_for_none_gets_none() {
        let cores = Cores::with_pool(vec![0, 2]);
        assert!(cores.acquire(0, &|| false).is_none());
        // and it did not quietly take anything on the way past
        assert_eq!(cores.try_acquire(2).map(|l| l.cpus().to_vec()), Some(vec![0, 2]));
    }

    #[test]
    fn a_wait_can_be_abandoned() {
        let cores = Cores::with_pool(vec![0, 2]);
        let _all = cores.acquire(2, &|| false).expect("the whole pool");
        // nothing is free, and the predicate says do not wait for it
        assert!(cores.acquire(1, &|| true).is_none());
    }

    /// Note: if the wake ever breaks, this hangs rather than failing — there is
    /// no timeout on the inner wait to bound it with.
    #[test]
    fn a_waiting_thread_gets_the_cores_when_they_come_back() {
        let cores = Cores::with_pool(vec![0, 2, 4, 6]);
        let all = cores.acquire(4, &|| false).expect("the whole pool");

        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| cores.acquire(2, &|| false).map(|l| l.cpus().to_vec()));
            // long enough that the other thread is parked rather than racing us
            std::thread::sleep(Duration::from_millis(50));
            drop(all);
            assert_eq!(waiting.join().unwrap(), Some(vec![0, 2]));
        });
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn a_mask_holds_exactly_the_cpus_it_was_given() {
        let set = mask(&[0, 2]);
        let held: Vec<usize> = (0..8)
            .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
            .collect();

        assert_eq!(held, [0, 2]);
        assert_eq!(list(&held), "0,2");
    }
}
