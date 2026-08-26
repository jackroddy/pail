use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::execute::Status;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Output {
    #[default]
    Null,
    Inherit,
    File(PathBuf),
    Append(PathBuf),
    OnFailure(PathBuf),
}

/// Something that can stand in as the value of an option.
///
/// There is no blanket impl over Display, so paths get their own and come out
/// without the quotes a Debug print would add.
pub trait Value {
    fn render(self) -> String;
}

macro_rules! value_via_display {
    ($($t:ty),* $(,)?) => {
        $(impl Value for $t {
            fn render(self) -> String {
                self.to_string()
            }
        })*
    };
}

value_via_display!(
    &str,
    String,
    &String,
    char,
    bool,
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize,
    f32,
    f64,
    std::fmt::Arguments<'_>,
);

impl Value for &Path {
    fn render(self) -> String {
        self.display().to_string()
    }
}

impl Value for PathBuf {
    fn render(self) -> String {
        self.display().to_string()
    }
}

impl Value for &PathBuf {
    fn render(self) -> String {
        self.display().to_string()
    }
}

/// An option: a flag on its own, or a flag with a value after it.
#[derive(Clone, Debug)]
pub(crate) struct Opt {
    flag: String,
    value: Option<String>,
}

/// A command, built but not run.
///
/// The pieces are kept apart rather than in one argv so that the order they are
/// added in doesn't matter. Several of these tools take their query and target
/// as trailing positionals, and an option tacked on after them would be read as
/// another file.
#[derive(Clone, Debug)]
pub struct Cmd {
    pub(crate) name: Option<String>,
    pub(crate) program: PathBuf,
    /// How many cores this asks for. Resolved when the pipeline is built, since
    /// it can come from the step instead.
    pub(crate) cores: Option<usize>,
    /// The cpus it was given, filled in when it runs and only for as long as it
    /// held them.
    pub(crate) cpus: Vec<usize>,
    pub(crate) sub: Vec<String>,
    pub(crate) opts: Vec<Opt>,
    pub(crate) positionals: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) dir: Option<PathBuf>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) stdout: Output,
    pub(crate) stderr: Output,
    pub(crate) fields: BTreeMap<String, String>,
    pub(crate) tags: BTreeSet<String>,
    pub(crate) status: Status,
}

impl Cmd {
    pub fn new(program: impl AsRef<Path>) -> Self {
        Cmd {
            name: None,
            program: program.as_ref().to_owned(),
            cores: None,
            cpus: Vec::new(),
            sub: Vec::new(),
            opts: Vec::new(),
            positionals: Vec::new(),
            env: BTreeMap::new(),
            dir: None,
            timeout: None,
            stdout: Output::Null,
            stderr: Output::Null,
            fields: BTreeMap::new(),
            tags: BTreeSet::new(),
            status: Status::NotRun,
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Pin this command to `cores` physical cores, whichever ones are going.
    /// Overrides whatever its step asked for.
    pub fn cores(mut self, cores: usize) -> Self {
        self.cores = Some(cores);
        self
    }

    /// A subcommand, like the `search` in `mmseqs search`. Call it more than
    /// once for tools that nest them.
    pub fn sub(mut self, sub: impl Into<String>) -> Self {
        self.sub.push(sub.into());
        self
    }

    /// An option that stands alone, like `--allow-overwrite`.
    pub fn flag(mut self, flag: impl Into<String>) -> Self {
        self.opts.push(Opt {
            flag: flag.into(),
            value: None,
        });
        self
    }

    /// An option and the value that follows it, like `-E 10`.
    pub fn arg(mut self, flag: impl Into<String>, value: impl Value) -> Self {
        self.opts.push(Opt {
            flag: flag.into(),
            value: Some(value.render()),
        });
        self
    }

    /// A positional. These come out in the order they were added, after
    /// everything else.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.positionals.push(path.as_ref().display().to_string());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Value) -> Self {
        self.env.insert(key.into(), value.render());
        self
    }

    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    pub fn timeout(mut self, after: Duration) -> Self {
        self.timeout = Some(after);
        self
    }

    pub fn stdout(mut self, out: Output) -> Self {
        self.stdout = out;
        self
    }

    pub fn stderr(mut self, out: Output) -> Self {
        self.stderr = out;
        self
    }

    pub fn stdout_to(self, path: impl Into<PathBuf>) -> Self {
        self.stdout(Output::File(path.into()))
    }

    pub fn stderr_to(self, path: impl Into<PathBuf>) -> Self {
        self.stderr(Output::File(path.into()))
    }

    pub fn field(mut self, key: impl Into<String>, value: impl Value) -> Self {
        self.fields.insert(key.into(), value.render());
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.insert(tag.into());
        self
    }

    /// What to call this command in a table or on the progress line: its name if
    /// it was given one, and otherwise the program it runs, with the path in
    /// front of it dropped. Not unique — the argv column is what tells two
    /// `mkdir`s apart.
    pub fn label(&self) -> String {
        match &self.name {
            Some(name) => name.clone(),
            None => match self.program.file_name() {
                Some(file) => file.to_string_lossy().into_owned(),
                None => self.program.display().to_string(),
            },
        }
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

    pub fn stderr_path(&self) -> Option<&Path> {
        match &self.stderr {
            Output::Null | Output::Inherit => None,
            Output::File(p) | Output::Append(p) | Output::OnFailure(p) => Some(p),
        }
    }

    /// Everything after the program, in the order it gets handed to the shell.
    pub(crate) fn args(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.sub.len() + self.opts.len() + self.positionals.len());

        out.extend(self.sub.iter().cloned());

        for opt in &self.opts {
            out.push(opt.flag.clone());
            if let Some(value) = &opt.value {
                out.push(value.clone());
            }
        }

        out.extend(self.positionals.iter().cloned());
        out
    }

    /// The program and everything after it, as they get handed to exec.
    pub(crate) fn argv(&self) -> (PathBuf, Vec<String>) {
        (self.program.clone(), self.args())
    }

    /// The command as a shell line.
    ///
    /// A working directory becomes a subshell, so running it leaves your own
    /// shell where it was. The redirects sit outside it, because the files are
    /// opened before the child moves anywhere, so a relative one lands in the
    /// same place either way.
    ///
    /// Pinning is not in here. The child sets its own affinity rather than
    /// being wrapped in something that sets it, so there is nothing on the
    /// command line to write down; the cpus column says where it ran instead.
    pub fn line(&self) -> String {
        let mut parts: Vec<String> = self
            .env
            .iter()
            .map(|(key, value)| format!("{key}={}", quote(value)))
            .collect();

        let (program, args) = self.argv();
        parts.push(quote(&program.display().to_string()));
        parts.extend(args.iter().map(|a| quote(a)));

        let mut line = parts.join(" ");

        if let Some(dir) = &self.dir {
            line = format!("(cd {} && {line})", quote(&dir.display().to_string()));
        }

        for redirect in [redirect(&self.stdout, ""), redirect(&self.stderr, "2")]
            .into_iter()
            .flatten()
        {
            line.push(' ');
            line.push_str(&redirect);
        }

        line
    }
}

fn redirect(out: &Output, fd: &str) -> Option<String> {
    let (op, path) = match out {
        Output::Inherit => return None,
        Output::Null => (">", "/dev/null".to_string()),
        // OnFailure still writes to the file, it just may not survive the run
        Output::File(p) | Output::OnFailure(p) => (">", quote(&p.display().to_string())),
        Output::Append(p) => (">>", quote(&p.display().to_string())),
    };

    Some(format!("{fd}{op} {path}"))
}

fn quote(arg: &str) -> String {
    const SAFE_PUNCT: &str = "_-./=:+,@%^";

    let plain = !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || SAFE_PUNCT.contains(c));

    if plain {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_leaves_alone_what_a_shell_would_read_the_same_way() {
        for plain in [
            "nail",
            "--allow-overwrite",
            "/home/jack/tools/bin/nail",
            "12.0",
            "a_b-c.d/e=f:g+h,i@j%k^l",
        ] {
            assert_eq!(quote(plain), plain);
        }
    }

    #[test]
    fn quote_wraps_anything_a_shell_would_read_differently() {
        assert_eq!(quote(""), "''");
        assert_eq!(quote("two words"), "'two words'");
        assert_eq!(quote("a*b"), "'a*b'");
        assert_eq!(quote("$HOME"), "'$HOME'");
        assert_eq!(quote("a\nb"), "'a\nb'");
        assert_eq!(quote("a;rm -rf /"), "'a;rm -rf /'");
        assert_eq!(quote("~/x"), "'~/x'");
        assert_eq!(quote("café"), "'café'");
    }

    #[test]
    fn quote_closes_and_reopens_around_a_single_quote() {
        // 'it'\''s' is the only way to get a literal quote inside single quotes
        assert_eq!(quote("it's"), r"'it'\''s'");
        assert_eq!(quote("'"), r"''\'''");
    }

    #[test]
    fn positionals_come_last_however_they_were_added() {
        // the whole reason a Cmd keeps its pieces apart: several of these tools
        // read a trailing option as another input file
        let cmd = Cmd::new("/bin/mmseqs")
            .path("query.fa")
            .arg("-s", "7.5")
            .path("target.fa")
            .flag("--quiet")
            .sub("search");

        assert_eq!(
            cmd.args(),
            ["search", "-s", "7.5", "--quiet", "query.fa", "target.fa"]
        );
    }

    #[test]
    fn options_keep_the_order_they_were_given_in() {
        let cmd = Cmd::new("/x").arg("-a", 1).flag("-b").arg("-c", 3);
        assert_eq!(cmd.args(), ["-a", "1", "-b", "-c", "3"]);
    }

    #[test]
    fn subcommands_nest_in_front() {
        let cmd = Cmd::new("/x").sub("outer").sub("inner").flag("-q");
        assert_eq!(cmd.args(), ["outer", "inner", "-q"]);
    }

    #[test]
    fn pinning_stays_out_of_the_argv() {
        let mut cmd = Cmd::new("/bin/nail").sub("search");
        cmd.cpus = vec![0, 2];
        let (program, args) = cmd.argv();

        assert_eq!(program, PathBuf::from("/bin/nail"));
        assert_eq!(args, ["search"]);
        assert_eq!(cmd.line(), "/bin/nail search > /dev/null 2> /dev/null");
    }

    #[test]
    fn a_line_carries_its_environment_in_front() {
        let line = Cmd::new("/usr/bin/wc")
            .env("LC_ALL", "C")
            .flag("-l")
            .stdout(Output::Inherit)
            .stderr(Output::Inherit)
            .line();

        assert_eq!(line, "LC_ALL=C /usr/bin/wc -l");
    }

    #[test]
    fn a_working_directory_becomes_a_subshell_with_the_redirects_outside_it() {
        // the files are opened before the child moves anywhere, so a relative
        // redirect lands in the same place whether or not you paste the cd
        let line = Cmd::new("/usr/bin/wc")
            .flag("-l")
            .path("data.txt")
            .dir("/tmp/work")
            .stdout_to("out.txt")
            .stderr(Output::Inherit)
            .line();

        assert_eq!(line, "(cd /tmp/work && /usr/bin/wc -l data.txt) > out.txt");
    }

    #[test]
    fn a_line_says_where_each_stream_went() {
        let base = || Cmd::new("/x").stderr(Output::Inherit);

        assert_eq!(base().line(), "/x > /dev/null");
        assert_eq!(base().stdout(Output::Inherit).line(), "/x");
        assert_eq!(base().stdout_to("o").line(), "/x > o");
        assert_eq!(base().stdout(Output::Append("o".into())).line(), "/x >> o");
        // OnFailure still writes to the file, it just may not survive the run
        assert_eq!(
            base().stdout(Output::OnFailure("o".into())).line(),
            "/x > o"
        );
        assert_eq!(
            Cmd::new("/x").stderr_to("e").line(),
            "/x > /dev/null 2> e",
            "stdout comes before stderr"
        );
    }

    #[test]
    fn a_label_falls_back_to_the_program_with_the_path_dropped() {
        assert_eq!(Cmd::new("/home/jack/tools/bin/nail").label(), "nail");
        assert_eq!(Cmd::new("mkdir").label(), "mkdir");
        assert_eq!(Cmd::new("/x").name("prep").label(), "prep");
    }
}
