//! Naming steps: once for a reader, once for a filename.

/// What a step is called in the table and on the progress lines: its number, and
/// its name if it was given one.
pub(crate) fn label(index: usize, name: Option<&str>) -> String {
    match name {
        Some(name) => format!("[{index}]({name})"),
        None => format!("[{index}]"),
    }
}

/// The same thing as a path component, for the stderr file a command may leave
/// behind.
pub(crate) fn filename(index: usize, name: Option<&str>) -> String {
    match name {
        Some(name) => format!("{index}-{}", safe(name)),
        None => index.to_string(),
    }
}

fn safe(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect()
}
