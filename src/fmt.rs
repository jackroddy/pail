//! Turning numbers into the strings a reader sees.
//!
//! Shared by the table and the progress lines, which disagree about wording but
//! agree about how a duration, a size and a percentage should look.

/// What goes in a cell with no number behind it.
pub(crate) fn dash() -> String {
    "-".to_string()
}

/// Seconds to two places, or a dash.
pub(crate) fn secs(s: Option<f64>) -> String {
    s.map(|s| format!("{s:.2}")).unwrap_or_else(dash)
}

/// `time`'s `%P`: the CPU something burned over the wall clock it took, so a
/// command that kept four cores busy the whole way through reads 400%.
///
/// Truncated rather than rounded, since `time` divides two integers. `time`
/// writes `?%` when there is no clock to divide by; the rest of the table says
/// `-` when it has no number, so that is what goes here.
pub(crate) fn cpu_pct(cpu_s: f64, wall_s: Option<f64>) -> String {
    match wall_s.filter(|wall| *wall > 0.0) {
        Some(wall) => format!("{:.0}%", (cpu_s / wall * 100.0).floor()),
        None => dash(),
    }
}

/// Peak memory in binary units, to about three significant figures so the column
/// reads at a glance: 940KiB, 10.4MiB, 1.02GiB.
pub(crate) fn bytes(kib: i64) -> String {
    if kib < 0 {
        return dash();
    }

    const STEP: f64 = 1024.0;
    let (value, unit) = match kib as f64 {
        v if v < STEP => (v, "KiB"),
        v if v < STEP * STEP => (v / STEP, "MiB"),
        v => (v / (STEP * STEP), "GiB"),
    };

    if value >= 100.0 {
        format!("{value:.0}{unit}")
    } else if value >= 10.0 {
        format!("{value:.1}{unit}")
    } else {
        format!("{value:.2}{unit}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_picks_a_unit_at_each_boundary() {
        assert_eq!(bytes(0), "0.00KiB");
        assert_eq!(bytes(1023), "1023KiB");
        assert_eq!(bytes(1024), "1.00MiB");
        assert_eq!(bytes(1024 * 1024 - 1), "1024MiB");
        assert_eq!(bytes(1024 * 1024), "1.00GiB");
    }

    #[test]
    fn bytes_keeps_three_figures_across_the_precision_switches() {
        // under 10 gets two decimals, 10 to 100 gets one, 100 and over gets none
        assert_eq!(bytes(9 * 1024), "9.00MiB");
        assert_eq!(bytes(10 * 1024), "10.0MiB");
        assert_eq!(bytes(99 * 1024), "99.0MiB");
        assert_eq!(bytes(100 * 1024), "100MiB");
    }

    #[test]
    fn bytes_rounds_up_into_an_extra_figure_just_below_a_switch() {
        // 99.98MiB is under the cutoff, so it takes the one-decimal branch and
        // rounds to a four-figure "100.0MiB". a character wider than the rest of
        // the column, and the only place this happens
        assert_eq!(bytes(99 * 1024 + 1013), "100.0MiB");
    }

    #[test]
    fn bytes_has_nothing_to_say_about_a_negative() {
        assert_eq!(bytes(-1), "-");
    }

    #[test]
    fn cpu_pct_truncates_rather_than_rounding() {
        // gnu time divides two integers, so 199.9% reads 199%, not 200%
        assert_eq!(cpu_pct(1.999, Some(1.0)), "199%");
        assert_eq!(cpu_pct(0.9999, Some(1.0)), "99%");
        assert_eq!(cpu_pct(4.0, Some(1.0)), "400%");
    }

    #[test]
    fn cpu_pct_needs_a_clock_to_divide_by() {
        assert_eq!(cpu_pct(1.0, None), "-");
        assert_eq!(cpu_pct(1.0, Some(0.0)), "-");
    }

    #[test]
    fn secs_is_two_places_or_a_dash() {
        assert_eq!(secs(Some(1.5)), "1.50");
        assert_eq!(secs(Some(0.004)), "0.00");
        assert_eq!(secs(None), "-");
    }
}
