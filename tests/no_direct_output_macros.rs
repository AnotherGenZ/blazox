use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn rust_sources_do_not_use_direct_output_macros() {
    let mut files = Vec::new();
    for root in ["src", "tests", "examples"] {
        collect_rust_files(Path::new(root), &mut files);
    }

    let forbidden = [
        format!("{}!", "println"),
        format!("{}!", "eprintln"),
        format!("{}!", "print"),
        format!("{}!", "eprint"),
        format!("{}!", "dbg"),
        ["std", "::", "io", "::", "stdout"].concat(),
        ["std", "::", "io", "::", "stderr"].concat(),
    ];

    let mut offenders = Vec::new();
    for path in files {
        let contents = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        for pattern in &forbidden {
            if contents.contains(pattern) {
                offenders.push(format!("{} contains `{pattern}`", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "direct output macros are not allowed; use tracing instead:\n{}",
        offenders.join("\n")
    );
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
            continue;
        }
        if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}
