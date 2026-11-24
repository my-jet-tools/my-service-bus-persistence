//! A Windows service has no console: what the process prints - its own lines, the logger's, a
//! panic - goes nowhere. This points the standard output and error of the process at a file, so
//! that the log of the service is a file next to its settings.
//!
//! Rust's `print!` asks Windows for the standard handle on every write, so changing the handle
//! here moves every later line, whoever prints it.

use std::os::windows::io::IntoRawHandle;
use std::path::Path;

use windows_sys::Win32::System::Console::{SetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE};

/// Writes the standard output and error of the process to `<folder>\<name>.log`, from now until
/// the process ends. The log of the previous start is kept as `<name>.prev.log`; older ones go.
pub fn write_console_to(folder: &Path, name: &str) -> Result<(), String> {
    std::fs::create_dir_all(folder)
        .map_err(|err| format!("The log folder {} can not be made: {err}", folder.display()))?;

    let file = folder.join(format!("{name}.log"));
    if file.exists() {
        let previous = folder.join(format!("{name}.prev.log"));
        // A previous log which can not be moved is written after: nothing is lost either way.
        let _ = std::fs::rename(&file, &previous);
    }

    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file)
        .map_err(|err| format!("The log {} can not be opened: {err}", file.display()))?;

    // The handle is given to the process for the rest of its life: it is never closed.
    let handle = log.into_raw_handle();

    // SAFETY: `handle` is an open file handle owned by nobody else from here on.
    let set = unsafe {
        SetStdHandle(STD_OUTPUT_HANDLE, handle) != 0 && SetStdHandle(STD_ERROR_HANDLE, handle) != 0
    };

    if !set {
        return Err(format!(
            "The output of the service can not be pointed at {}: {}",
            file.display(),
            std::io::Error::last_os_error()
        ));
    }

    println!("The log of {name} is {}", file.display());

    Ok(())
}
