//! The `main` of Windows: the console run, and the service as a Windows service - the command
//! line and the install checks of my-reverse-proxy's `win_service`, which this module follows. No
//! option runs the service in the console; `--install`, `--uninstall` and `--start` manage it as a
//! Windows service and exit; `--run-as-service` is what the installed service's command line
//! invokes. `--settings <file>` names the settings file, read instead of
//! `%HOME%\.myservicebus-persistence`, and its folder becomes the home directory - what a leading
//! `~` in the settings (the `data` folder, mostly) stands for.
//!
//! A service has no console: what it prints goes to `logs\my-service-bus-persistence.log` in the
//! folder of the settings file (the log of the previous start is kept as `.prev.log`).
//!
//! The service has no authentication of its own: its gRPC port reads, writes and deletes every
//! message, and its HTTP port reads them. On Windows it is meant to run with
//! `listen_ip: 127.0.0.1`, next to the one bus node that uses it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use win_service_host::cli::{CliArgs, Mode};
use win_service_host::ServiceAccount;

use crate::app::AppContext;

mod console_log;
mod file_access;
mod harden;

/// `NT SERVICE\<service name>`, an account of this service alone with the rights of a plain
/// user: the service needs nothing of the machine but its own folders and two ports.
const DEFAULT_SERVICE_ACCOUNT: ServiceAccount = ServiceAccount::Virtual;

const DEFAULT_SERVICE_NAME: &str = "my-service-bus-persistence";

const DEFAULT_SERVICE_DESCRIPTION: &str =
    "my-service-bus-persistence: keeps the topics, the queues and the messages of my-service-bus \
     in files (gRPC 7124 for the bus, HTTP 7123 for reads and the UI). The settings file is \
     named in the command line of the service (--settings).";

/// What the binary is called in the usage text.
const APP_NAME: &str = "my-sb-persistence";

pub fn main() {
    let args = match win_service_host::cli::parse_args(APP_NAME, DEFAULT_SERVICE_NAME) {
        Ok(args) => args,
        Err(usage) => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    };

    let result = match args.mode {
        Mode::Console => console(args.settings.as_deref()),
        Mode::Install => install(&args),
        Mode::Uninstall => win_service_host::uninstall(&args.name),
        Mode::Start => win_service_host::start(&args.name),
        Mode::RunAsService => run_as_service(args),
    };

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

/// No option: the service in the console, until Ctrl+C - with the settings file `--settings`
/// names.
fn console(settings_file: Option<&str>) -> Result<(), String> {
    harden::harden_process().map_err(|err| format!("Can not start. {err}"))?;

    if let Some(settings_file) = settings_file {
        use_settings_file(settings_file).map_err(|err| format!("Can not start. {err}"))?;
    }

    let runtime = tokio::runtime::Runtime::new().expect("Can not build the tokio runtime");
    runtime.block_on(crate::run_service(|_| {}));

    Ok(())
}

/// Makes `file` the settings file of the process and its folder the home directory. The file has
/// to exist: a wrong path is found out here, before anything else starts.
fn use_settings_file(file: &str) -> Result<(), String> {
    let file = settings_file_path(file)?;

    // `~` in a path of the settings is expanded from HOME (rust-extensions' `format_path`).
    std::env::set_var("HOME", folder_of(&file)?);

    crate::settings::set_settings_file(file.to_string_lossy().to_string())
}

fn settings_file_path(file: &str) -> Result<PathBuf, String> {
    let file = std::path::absolute(file)
        .map_err(|err| format!("Can not resolve the settings file path {file}: {err}"))?;
    let file = PathBuf::from(strip_verbatim_prefix(&file.to_string_lossy()));

    if !file.is_file() {
        return Err(format!("The settings file {} does not exist", file.display()));
    }

    Ok(file)
}

/// `--install`: registers this exe with the Service Control Manager - auto-start at boot,
/// restart-on-failure, a command line with `--run-as-service` and the settings file. The settings
/// file is required here, as `--settings` or as `settings.yaml` next to the exe.
///
/// Before that, the folders the service takes its exe and its settings from, and the `data` folder
/// it keeps the messages in, are checked: one which a plain user can write to is a way to the
/// rights of the service, and one a plain user can read gives them every message. Either refuses
/// the install. After it, the account of the service is given these folders.
fn install(args: &CliArgs) -> Result<(), String> {
    let settings_near_exe = win_service_host::cli::resolve_settings_file_near_exe();

    let settings_file = match args.settings.as_deref() {
        Some(file) => std::fs::canonicalize(file)
            .map_err(|err| format!("settings file '{file}' is not readable: {err}"))?,
        None => settings_near_exe.clone().ok_or_else(|| {
            "--install needs the settings file: --settings <file>, or a settings.yaml next to the \
             exe. The service does not run as you, so the .myservicebus-persistence of your \
             profile would not be read."
                .to_string()
        })?,
    };

    let exe = std::env::current_exe()
        .map_err(|err| format!("Can not determine the path of this exe: {err}"))?;

    let settings_file = PathBuf::from(strip_verbatim_prefix(&settings_file.to_string_lossy()));
    let settings_folder = folder_of(&settings_file)?;
    let exe_folder = folder_of(&exe)?;

    // The data folder as the service will see it: `~` is the folder of the settings file.
    std::env::set_var("HOME", &settings_folder);
    let data_folder = PathBuf::from(crate::settings::SettingsModel::data_folder_of(
        &settings_file,
    )?);

    if !data_folder.is_dir() {
        return Err(format!(
            "--install is refused: the data folder {} does not exist. Make it, close it, and \
             install again:\n  mkdir \"{}\"\n{}",
            data_folder.display(),
            data_folder.display(),
            close_folder_commands(&data_folder)
        ));
    }

    check_access(&settings_folder, &settings_file, &exe_folder, &exe)?;
    check_data_folder(&data_folder)?;

    let mut folders = folders_to_keep_in_place(&settings_folder, &exe_folder, &exe)?;
    if !folders.contains(&data_folder) {
        folders.push(data_folder.clone());
    }
    check_folders_above(&folders)?;

    let account = args.account.unwrap_or(DEFAULT_SERVICE_ACCOUNT);

    win_service_host::install(win_service_host::InstallSpec {
        account,
        name: &args.name,
        description: args
            .description
            .as_deref()
            .unwrap_or(DEFAULT_SERVICE_DESCRIPTION),
        settings: args.settings.as_deref(),
        settings_near_exe,
    })?;

    if let Some(account) = account.account_name(&args.name) {
        grant_folders(account.as_str(), &settings_folder, &exe_folder, &data_folder);
    }

    Ok(())
}

/// The folder of a file, without the verbatim prefix `canonicalize` gives a path.
fn folder_of(file: &Path) -> Result<PathBuf, String> {
    let folder = file
        .parent()
        .ok_or_else(|| format!("{} is not in a folder", file.display()))?;

    Ok(PathBuf::from(strip_verbatim_prefix(&folder.to_string_lossy())))
}

/// `\\?\C:\x` is `C:\x` and `\\?\UNC\server\share\x` is `\\server\share\x`; any other path is
/// kept.
fn strip_verbatim_prefix(path: &str) -> String {
    match path.strip_prefix(r"\\?\UNC\") {
        Some(rest) => format!(r"\\{rest}"),
        None => path.strip_prefix(r"\\?\").unwrap_or(path).to_string(),
    }
}

/// Refuses folders and files a plain account can change, with the commands which close them;
/// warns about a settings folder a plain account can read - the logs are in it.
fn check_access(
    settings_folder: &Path,
    settings_file: &Path,
    exe_folder: &Path,
    exe: &Path,
) -> Result<(), String> {
    let mut open_folders: Vec<&Path> = Vec::new();
    let mut problems = String::new();

    for (folder, file) in [(settings_folder, settings_file), (exe_folder, exe)] {
        let mut writers = file_access::read(folder)?.findings().writers;
        for writer in file_access::read(file)?.findings().writers {
            if !writers.contains(&writer) {
                writers.push(writer);
            }
        }

        if !writers.is_empty() && !open_folders.contains(&folder) {
            open_folders.push(folder);
            problems.push_str(&format!(
                "  {} can be changed by: {}\n",
                folder.display(),
                writers.join(", ")
            ));
        }
    }

    if !open_folders.is_empty() {
        let mut commands = String::new();
        for folder in open_folders {
            commands.push_str(&close_folder_commands(folder));
        }

        return Err(format!(
            "--install is refused: the service takes its exe and its settings from folders which \
             accounts other than the system and the administrators can change - whoever changes \
             them runs what they like with the rights of the service.\n\
             {problems}\
             Close them, in an elevated prompt, and install again:\n{commands}"
        ));
    }

    let readers = file_access::read(settings_folder)?.findings().readers;
    if !readers.is_empty() {
        println!(
            "WARNING: {} can be read by: {}. The logs of the service, with the names of its \
             topics, are written there; the delete key of the settings is in it. To close the \
             folder:\n{}",
            settings_folder.display(),
            readers.join(", "),
            close_folder_commands(settings_folder)
        );
    }

    Ok(())
}

/// The data folder holds every message the bus ever kept: a plain account which can read it reads
/// them, and one which can write it changes what the subscribers will get.
fn check_data_folder(data_folder: &Path) -> Result<(), String> {
    let findings = file_access::read(data_folder)?.findings();

    let mut accounts = findings.writers;
    for reader in findings.readers {
        if !accounts.contains(&reader) {
            accounts.push(reader);
        }
    }

    if accounts.is_empty() {
        return Ok(());
    }

    Err(format!(
        "--install is refused: the data folder {} - every message of the bus - can be read or \
         changed by: {}.\nClose it, in an elevated prompt, and install again:\n{}",
        data_folder.display(),
        accounts.join(", "),
        close_folder_commands(data_folder)
    ))
}

/// The folders which have to stay where they are: the ones of the settings and of the exe, and
/// the folder the exe is really in when its path goes through a link.
fn folders_to_keep_in_place(
    settings_folder: &Path,
    exe_folder: &Path,
    exe: &Path,
) -> Result<Vec<PathBuf>, String> {
    let real_exe = std::fs::canonicalize(exe)
        .map_err(|err| format!("the exe {} is not readable: {err}", exe.display()))?;

    let mut folders = vec![settings_folder.to_path_buf()];
    for folder in [exe_folder.to_path_buf(), folder_of(&real_exe)?] {
        if !folders.contains(&folder) {
            folders.push(folder);
        }
    }

    Ok(folders)
}

/// The folders above `folder`, up to the root of its drive.
fn folders_above(folder: &Path) -> Vec<&Path> {
    folder
        .ancestors()
        .skip(1)
        .filter(|above| !above.as_os_str().is_empty())
        .collect()
}

/// Refuses folders of the service which a plain account can move away: one who can rename a
/// folder above puts a folder of their own at the path the service is started from - or takes
/// the data folder away with everything in it.
fn check_folders_above(folders: &[PathBuf]) -> Result<(), String> {
    let mut checked: Vec<&Path> = Vec::new();
    let mut problems = String::new();

    for folder in folders {
        for above in folders_above(folder) {
            if checked.contains(&above) {
                continue;
            }
            checked.push(above);

            let movers = file_access::read(above)?.movers(above.parent().is_none());
            if !movers.is_empty() {
                problems.push_str(&format!(
                    "  {} - by: {}\n",
                    above.display(),
                    movers.join(", ")
                ));
            }
        }
    }

    if problems.is_empty() {
        return Ok(());
    }

    Err(format!(
        "--install is refused: a folder above the folders of the service can be renamed, or \
         have what is in it renamed, by accounts other than the system and the administrators \
         - whoever does it puts their own exe and settings at the path the service is started \
         from, or carries the data folder away.\n\
         {problems}\
         Put the exe, the settings and the data where only the administrators change the \
         folders above - a folder of its own in the root of the drive, or in %ProgramFiles% - \
         and install again.\n"
    ))
}

/// The owner becomes the administrators; the folder stops inheriting, and is given to the system
/// and the administrators alone. The SIDs are written out: the names of these groups differ with
/// the language of Windows.
fn close_folder_commands(folder: &Path) -> String {
    let folder = folder.display();
    format!(
        "  icacls \"{folder}\" /setowner *S-1-5-32-544 /T /C\n  \
         icacls \"{folder}\" /inheritance:r /grant:r \"*S-1-5-18:(OI)(CI)F\" \"*S-1-5-32-544:(OI)(CI)F\"\n  \
         icacls \"{folder}\\*\" /reset /T /C\n"
    )
}

/// Gives the account of the service its folders: the settings folder and the exe folder to read,
/// `logs\` of the settings folder and the data folder to write. The service can not change its
/// own settings: one taken over can not widen its own `listen_ip`. A grant which fails does not
/// undo the install: the command is printed for the operator.
fn grant_folders(account: &str, settings_folder: &Path, exe_folder: &Path, data_folder: &Path) {
    let logs_folder = settings_folder.join("logs");
    if let Err(err) = std::fs::create_dir_all(&logs_folder) {
        println!(
            "WARNING: the log folder {} can not be made: {err}",
            logs_folder.display()
        );
    }

    let mut grants = vec![
        (settings_folder, "(OI)(CI)RX"),
        (logs_folder.as_path(), "(OI)(CI)M"),
    ];
    if data_folder != settings_folder {
        grants.push((data_folder, "(OI)(CI)M"));
    }
    if exe_folder != settings_folder && exe_folder != data_folder {
        grants.push((exe_folder, "(OI)(CI)RX"));
    }

    let icacls = std::env::var("SystemRoot")
        .map(|root| format!(r"{root}\System32\icacls.exe"))
        .unwrap_or_else(|_| "icacls.exe".to_string());

    for (folder, rights) in grants {
        let grant = format!("{account}:{rights}");

        let granted = std::process::Command::new(icacls.as_str())
            .arg(folder)
            .arg("/grant")
            .arg(grant.as_str())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);

        if granted {
            println!("{account} is given {}: {rights}", folder.display());
        } else {
            println!(
                "WARNING: {account} could not be given {}; the service will not start without \
                 it. Run:  icacls \"{}\" /grant \"{grant}\"",
                folder.display(),
                folder.display()
            );
        }
    }
}

/// `--run-as-service`: the service host speaks the SCM protocol and calls back in to bring the
/// service up, and to take it down when the SCM says stop.
fn run_as_service(args: CliArgs) -> Result<(), String> {
    // A service starts in C:\Windows\System32. The UI (wwwroot\) is read relative to the working
    // directory, so make it the folder of the exe - where a release puts it, next to it.
    set_current_dir_to_exe_folder()?;

    let settings_file = args.settings;
    let service_name = args.name.clone();

    win_service_host::run_as_service(
        args.name,
        Box::new(move || {
            Box::pin(async move {
                let settings_file = settings_file.ok_or_else(|| {
                    "The service was installed without --settings: reinstall it with one"
                        .to_string()
                })?;

                use_settings_file(settings_file.as_str())?;

                let log_folder = folder_of(Path::new(settings_file.as_str()))?.join("logs");
                console_log::write_console_to(&log_folder, service_name.as_str())?;

                harden::harden_process()?;

                let service = start_service().await?;

                let shutdown: win_service_host::ShutdownFn = Box::new(move || {
                    Box::pin(async move {
                        println!("The service is asked to stop");
                        service.stop().await;
                        println!("The service is stopped");
                    })
                });

                Ok(shutdown)
            })
        }),
    )
}

/// The service, running on a task of its own: `run_service` returns only when it is down again.
struct StartedService {
    app: Arc<AppContext>,
    stopped: tokio::sync::oneshot::Receiver<()>,
}

impl StartedService {
    /// Says shut down, and waits until everything is written to disk.
    async fn stop(self) {
        self.app.app_states.set_shutting_down();
        let _ = self.stopped.await;
    }
}

async fn start_service() -> Result<StartedService, String> {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        crate::run_service(move |app| {
            let _ = started_tx.send(app);
        })
        .await;

        let _ = stopped_tx.send(());
    });

    // A service which ends before it is up has panicked - on its settings or its data folder,
    // mostly - and the panic is in the log.
    let app = started_rx.await.map_err(|_| {
        "The service stopped while it was starting - the reason is in its log".to_string()
    })?;

    Ok(StartedService {
        app,
        stopped: stopped_rx,
    })
}

fn set_current_dir_to_exe_folder() -> Result<(), String> {
    let exe = std::env::current_exe()
        .map_err(|err| format!("Can not determine the path of this exe: {err}"))?;

    let folder = exe
        .parent()
        .ok_or_else(|| format!("The exe {} is not in a folder", exe.display()))?;

    std::env::set_current_dir(folder).map_err(|err| {
        format!(
            "Can not make {} the working directory: {err}",
            folder.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_folders_above_end_with_the_root_of_the_drive() {
        assert_eq!(
            folders_above(Path::new(r"D:\sb\data")),
            vec![Path::new(r"D:\sb"), Path::new(r"D:\")]
        );
        assert!(folders_above(Path::new(r"D:\")).is_empty());
    }

    /// The temp folder of a user is theirs to rename, whatever a folder made in it is closed to.
    #[test]
    fn a_folder_in_the_profile_of_a_user_is_refused() {
        let folder = std::env::temp_dir().join("my-sb-persistence-service");

        let err = check_folders_above(&[folder]).unwrap_err();
        assert!(err.contains("--install is refused"), "{err}");
    }

    /// The temp folder of a user is readable by its user: as a data folder it is refused.
    #[test]
    fn a_data_folder_a_plain_account_can_read_is_refused() {
        let folder = std::env::temp_dir();

        let err = check_data_folder(&folder).unwrap_err();
        assert!(err.contains("--install is refused"), "{err}");
    }
}
