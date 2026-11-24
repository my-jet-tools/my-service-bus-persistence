use std::{sync::Arc, time::Duration};
mod app;

mod archive_storage;
mod cold_storage;

mod file_storage;
mod grpc;
mod http;
mod index_by_minute;
mod message_pages;
mod operations;

mod settings;
mod timers;
mod topic_data;
mod topic_key;
mod topics_snapshot;
mod utils;
#[cfg(windows)]
mod win_service;

use rust_extensions::MyTimer;
mod typing;

use crate::{
    app::AppContext,
    settings::SettingsModel,
    timers::{
        cold_storage_uploader::ColdStorageUploaderTimer, metrics_updater::MetricsUpdater,
        pages_gc::PagesGcTimer, save_min_index::SaveMinIndexTimer,
        topics_snapshot_saver::TopicsSnapshotSaverTimer,
    },
};
#[allow(non_snake_case)]
pub mod persistence_grpc {
    tonic::include_proto!("persistence");
}

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Read by jemalloc before `main`: a background thread purges freed pages back to the OS
// after ~1s (dirty) and skips the MADV_FREE stage (muzzy), even when the service is idle.
// tikv-jemalloc-sys builds prefixed by default, hence `_rjem_malloc_conf`.
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: Option<&'static std::ffi::c_char> =
    Some(unsafe { &*c"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0".as_ptr() });

/// Linux and macOS: the service in the console, until SIGTERM or Ctrl+C.
#[cfg(not(windows))]
#[tokio::main]
async fn main() {
    run_service(|_| {}).await;
}

/// Windows: the console run, and the service as a Windows service - see `win_service`.
#[cfg(windows)]
fn main() {
    win_service::main();
}

/// The whole life of the service: it reads its settings, comes up, calls `on_started` once its
/// timers and listeners run, waits for the shutdown (a signal, or `AppStates::set_shutting_down`)
/// and writes what it holds to disk before it returns. One function, so that the timers live
/// exactly as long as the service.
pub async fn run_service(on_started: impl FnOnce(Arc<AppContext>)) {
    let settings = SettingsModel::read().await;

    // Two phases, both before anything reads the snapshot or opens a topic. The first only brings
    // over what is needed to serve; the rest follows in the background once the service is up.
    let legacy_migration = settings
        .legacy
        .as_ref()
        .map(|legacy| operations::LegacyMigration::new(settings.data.clone(), legacy.clone()));

    if let Some(migration) = legacy_migration.as_ref() {
        migration.move_working_files().await;
    }

    operations::migrate_legacy_layout(settings.data.as_str()).await;

    let app = AppContext::new(settings).await;

    let app = Arc::new(app);

    let mut timer_3s = MyTimer::new(Duration::from_secs(3), my_logger::LOGGER.clone());

    timer_3s.register_timer(
        "TopicsSnapshotSaver",
        Arc::new(TopicsSnapshotSaverTimer::new(app.clone())),
    );

    timer_3s.register_timer(
        "SaveMinIndexTimer",
        Arc::new(SaveMinIndexTimer::new(app.clone())),
    );

    timer_3s.start();

    let mut timer_persist_queues = MyTimer::new(Duration::from_secs(1), my_logger::LOGGER.clone());

    timer_persist_queues.register_timer("PagesGc", Arc::new(PagesGcTimer::new(app.clone())));

    timer_persist_queues.start();

    // TODO: re-enable DeletedTopicsGc timer once soft-delete + GC flow is reimplemented (see TODO.md)
    // let mut timer_30s = MyTimer::new(Duration::from_secs(30), my_logger::LOGGER.clone());
    // timer_30s.register_timer(
    //     "DeletedTopicsGc",
    //     Arc::new(DeletedTopicsGcTimer::new(app.clone())),
    // );
    // timer_30s.start();

    let http_connections_counter = crate::http::start_up::setup_server(&app, 7123);

    let mut timer_1s = MyTimer::new(Duration::from_secs(1), my_logger::LOGGER.clone());
    timer_1s.register_timer(
        "MetricsUpdater",
        Arc::new(MetricsUpdater::new(app.clone(), http_connections_counter)),
    );
    timer_1s.start();

    let mut timer_60s = MyTimer::new(Duration::from_secs(60), my_logger::LOGGER.clone());
    timer_60s.register_timer(
        "ColdStorageUploader",
        Arc::new(ColdStorageUploaderTimer::new(app.clone())),
    );
    timer_60s.start();

    if let Some(migration) = legacy_migration {
        let app = app.clone();
        tokio::spawn(async move {
            migration
                .move_the_rest(app.get_cold_storage().map(|itm| itm.as_ref()))
                .await;
        });
    }

    tokio::spawn(operations::data_initializer::init(app.clone()));

    tokio::spawn(grpc::server::start(
        app.clone(),
        app.settings.listen_ip(),
        7124,
    ));

    #[cfg(unix)]
    if let Some(unix_socket_addr) = app.settings.listen_unix_socket.clone() {
        tokio::spawn(grpc::server::start_unix_socket(
            app.clone(),
            unix_socket_addr,
        ));
    }

    // There are no unix sockets to listen on here: a setting which asks for one is said out loud
    // rather than ignored, and the TCP port serves.
    #[cfg(not(unix))]
    if let Some(unix_socket_addr) = app.settings.listen_unix_socket.as_ref() {
        println!(
            "listen_unix_socket '{unix_socket_addr}' is ignored: unix sockets are not supported on this platform"
        );
    }

    on_started(app.clone());

    app.app_states.wait_until_shutdown().await;

    crate::operations::before_shut_down::execute_before_shutdown(app).await;
}
