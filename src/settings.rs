use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncReadExt};

/// Where sealed archives and closed year indexes are uploaded.
///
/// Expressed as one connection string, in the same `Key=Value;Key=Value` shape the Azure ones
/// used, so there is no new convention to learn:
///
/// ```text
/// s3_conn_string: "Endpoint=https://fsn1.your-objectstorage.com;Region=fsn1;AccessKey=...;SecretKey=...;Bucket=sb-data"
/// ```
///
/// Leave the whole setting out and nothing is ever uploaded: every file stays on the local disk
/// forever.
#[derive(Debug, Clone)]
pub struct S3ConnectionSettings {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// `Bucket=sb-data` - the one bucket for every namespace. Keys go under
    /// `/sb-data/my-sb-persistence/{namespace}/{topic}/{file}`.
    pub bucket: String,

    /// `Debug=1` - trace every S3 request to the console. Off unless asked for.
    ///
    /// Meant to be switched on in a running deployment without a rebuild, which is the only way to
    /// see why the cold tier is refusing a request: the failure is an XML `<Error><Code>` in the
    /// answer body, and nothing else surfaces it.
    pub debug: bool,
}

impl S3ConnectionSettings {
    pub fn parse(conn_string: &str) -> Self {
        let mut endpoint = None;
        let mut region = None;
        let mut access_key = None;
        let mut secret_key = None;
        let mut bucket = None;
        let mut debug = None;

        for pair in conn_string.split(';') {
            let pair = pair.trim();

            if pair.is_empty() {
                continue;
            }

            // Only the first `=` separates - a base64 secret key carries its own.
            let Some(separator) = pair.find('=') else {
                panic!("Invalid s3_conn_string: '{}' is not Key=Value", pair);
            };

            let key = pair[..separator].trim();
            let value = pair[separator + 1..].trim().to_string();

            match key {
                "Endpoint" => endpoint = Some(value),
                "Region" => region = Some(value),
                "AccessKey" => access_key = Some(value),
                "SecretKey" => secret_key = Some(value),
                "Bucket" => bucket = Some(value),
                "Debug" => debug = Some(parse_bool(value.as_str(), "Debug")),
                _ => panic!(
                    "Invalid s3_conn_string: unknown key '{}'. Expected Endpoint, Region, AccessKey, SecretKey, Bucket, and optionally Debug",
                    key
                ),
            }
        }

        Self {
            endpoint: required(endpoint, "Endpoint"),
            region: required(region, "Region"),
            access_key: required(access_key, "AccessKey"),
            secret_key: required(secret_key, "SecretKey"),
            bucket: required(bucket, "Bucket"),
            debug: debug.unwrap_or(false),
        }
    }
}

/// Spelled out rather than `== "1"`: the setting is typed by hand into a deployment config, and a
/// `Debug=true` that silently means "off" is worse than a refusal to start.
fn parse_bool(value: &str, key: &str) -> bool {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => panic!(
            "Invalid s3_conn_string: '{}' expects 1/0, true/false, yes/no or on/off - got '{}'",
            key, value
        ),
    }
}

fn required(value: Option<String>, key: &str) -> String {
    match value {
        Some(value) if !value.is_empty() => value,
        _ => panic!("Invalid s3_conn_string: '{}' is missing", key),
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SettingsModel {
    /// Root of every file this service owns. One root - the archive, the year index, the open tail
    /// of a topic and that namespace's snapshot all live under `{data}/{namespace}/`.
    pub data: String,

    pub max_response_records_amount: usize,
    pub delete_topic_secret_key: String,

    pub listen_unix_socket: Option<String>,

    pub s3_conn_string: Option<String>,

    /// The three folders the service used before everything moved under one root. Set the section
    /// only for the first start after upgrading; delete it once the migration has finished.
    ///
    /// Either the whole section is absent or all three folders are given - none of the fields is
    /// optional, so a half-filled section fails to parse instead of migrating half the data.
    pub legacy: Option<LegacyFoldersSettingsModel>,
}

/// Contents are **moved**: a file is written to its new home and only then removed from the legacy
/// folder, so what is still there is exactly what has not been migrated yet.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LegacyFoldersSettingsModel {
    /// Held `topics/topicsdata`, `topics/.active-pages` and an empty folder per topic.
    pub topics: String,
    /// Held `{topic}/.{year}.yearindex`.
    pub messages: String,
    /// Held `{topic}/{:019}.archive`.
    pub archive: String,
}

impl SettingsModel {
    pub fn get_s3_connection(&self) -> Option<S3ConnectionSettings> {
        let conn_string = self.s3_conn_string.as_ref()?;

        if conn_string.is_empty() {
            return None;
        }

        Some(S3ConnectionSettings::parse(conn_string))
    }

    pub async fn read() -> Self {
        let filename = my_service_bus::shared::settings::get_settings_filename_path(
            ".myservicebus-persistence",
        );

        println!("Reading settings file {}", filename);

        let file = File::open(&filename).await;

        if let Err(err) = file {
            panic!(
                "Can not open settings file: {}. The reason is: {:?}",
                filename, err
            );
        }

        let mut file = file.unwrap();

        let mut file_content: Vec<u8> = Vec::new();

        loop {
            let res = file.read_buf(&mut file_content).await.unwrap();

            if res == 0 {
                break;
            }
        }

        let mut result: SettingsModel = serde_yaml::from_slice(file_content.as_slice()).unwrap();

        result.data = format_folder(result.data);

        if let Some(legacy) = result.legacy.as_mut() {
            legacy.topics = format_folder(legacy.topics.clone());
            legacy.messages = format_folder(legacy.messages.clone());
            legacy.archive = format_folder(legacy.archive.clone());
        }

        result
    }
}

fn format_folder(src: String) -> String {
    let mut result = rust_extensions::file_utils::format_path(src).to_string();

    while result.ends_with('/') {
        result.pop();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_connection_string() {
        let parsed = S3ConnectionSettings::parse(
            "Endpoint=https://s3.eu-central-1.amazonaws.com;Region=eu-central-1;AccessKey=AKIA123;SecretKey=abc/def+ghi=;Bucket=my-bucket",
        );

        assert_eq!("https://s3.eu-central-1.amazonaws.com", parsed.endpoint);
        assert_eq!("eu-central-1", parsed.region);
        assert_eq!("AKIA123", parsed.access_key);
        // A base64 secret carries its own '=' - only the first one separates
        assert_eq!("abc/def+ghi=", parsed.secret_key);
        assert_eq!("my-bucket", parsed.bucket);
    }

    /// Not a default to guess at: the bucket is where every object goes, and switching later means
    /// moving every object.
    #[test]
    #[should_panic(expected = "'Bucket' is missing")]
    fn a_missing_bucket_is_an_error() {
        S3ConnectionSettings::parse("Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b");
    }

    #[test]
    #[should_panic(expected = "'Region' is missing")]
    fn a_missing_key_is_loud() {
        S3ConnectionSettings::parse("Endpoint=https://s3;AccessKey=a;SecretKey=b;Bucket=c");
    }

    #[test]
    #[should_panic(expected = "unknown key")]
    fn a_typo_is_loud() {
        S3ConnectionSettings::parse(
            "Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b;Buckett=c",
        );
    }

    /// Off unless the connection string says otherwise - tracing every request is not something to
    /// end up with by accident.
    #[test]
    fn debug_is_off_unless_asked_for() {
        let parsed = S3ConnectionSettings::parse(
            "Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b;Bucket=c",
        );

        assert!(!parsed.debug);
    }

    #[test]
    fn debug_is_switched_on_by_the_connection_string() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            let parsed = S3ConnectionSettings::parse(
                format!(
                    "Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b;Bucket=c;Debug={}",
                    value
                )
                .as_str(),
            );

            assert!(parsed.debug, "'{}' should have switched debug on", value);
        }

        for value in ["0", "false", "no", "off"] {
            let parsed = S3ConnectionSettings::parse(
                format!(
                    "Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b;Bucket=c;Debug={}",
                    value
                )
                .as_str(),
            );

            assert!(!parsed.debug, "'{}' should have left debug off", value);
        }
    }

    /// A value that is neither is a typo, and quietly reading it as "off" would leave the operator
    /// waiting for a trace that never comes.
    #[test]
    #[should_panic(expected = "'Debug' expects 1/0")]
    fn an_unreadable_debug_value_is_loud() {
        S3ConnectionSettings::parse(
            "Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b;Bucket=c;Debug=maybe",
        );
    }

    #[test]
    fn trailing_separators_are_tolerated() {
        let parsed = S3ConnectionSettings::parse(
            "Endpoint=https://s3;Region=eu;AccessKey=a;SecretKey=b;Bucket=c;",
        );

        assert_eq!("c", parsed.bucket);
    }
}
