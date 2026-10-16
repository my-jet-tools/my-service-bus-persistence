/// Namespaces isolate topics from each other: a topic name is unique only within its namespace,
/// so `orders` in `default` and `orders` in `alpha` are two different topics with independent
/// messages, queues and cursors.
///
/// A missing or empty value always means the `default` namespace - that is exactly what a node
/// which knows nothing about namespaces sends, and what every blob written before namespaces
/// existed decodes into.
pub const DEFAULT_NAMESPACE: &str = "default";

pub const NAMESPACE_MAX_LEN: usize = 63;

#[derive(Debug)]
pub enum NamespaceError {
    TooLong { len: usize },
    StartsWithDash,
    InvalidChar { invalid_char: char },
}

impl NamespaceError {
    pub fn as_string(&self) -> String {
        match self {
            NamespaceError::TooLong { len } => format!(
                "Namespace is {} chars long. Maximum is {}",
                len, NAMESPACE_MAX_LEN
            ),
            NamespaceError::StartsWithDash => "Namespace can not start with '-'".to_string(),
            NamespaceError::InvalidChar { invalid_char } => format!(
                "Namespace contains invalid char '{}'. Allowed chars are [a-z0-9-]",
                invalid_char
            ),
        }
    }
}

impl std::fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

/// A validated namespace name. Always non-empty: an absent/empty source value is normalized
/// to [`DEFAULT_NAMESPACE`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Namespace(String);

impl Namespace {
    pub fn default_namespace() -> Self {
        Self(DEFAULT_NAMESPACE.to_string())
    }

    /// `None` and `""` both resolve to the `default` namespace.
    ///
    /// The name itself is validated by the bus node; we re-validate here because the value ends
    /// up in physical storage keys and a malformed one must never reach them.
    pub fn parse(src: Option<&str>) -> Result<Self, NamespaceError> {
        let src = match src {
            Some(src) => src,
            None => return Ok(Self::default_namespace()),
        };

        if src.is_empty() {
            return Ok(Self::default_namespace());
        }

        if src.len() > NAMESPACE_MAX_LEN {
            return Err(NamespaceError::TooLong { len: src.len() });
        }

        if src.starts_with('-') {
            return Err(NamespaceError::StartsWithDash);
        }

        for invalid_char in src.chars() {
            let is_valid = invalid_char.is_ascii_lowercase()
                || invalid_char.is_ascii_digit()
                || invalid_char == '-';

            if !is_valid {
                return Err(NamespaceError::InvalidChar { invalid_char });
            }
        }

        Ok(Self(src.to_string()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn is_default(&self) -> bool {
        self.0 == DEFAULT_NAMESPACE
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What we put into a persisted (protobuf) namespace field.
///
/// The `default` namespace is written as an empty string so a deployment which does not use
/// namespaces keeps producing byte-for-byte the same blobs as before namespaces existed.
pub fn namespace_to_persist(namespace: &str) -> String {
    if namespace == DEFAULT_NAMESPACE {
        return String::new();
    }

    namespace.to_string()
}

/// What we put into an `optional string Namespace` gRPC field. `None` for `default`, so the
/// wire stays identical to the pre-namespace contract for nodes which never send it.
pub fn namespace_to_grpc(namespace: &str) -> Option<String> {
    if namespace == DEFAULT_NAMESPACE {
        return None;
    }

    Some(namespace.to_string())
}

/// How a persisted namespace field reads back: empty means `default`.
pub fn namespace_from_persisted(src: &str) -> &str {
    if src.is_empty() {
        return DEFAULT_NAMESPACE;
    }

    src
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_empty_are_default() {
        assert!(Namespace::parse(None).unwrap().is_default());
        assert!(Namespace::parse(Some("")).unwrap().is_default());
        assert!(Namespace::parse(Some(DEFAULT_NAMESPACE))
            .unwrap()
            .is_default());
    }

    #[test]
    fn valid_names() {
        assert_eq!("alpha", Namespace::parse(Some("alpha")).unwrap().as_str());
        assert_eq!("a-b-1", Namespace::parse(Some("a-b-1")).unwrap().as_str());
        assert_eq!("9", Namespace::parse(Some("9")).unwrap().as_str());
    }

    #[test]
    fn invalid_names() {
        assert!(Namespace::parse(Some("-alpha")).is_err());
        assert!(Namespace::parse(Some("Alpha")).is_err());
        assert!(Namespace::parse(Some("al_pha")).is_err());
        assert!(Namespace::parse(Some("al/pha")).is_err());
        assert!(Namespace::parse(Some("a".repeat(NAMESPACE_MAX_LEN + 1).as_str())).is_err());
        assert!(Namespace::parse(Some("a".repeat(NAMESPACE_MAX_LEN).as_str())).is_ok());
    }

    #[test]
    fn default_namespace_round_trips_as_empty() {
        assert_eq!("", namespace_to_persist(DEFAULT_NAMESPACE));
        assert_eq!("alpha", namespace_to_persist("alpha"));

        assert_eq!(DEFAULT_NAMESPACE, namespace_from_persisted(""));
        assert_eq!("alpha", namespace_from_persisted("alpha"));

        assert_eq!(None, namespace_to_grpc(DEFAULT_NAMESPACE));
        assert_eq!(Some("alpha".to_string()), namespace_to_grpc("alpha"));
    }
}

/// A topic id is chosen by the bus node and ends up as a **path component** and an S3 key
/// segment, so it has to be checked before it gets anywhere near storage: `../` in a topic id
/// would otherwise walk out of its namespace folder, which is exactly the isolation the whole
/// namespace design exists to provide.
#[derive(Debug)]
pub enum TopicIdError {
    Empty,
    TooLong { len: usize },
    PathSeparator,
    RelativePath,
    InvalidChar { invalid_char: char },
}

impl TopicIdError {
    pub fn as_string(&self) -> String {
        match self {
            TopicIdError::Empty => "Topic id is empty".to_string(),
            TopicIdError::TooLong { len } => format!(
                "Topic id is {} chars long. Maximum is {}",
                len, TOPIC_ID_MAX_LEN
            ),
            TopicIdError::PathSeparator => "Topic id can not contain a path separator".to_string(),
            TopicIdError::RelativePath => "Topic id can not be '.' or '..'".to_string(),
            TopicIdError::InvalidChar { invalid_char } => {
                format!("Topic id contains invalid char '{}'", invalid_char)
            }
        }
    }
}

impl std::fmt::Display for TopicIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

pub const TOPIC_ID_MAX_LEN: usize = 255;

pub fn validate_topic_id(topic_id: &str) -> Result<(), TopicIdError> {
    if topic_id.is_empty() {
        return Err(TopicIdError::Empty);
    }

    if topic_id.len() > TOPIC_ID_MAX_LEN {
        return Err(TopicIdError::TooLong {
            len: topic_id.len(),
        });
    }

    if topic_id == "." || topic_id == ".." {
        return Err(TopicIdError::RelativePath);
    }

    for invalid_char in topic_id.chars() {
        if invalid_char == '/' || invalid_char == '\\' {
            return Err(TopicIdError::PathSeparator);
        }

        if invalid_char.is_control() || invalid_char == '\0' {
            return Err(TopicIdError::InvalidChar { invalid_char });
        }
    }

    Ok(())
}

#[cfg(test)]
mod topic_id_tests {
    use super::*;

    #[test]
    fn a_topic_id_can_not_walk_out_of_its_namespace() {
        assert!(validate_topic_id("../other-namespace/orders").is_err());
        assert!(validate_topic_id("..").is_err());
        assert!(validate_topic_id(".").is_err());
        assert!(validate_topic_id("a/b").is_err());
        assert!(validate_topic_id("a\\b").is_err());
        assert!(validate_topic_id("").is_err());
        assert!(validate_topic_id("a\0b").is_err());
        assert!(validate_topic_id("a".repeat(TOPIC_ID_MAX_LEN + 1).as_str()).is_err());
    }

    #[test]
    fn ordinary_topic_ids_pass() {
        assert!(validate_topic_id("orders").is_ok());
        assert!(validate_topic_id("Orders-2024.v1_x").is_ok());
        assert!(validate_topic_id("a").is_ok());
    }
}
