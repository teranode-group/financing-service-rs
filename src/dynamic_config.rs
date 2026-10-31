use std::path::Path;

use crate::config::{ClientConfig, Config};
use crate::secrets::is_secret_reference;

use serde::{Deserialize, Serialize};

// Represents the service's dynamically configurable elements

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct FileContents {
    // An empty file is a file with no clients, not a malformed one.
    #[serde(default)]
    pub clients: Vec<ClientConfig>,
}

pub struct DynamicConfig {
    filename: String,
    allow_plaintext_wif: bool,
    pub contents: FileContents,
}

/// The file's contents, or `None` when there is no file yet.
///
/// Anything else that stops it being read is an error (CS-526). Carrying on
/// with no dynamic clients, as this used to, dropped every one of them on a
/// bad hand edit or a truncated write -- and the next `POST /client` then
/// wrote the empty list back, deleting what may have been the only copy of
/// their WIFs.
fn read_dynamic_config(filename: &str) -> Result<Option<FileContents>, String> {
    let content = match std::fs::read_to_string(filename) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read dynamic config {filename}: {e}")),
    };
    toml::from_str(&content)
        .map(Some)
        .map_err(|e| format!("dynamic config {filename} does not parse: {e}"))
}

/// Writes the file atomically and owner-only (CS-526): it holds client WIFs.
fn save_dynamic_config(filename: &str, file_contents: &FileContents) -> Result<(), String> {
    let content =
        toml::to_string(file_contents).map_err(|e| format!("Failed to serialize config: {e}"))?;
    crate::private_file::write_private(Path::new(filename), content.as_bytes())
        .map_err(|e| format!("Failed to write config: {e}"))
}

impl DynamicConfig {
    /// Loads the dynamic clients. A missing file is a deployment with none
    /// yet; a file that cannot be read or parsed refuses startup (CS-526), so
    /// it is never silently replaced with an empty one.
    pub fn new(config: &Config) -> Result<Self, String> {
        let filename = config.dynamic_config.filename.clone();
        let contents = match read_dynamic_config(&filename)? {
            Some(contents) => {
                // Written before CS-526 under the umask, typically 0644.
                match crate::private_file::make_private(Path::new(&filename)) {
                    Ok(true) => log::warn!(
                        "dynamic config {filename} was readable by other users; it is now 0600"
                    ),
                    Ok(false) => {}
                    Err(e) => log::warn!(
                        "dynamic config {filename} may be readable by other users, and could \
                         not be made private: {e}"
                    ),
                }
                contents
            }
            None => FileContents::default(),
        };
        Ok(DynamicConfig {
            filename,
            allow_plaintext_wif: config.dynamic_config.allow_plaintext_wif,
            contents,
        })
    }

    /// Whether a client with this `wif_key` may be stored (CS-526).
    ///
    /// Only an `env:VAR` reference, unless `allow_plaintext_wif` is set: the
    /// file is a copy of the key that outlives the request, on a volume that
    /// may be bind-mounted from the host and backed up with it.
    pub fn accepts_wif(&self, wif_key: &str) -> bool {
        self.allow_plaintext_wif || is_secret_reference(wif_key)
    }

    /// Adds a client. Memory changes only once the file has been written.
    pub fn add(&mut self, new_client: &ClientConfig) -> Result<(), String> {
        let mut next = self.contents.clone();
        next.clients.push(new_client.clone());
        save_dynamic_config(&self.filename, &next)?;
        self.contents = next;
        Ok(())
    }

    /// Whether `client_id` was added at runtime, and so is this file's to
    /// remove (CS-653).
    pub fn contains(&self, client_id: &str) -> bool {
        self.contents
            .clients
            .iter()
            .any(|c| c.client_id == client_id)
    }

    /// Removes a client. Memory changes only once the file has been written.
    pub fn remove(&mut self, client_id: &str) -> Result<(), String> {
        let mut next = self.contents.clone();
        let before = next.clients.len();
        next.clients.retain(|c| c.client_id != client_id);
        if next.clients.len() == before {
            return Ok(());
        }
        save_dynamic_config(&self.filename, &next)?;
        self.contents = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DynamicConfigConfig};

    fn temp_dynamic_config() -> (String, DynamicConfig) {
        let path = std::env::temp_dir()
            .join(format!(
                "financing-service-dynamic-{}-{}.toml",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .into_owned();
        let config = Config {
            dynamic_config: DynamicConfigConfig {
                filename: path.clone(),
                wif_env_prefix: crate::config::DEFAULT_WIF_ENV_PREFIX.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        (path, DynamicConfig::new(&config).unwrap())
    }

    // The struct update covers fields other changes add to the section.
    #[allow(clippy::needless_update)]
    fn config_for(path: &str, allow_plaintext_wif: bool) -> Config {
        Config {
            dynamic_config: DynamicConfigConfig {
                filename: path.to_string(),
                allow_plaintext_wif,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn client(id: &str) -> ClientConfig {
        ClientConfig {
            client_id: id.to_string(),
            wif_key: format!("env:FS_{}_WIF", id.to_uppercase()),
            api_key: None,
        }
    }

    /// CS-526: a file that does not parse refuses startup, and is left as it
    /// was rather than replaced with an empty one.
    #[test]
    fn sr_sec_023_an_unparseable_file_refuses_startup_and_is_left_alone() {
        let (path, _) = temp_dynamic_config();
        let broken = "[[clients]]\nclient_id = \"half-written";
        std::fs::write(&path, broken).unwrap();
        let err = DynamicConfig::new(&config_for(&path, false)).err().unwrap();
        assert!(err.contains("does not parse"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), broken);
    }

    #[test]
    fn sr_sec_023_an_unreadable_file_refuses_startup() {
        // A directory where the file should be: it exists, but cannot be read.
        let (path, _) = temp_dynamic_config();
        std::fs::create_dir(&path).unwrap();
        let err = DynamicConfig::new(&config_for(&path, false)).err().unwrap();
        assert!(err.contains("cannot read"), "{err}");
    }

    /// No file yet is a deployment with no dynamic clients, and so is an
    /// empty one.
    #[test]
    fn sr_sec_023_a_missing_or_empty_file_is_no_clients() {
        let (path, dynamic_config) = temp_dynamic_config();
        assert!(dynamic_config.contents.clients.is_empty());
        std::fs::write(&path, "").unwrap();
        let loaded = DynamicConfig::new(&config_for(&path, false)).unwrap();
        assert!(loaded.contents.clients.is_empty());
    }

    #[test]
    fn sr_nfr_008_clients_survive_a_reload() {
        let (path, mut dynamic_config) = temp_dynamic_config();
        dynamic_config.add(&client("a")).unwrap();
        dynamic_config.add(&client("b")).unwrap();
        let loaded = DynamicConfig::new(&config_for(&path, false)).unwrap();
        let ids: Vec<_> = loaded
            .contents
            .clients
            .iter()
            .map(|c| c.client_id.as_str())
            .collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[cfg(unix)]
    #[test]
    fn sr_sec_022_the_file_is_written_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let (path, mut dynamic_config) = temp_dynamic_config();
        dynamic_config.add(&client("a")).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    /// A file written before CS-526 under a 0644 umask is tightened at load.
    #[cfg(unix)]
    #[test]
    fn sr_sec_022_a_readable_file_is_made_private_at_startup() {
        use std::os::unix::fs::PermissionsExt;
        let (path, _) = temp_dynamic_config();
        std::fs::write(&path, "clients = []\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        DynamicConfig::new(&config_for(&path, false)).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    /// A failed write changes neither the file nor what is in memory, so the
    /// two cannot drift apart.
    #[test]
    fn sr_nfr_008_a_failed_write_changes_nothing() {
        let (path, mut dynamic_config) = temp_dynamic_config();
        dynamic_config.add(&client("a")).unwrap();
        let mut unwritable = DynamicConfig {
            filename: std::env::temp_dir()
                .join("no-such-directory-cs-526")
                .join("dynamic.toml")
                .to_string_lossy()
                .into_owned(),
            ..dynamic_config
        };
        assert!(unwritable.add(&client("b")).is_err());
        assert!(unwritable.remove("a").is_err());
        let ids: Vec<_> = unwritable
            .contents
            .clients
            .iter()
            .map(|c| c.client_id.as_str())
            .collect();
        assert_eq!(ids, vec!["a"]);
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains("client_id = \"a\""));
    }

    #[test]
    fn sr_sec_021_only_env_references_are_accepted_by_default() {
        let (path, _) = temp_dynamic_config();
        let strict = DynamicConfig::new(&config_for(&path, false)).unwrap();
        assert!(strict.accepts_wif("env:FS_RUNTIME_WIF_X"));
        assert!(!strict.accepts_wif("cVLcPuZMfnNNcaU"));
        let relaxed = DynamicConfig::new(&config_for(&path, true)).unwrap();
        assert!(relaxed.accepts_wif("cVLcPuZMfnNNcaU"));
    }

    #[test]
    fn sr_clnt_004_and_sr_nfr_008_add_client_persists_to_dynamic_config_file() {
        let (path, mut dynamic_config) = temp_dynamic_config();
        let client = ClientConfig {
            client_id: "runtime-client".to_string(),
            wif_key: "env:FS_RUNTIME_WIF".to_string(),
            api_key: Some("env:FS_RUNTIME_API_KEY".to_string()),
        };
        dynamic_config.add(&client).unwrap();
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(saved.contains("runtime-client"));
        assert!(saved.contains("env:FS_RUNTIME_WIF"));
        assert!(saved.contains("env:FS_RUNTIME_API_KEY"));
    }

    #[test]
    fn sr_nfr_008_remove_client_updates_dynamic_config_file() {
        let (path, mut dynamic_config) = temp_dynamic_config();
        let client = ClientConfig {
            client_id: "to-remove".to_string(),
            wif_key: "env:FS_REMOVE_WIF".to_string(),
            api_key: None,
        };
        dynamic_config.add(&client).unwrap();
        assert!(dynamic_config.contains("to-remove"));
        dynamic_config.remove("to-remove").unwrap();
        assert!(!dynamic_config.contains("to-remove"));
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(!saved.contains("to-remove"));
    }
}
