use std::path::{Path, PathBuf};

use serde_json::Value;

const WORKING_DIRECTORY_PREFIXES: [&str; 3] = [
    "Primary working directory:",
    "Working directory:",
    "Current working directory:",
];

pub fn name_from_system(system: Option<&Value>) -> Option<String> {
    system.and_then(name_from_value)
}

pub fn name_from_request<'a>(
    system: Option<&Value>,
    message_contents: impl IntoIterator<Item = &'a Value>,
) -> Option<String> {
    name_from_system(system).or_else(|| message_contents.into_iter().find_map(name_from_value))
}

fn name_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => name_from_text(text),
        Value::Array(values) => values.iter().find_map(name_from_value),
        Value::Object(object) => object
            .get("text")
            .and_then(Value::as_str)
            .and_then(name_from_text)
            .or_else(|| object.get("content").and_then(name_from_value)),
        _ => None,
    }
}

fn name_from_text(text: &str) -> Option<String> {
    text.lines()
        .find_map(working_directory_from_line)
        .and_then(name_from_working_directory)
}

fn working_directory_from_line(line: &str) -> Option<&str> {
    let line = line.trim().strip_prefix("- ").unwrap_or(line.trim());
    WORKING_DIRECTORY_PREFIXES.iter().find_map(|prefix| {
        line.strip_prefix(prefix)
            .map(str::trim)
            .filter(|path| !path.is_empty())
    })
}

fn name_from_working_directory(path: &str) -> Option<String> {
    let working_directory = Path::new(path);
    let repository_root = working_directory
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists());

    repository_root
        .and_then(repository_name)
        .or_else(|| path_name(working_directory))
}

fn repository_name(root: &Path) -> Option<String> {
    let git_marker = root.join(".git");
    if git_marker.is_dir() {
        return path_name(root);
    }

    let contents = std::fs::read_to_string(git_marker).ok()?;
    let git_dir = contents.trim().strip_prefix("gitdir:")?.trim();
    let git_dir = if Path::new(git_dir).is_absolute() {
        PathBuf::from(git_dir)
    } else {
        root.join(git_dir)
    };
    git_dir
        .ancestors()
        .find(|ancestor| ancestor.file_name().is_some_and(|name| name == ".git"))
        .and_then(Path::parent)
        .and_then(path_name)
        .or_else(|| path_name(root))
}

fn path_name(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn reads_primary_working_directory_from_claude_code_system_blocks() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join(".git")).unwrap();
        let system = json!([
            {
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.177.45c"
            },
            {
                "type": "text",
                "text": "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
                "cache_control": {"type": "ephemeral"}
            },
            {
                "type": "text",
                "text": format!(
                    "\nYou are an interactive agent.\n\n# Environment\nYou have been invoked in the following environment: \n - Primary working directory: {}\n - Is a git repository: true",
                    root.path().display()
                ),
                "cache_control": {"type": "ephemeral"}
            }
        ]);

        assert_eq!(
            name_from_system(Some(&system)).as_deref(),
            root.path().file_name().and_then(|name| name.to_str())
        );
    }

    #[test]
    fn reads_legacy_working_directory_from_string_system_prompt() {
        let system = json!("<env>\nWorking directory: /home/user/example\n</env>");

        assert_eq!(name_from_system(Some(&system)).as_deref(), Some("example"));
    }

    #[test]
    fn reads_working_directory_from_message_system_reminder() {
        let content = json!([
            {"type": "text", "text": "hello"},
            {"type": "text", "text": "<system-reminder>\n# Environment\n - Primary working directory: /home/user/example\n</system-reminder>"}
        ]);

        assert_eq!(
            name_from_request(None, [&content]).as_deref(),
            Some("example")
        );
    }

    #[test]
    fn resolves_linked_worktree_to_main_repository_name() {
        let temp = tempdir().unwrap();
        let main = temp.path().join("project");
        let worktree = temp.path().join("worktrees").join("feature");
        let git_dir = main.join(".git").join("worktrees").join("feature");
        fs::create_dir_all(&git_dir).unwrap();
        fs::create_dir_all(&worktree).unwrap();
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", git_dir.display()),
        )
        .unwrap();

        assert_eq!(
            name_from_working_directory(worktree.to_str().unwrap()).as_deref(),
            Some("project")
        );
    }

    #[test]
    fn returns_none_without_working_directory_metadata() {
        assert_eq!(name_from_system(Some(&json!("instructions"))), None);
        assert_eq!(name_from_system(None), None);
    }
}
