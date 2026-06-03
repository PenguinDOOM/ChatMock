use std::{env, path::PathBuf};

use crate::{auth::read_auth_file, config::InfoArgs, errors::AppError};

pub fn read_auth_json_from_env() -> Result<String, AppError> {
    let chatgpt_local_home = env_path("CHATGPT_LOCAL_HOME");
    let codex_home = env_path("CODEX_HOME");
    let user_home = env_path("USERPROFILE").or_else(|| env_path("HOME"));
    let auth = read_auth_file(
        chatgpt_local_home.as_deref(),
        codex_home.as_deref(),
        user_home.as_deref(),
    );

    let value = match auth {
        Some(auth) => serde_json::to_value(auth)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
        None => serde_json::json!({}),
    };

    serde_json::to_string_pretty(&value)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error).into())
}

pub fn run(args: InfoArgs) -> Result<(), AppError> {
    if args.json {
        println!("{}", read_auth_json_from_env()?);
        return Ok(());
    }

    let auth_json = read_auth_json_from_env()?;
    let auth_value: serde_json::Value = serde_json::from_str(&auth_json)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

    if auth_value
        .as_object()
        .is_some_and(|object| object.is_empty())
    {
        println!("Account: not signed in");
        println!("Run `chatmock-rs login` to initialize auth storage.");
    } else {
        println!("Account: auth.json found");
        if let Some(account_id) = auth_value
            .get("tokens")
            .and_then(|tokens| tokens.get("account_id"))
            .and_then(|value| value.as_str())
        {
            println!("Account ID: {account_id}");
        }
    }

    Ok(())
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
}
