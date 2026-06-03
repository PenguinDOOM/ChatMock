use crate::{
    config::{Cli, Command, RuntimeConfig},
    errors::AppError,
    info, login, server,
};

pub async fn run(cli: Cli) -> Result<(), AppError> {
    match cli.command {
        Command::Login(args) => login::run(args),
        Command::Serve(args) => {
            let config = RuntimeConfig::from_sources(args)?;
            server::run(config).await
        }
        Command::Info(args) => info::run(args),
    }
}
