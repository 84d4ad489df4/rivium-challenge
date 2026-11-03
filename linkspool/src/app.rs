//! Rivium application composition for both desktop and Android hosts.

use linkspool_core::{Config, shared};
use rivium::{Result, Service};
use rivium_host::{App as HostApp, Context};

use crate::{api, delivery, inbox, maintenance, status};

/// The LinkSpool application.
pub struct App;

impl HostApp for App {
    const NAME: &'static str = "linkspool";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = Config;

    fn validate(config: &Self::Config) -> Result<()> {
        config
            .validate()
            .map_err(|error| rivium::Error::invalid(error.to_string()))
    }

    fn services(
        config: &Self::Config,
        context: &Context<Self::Config>,
    ) -> Result<Vec<Box<dyn Service>>> {
        let mut config = config.clone();
        config.resolve_paths(context.paths().root());
        let core = shared(config.clone());

        let mut services: Vec<Box<dyn Service>> = Vec::new();
        if config.api.enabled {
            services.push(api::service(core.clone(), config.api.clone()));
        }
        services.push(delivery::service(core.clone()));
        services.push(maintenance::service(
            core.clone(),
            config.maintenance.clone(),
        ));
        if config.inbox.enabled {
            services.push(inbox::service(core.clone(), config.inbox.clone()));
        }
        if config.status.enabled {
            services.push(status::service(core.clone(), config.status.clone()));
        }

        Ok(services)
    }
}
