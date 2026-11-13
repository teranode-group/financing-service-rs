use std::{sync::Arc, time::Duration};
use tokio::time;

use actix_governor::Governor;
use actix_web::{middleware::from_fn, web, App, HttpServer};

mod address_watcher;
mod auth;
mod blockchain_factory;
mod broadcaster;
mod client;
mod config;
mod dynamic_config;
mod funding_limits;
mod idempotency;
mod interface_timeout;
mod mapi_lite_interface;
mod metrics;
mod outbound_rate_limit;
mod private_file;
mod rate_limit;
mod responses;
mod rest_api;
mod secrets;
mod service;
mod telemetry;
mod util;

#[cfg(test)]
mod test_support;

use crate::{
    config::{config_file, file_only, load_config},
    rest_api::{
        add_client, balance, delete_client, get_address, get_funds, health, index, ready, status,
        update_clients, AppState,
    },
    secrets::warn_plaintext_secrets,
    service::Service,
};

#[cfg(test)]
mod system_requirements;

#[cfg(test)]
mod dependency_graph;

/// Main - Read config and setup Web server.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config_path = config_file();
    let config = load_config(&config_path).map_err(|e| {
        eprintln!("Unable to read config: {e}");
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;

    let telemetry = telemetry::init(&config.telemetry).map_err(|e| {
        eprintln!("Unable to initialise telemetry: {e}");
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    // Only now is there a logger to warn through.
    if let Ok(file) = file_only(&config_path) {
        warn_plaintext_secrets(&file);
    }

    let service = Service::new(&config).await.map_err(|e| {
        log::error!("Unable to start service: {e}");
        std::io::Error::other(e)
    })?;
    // CS-514: on mainnet and testnet, every route that spends needs a key.
    service.check_auth_policy().await.map_err(|e| {
        log::error!("Unable to start service: {e}");
        std::io::Error::other(e)
    })?;
    if config.is_production_network() && config.web_interface.allow_unauthenticated {
        log::warn!(
            "web_interface.allow_unauthenticated is set on {}: authentication is left to whatever \
             is in front of this service",
            config.blockchain_interface.network
        );
    }
    // Which way funding transactions leave the service. Reads always go
    // through [blockchain_interface]; this names the write path.
    log::info!("{}", broadcaster::factory::describe_broadcaster(&config));
    if let Some(warning) = config.shared_woc_budget_warning() {
        log::warn!("{warning}");
    }
    if service.admin_auth_required() {
        log::info!("Admin API key authentication enabled for POST /client");
    } else {
        log::warn!(
            "Admin API key not configured; POST /client is unauthenticated. Set web_interface.admin_api_key or restrict via network isolation."
        );
    }
    let clients_without_api_key = service.clients_without_api_key().await;
    if service.client_count().await > 0 && clients_without_api_key.is_empty() {
        log::info!("Per-client API key authentication enabled for all clients");
    } else if !clients_without_api_key.is_empty() {
        log::warn!(
            "Clients without api_key configured: {}. Protect these via network isolation or set api_key per client.",
            clients_without_api_key.join(", ")
        );
    }
    let governor_config = rate_limit::build_governor_config(&config.web_interface.rate_limit);
    if config.web_interface.rate_limit.enabled {
        log::info!(
            "Rate limiting enabled: {} requests/s per IP (burst {})",
            config.web_interface.rate_limit.requests_per_second,
            config.web_interface.rate_limit.effective_burst_size()
        );
        rate_limit::spawn_retain_task(&governor_config);
    }
    // After telemetry::init, so the instruments come from the meter provider
    // it installed (or the no-op one when metrics export is off).
    let app_state = web::Data::new(AppState::new(Arc::new(service)));
    // CS-608: the gauges, and the task that samples the service for them,
    // only when metrics are exported.
    let _gauges = telemetry
        .as_ref()
        .is_some_and(|guard| guard.metrics_enabled())
        .then(|| {
            let samples = metrics::SharedSamples::default();
            let gauges = metrics::register_gauges(
                &opentelemetry::global::meter(metrics::METER_NAME),
                samples.clone(),
            );
            metrics::spawn_sampler(Arc::clone(&app_state.service), samples);
            gauges
        });
    let app_state2 = app_state.clone();
    let addr = config
        .listen_socket_addr()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // The startup chain reads, then the periodic refresh, behind the server
    // rather than in front of it, so /health answers however long the chain
    // source takes (CS-722).
    let utxo_refresh_period = Duration::from_secs(config.service.utxo_refresh_period);
    tokio::spawn(async move {
        Service::start_chain(&app_state2.service).await;
        let mut interval = time::interval_at(
            time::Instant::now() + utxo_refresh_period,
            utxo_refresh_period,
        );
        loop {
            interval.tick().await;
            // Refresh the utxo for clients
            update_clients(app_state2.clone()).await;
        }
    });

    let governor_config = governor_config.clone();
    HttpServer::new(move || {
        App::new()
            .wrap(tracing_actix_web::TracingLogger::default())
            .wrap(Governor::new(&governor_config))
            // Outermost, so requests the rate limiter refuses are counted too.
            .wrap(from_fn(metrics::record_http))
            .app_data(app_state.clone())
            .service(index)
            .service(health)
            .service(ready)
            .service(status)
            .service(balance)
            .service(get_funds)
            .service(add_client)
            .service(delete_client)
            .service(get_address)
    })
    .bind(addr)
    .map_err(|e| {
        log::error!(r#"Unable to bind to address/port "{addr:?}": {e}"#);
        e
    })?
    .run()
    .await
}
