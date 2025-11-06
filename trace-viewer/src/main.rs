#![allow(unused_crate_dependencies)]

use cfg_if::cfg_if;
use leptos::prelude::*;

cfg_if! {
    if #[cfg(feature = "ssr")] {
        use clap::Parser;
        use std::net::SocketAddr;
        use digital_muon_common::CommonKafkaOpts;
        use trace_viewer::{structs::{ClientSideData, DefaultData, ServerSideData, Topics}, sessions::{SessionEngineSettings}, shell};
        use tracing::info;
        use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt};
        use url::Url;

        #[derive(Parser)]
        #[clap(author, version, about)]
        struct Cli {
            #[clap(flatten)]
            common_kafka_options: CommonKafkaOpts,

            /// Kafka consumer group.
            #[clap(long)]
            consumer_group: String,

            #[clap(flatten)]
            topics: Topics,

            /// If set, then OpenTelemetry data is sent to the URL specified, otherwise the standard tracing subscriber is used.
            #[clap(long)]
            otel_endpoint: Option<String>,

            /// All OpenTelemetry spans are emitted with this as the "service.namespace" property. Can be used to track different instances of the pipeline running in parallel.
            #[clap(long, default_value = "")]
            otel_namespace: String,

            /// Endpoint on which OpenMetrics flavour metrics are available.
            #[clap(long, default_value = "127.0.0.1:9090")]
            observability_address: SocketAddr,

            #[clap(flatten)]
            default: DefaultData,

            /// Name of the broker.
            #[clap(long)]
            broker_name: String,

            /// Origin of the host from which the app is served (without the trailing slash).
            #[clap(long, default_value = "http://localhost:3000")]
            public_url: Url,

            /// Optional link to the redpanda console. If present, displayed in the topbar.
            #[clap(long)]
            link_to_redpanda_console: Option<String>,

            /// The frequency with which the server purges expired sessions.
            #[clap(long, default_value = "600")]
            purge_session_interval_sec: u64,

            /// The frequency with which a client sends a refresh call to its corresponding session.
            #[clap(long, default_value = "300")]
            refresh_session_interval_sec: u64,

            /// Specifies the time-to-live of a user session. Any session whose time since last refresh is older than this is removed during a session purge cycle.
            #[clap(long, default_value = "600")]
            session_ttl_sec: i64,

            /// Name to apply to this particular instance.
            #[clap(long)]
            name: Option<String>,
        }

        #[actix_web::main]
        async fn main() -> miette::Result<()> {
            use actix_files::Files;
            use leptos_actix::{generate_route_list, LeptosRoutes};
            use miette::IntoDiagnostic;
            use trace_viewer::{App, sessions::SessionEngine};

            // set up logging
            console_error_panic_hook::set_once();

            let stdout_tracer = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stdout)
                .with_ansi(false)
                .with_target(false);

            // This filter is applied to the stdout tracer
            let log_filter = EnvFilter::from_default_env();

            let subscriber =
                tracing_subscriber::Registry::default().with(stdout_tracer.with_filter(log_filter));

            //  This is only called once, so will never panic
            tracing::subscriber::set_global_default(subscriber)
                .expect("tracing::subscriber::set_global_default should only be called once");

            let args = Cli::parse();

            let session_engine = SessionEngine::with_arc_mutex(SessionEngineSettings {
                broker: args.common_kafka_options.broker.clone(),
                topics: args.topics.clone(),
                username: args.common_kafka_options.username.clone(),
                password: args.common_kafka_options.password.clone(),
                consumer_group: args.consumer_group.clone(),
                session_ttl_sec: args.session_ttl_sec,
            });

            let server_side_data = ServerSideData {
                session_engine: session_engine.clone(),
            };

            let client_side_data = ClientSideData {
                broker_name: args.broker_name,
                link_to_redpanda_console: args.link_to_redpanda_console,
                default_data : args.default,
                refresh_session_interval_sec: args.refresh_session_interval_sec,
                public_url: args.public_url,
            };

            // Spawn the "purge expired sessions" task.
            let _purge_sessions = SessionEngine::spawn_purge_task(session_engine.clone(), args.purge_session_interval_sec);

            let conf = get_configuration(None).unwrap();
            let addr = conf.leptos_options.site_addr;

            actix_web::HttpServer::new(move || {
                // Generate the list of routes in your Leptos App
                let routes = generate_route_list({
                    let client_side_data = client_side_data.clone();
                    move || {
                        provide_context(client_side_data.clone());
                        view!{ <App /> }
                    }
                });
                let leptos_options = &conf.leptos_options;
                let site_root = leptos_options.site_root.clone().to_string();

                info!("listening on http://{}", &addr);
                actix_web::App::new()
                    .service(Files::new("/pkg", format!("{site_root}/pkg")))
                    .leptos_routes_with_context(routes, {
                        let server_side_data = server_side_data.clone();
                        let client_side_data = client_side_data.clone();
                        move ||{
                            provide_context(server_side_data.clone());
                            provide_context(client_side_data.clone());
                        }
                    }, {
                        let leptos_options = leptos_options.clone();
                        move ||shell(leptos_options.clone())
                    })
                    .app_data(actix_web::web::Data::new(leptos_options.to_owned()))
            })
            .bind(&addr)
            .into_diagnostic()?
            .run()
            .await
            .into_diagnostic()
        }
    }
}

#[cfg(not(feature = "ssr"))]
fn main() {
    use console_error_panic_hook as _;
    use trace_viewer as _;
    mount_to_body(|| {
        view! {
            "Please run using SSR"
        }
    });
}
