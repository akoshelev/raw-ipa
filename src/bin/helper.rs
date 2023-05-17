use clap::{self, Parser, Subcommand};
use hyper::http::uri::Scheme;
use ipa::{
    cli::{keygen, test_setup, KeygenArgs, TestSetupArgs, Verbosity},
    config::{MatchKeyEncryptionConfig, NetworkConfig, ServerConfig, TlsConfig},
    helpers::HelperIdentity,
    net::{HttpTransport, MpcHelperClient},
    AppSetup,
};
use std::{error::Error, fs, path::PathBuf};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Parser)]
#[clap(
    name = "helper",
    about = "Interoperable Private Attribution (IPA) MPC helper"
)]
#[command(subcommand_negates_reqs = true)]
struct Args {
    /// Configure logging.
    #[clap(flatten)]
    logging: Verbosity,

    #[clap(flatten, next_help_heading = "Server Options")]
    server: ServerArgs,

    #[command(subcommand)]
    command: Option<HelperCommand>,
}

#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// Identity of this helper in the MPC protocol (1, 2, or 3)
    // This is required when running the server, but the `subcommand_negates_reqs`
    // attribute on `Args` makes it optional when running a utility command.
    #[arg(short, long, required = true)]
    identity: Option<usize>,

    /// Port to listen on
    #[arg(short, long, default_value = "3000")]
    port: Option<u16>,

    /// Use insecure HTTP
    #[arg(short = 'k', long)]
    disable_https: bool,

    /// File containing helper network configuration
    #[arg(long, required = true)]
    network: Option<PathBuf>,

    /// TLS certificate for helper-to-helper communication
    #[arg(
        long,
        visible_alias("cert"),
        visible_alias("tls-certificate"),
        requires = "tls_key"
    )]
    tls_cert: Option<PathBuf>,

    /// TLS key for helper-to-helper communication
    #[arg(long, visible_alias("key"), requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Public key for encrypting match keys
    #[arg(long, visible_alias("mk_enc"), requires = "matchkey_encryption_file")]
    matchkey_encryption_file: Option<PathBuf>,

    /// Private key for decrypting match keys
    #[arg(long, visible_alias("mk_dec"), requires = "matchkey_decryption_file")]
    matchkey_decryption_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum HelperCommand {
    Keygen(KeygenArgs),
    TestSetup(TestSetupArgs),
}

async fn server(args: ServerArgs) -> Result<(), Box<dyn Error>> {
    let my_identity = HelperIdentity::try_from(args.identity.expect("enforced by clap")).unwrap();

    let tls = match (args.tls_cert, args.tls_key) {
        (Some(cert), Some(key)) => Some(TlsConfig::File {
            certificate_file: cert,
            private_key_file: key,
        }),
        (None, None) => None,
        _ => panic!("should have been rejected by clap"),
    };

    let matchkey_encryption_info =
        match (args.matchkey_encryption_file, args.matchkey_decryption_file) {
            (Some(matchkey_encryption_file), Some(matchkey_decryption_file)) => {
                Some(MatchKeyEncryptionConfig::File {
                    public_key_file: matchkey_encryption_file,
                    private_key_file: matchkey_decryption_file,
                })
            }
            (None, None) => None,
            _ => panic!("should have been rejected by clap"),
        };

    let server_config = ServerConfig {
        port: args.port,
        disable_https: args.disable_https,
        tls,
        matchkey_encryption_info,
    };

    let (setup, callbacks) = AppSetup::new();

    let scheme = if args.disable_https {
        Scheme::HTTP
    } else {
        Scheme::HTTPS
    };
    let network_config_path = args.network.as_deref().unwrap();
    let network_config = NetworkConfig::from_toml_str(&fs::read_to_string(network_config_path)?)?
        .override_scheme(&scheme);
    let clients = MpcHelperClient::from_conf(&network_config);

    let (transport, server) = HttpTransport::new(
        my_identity,
        server_config,
        network_config,
        clients,
        callbacks,
    );

    let _app = setup.connect(transport.clone());

    let (_addr, server_handle) = server
        .start(
            // TODO, trace based on the content of the query.
            None as Option<()>,
        )
        .await;

    server_handle.await?;

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let _handle = args.logging.setup_logging();

    match args.command {
        None => server(args.server).await,
        Some(HelperCommand::Keygen(args)) => keygen(&args),
        Some(HelperCommand::TestSetup(args)) => test_setup(args),
    }
}
