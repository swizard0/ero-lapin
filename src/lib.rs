use futures::{
    Future,
    future::result,
};

use lapin_futures::{
    Client,
    Channel,
};

use log::{
    debug,
    error,
};

use ero::{
    ErrorSeverity,
    lode::{self, LodeResource},
    supervisor::Supervisor,
};

#[derive(Debug)]
pub struct AmqpParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub prefetch_count: u16,
    pub frame_max: Option<u32>,
    pub channel_max: Option<u16>,
    pub heartbeat: Option<u16>,
}

impl Default for AmqpParams {
    fn default() -> AmqpParams {
        AmqpParams {
            host: "localhost".to_string(),
            port: lapin::uri::AMQPScheme::AMQP.default_port(),
            user: "guest".to_string(),
            pass: "guest".to_string(),
            prefetch_count: 32,
            frame_max: Some(262144),
            channel_max: None,
            heartbeat: None,
        }
    }
}

pub struct Params<N> {
    pub amqp_params: AmqpParams,
    pub lode_params: ero::Params<N>,
}

pub fn spawn_link<N>(
    supervisor: &Supervisor,
    params: Params<N>,
)
    -> LodeResource<Channel>
where N: AsRef<str> + Send + 'static,
{
    let Params { amqp_params, lode_params, } = params;

    let init_state = InitState {
        amqp_params,
    };

    lode::shared::spawn_link(
        supervisor,
        lode_params,
        init_state,
        init,
        aquire,
        release,
        close,
    )
}

struct InitState {
    amqp_params: AmqpParams,
}

struct AmqpConnected {
    init_state: InitState,
    client: Client,
}

fn init(
    init_state: InitState,
)
    -> Box<dyn Future<Item = AmqpConnected, Error = ErrorSeverity<InitState, ()>> + Send + 'static>
{
    let InitState { amqp_params, } = init_state;

    let amqp_uri = lapin::uri::AMQPUri {
        scheme: lapin::uri::AMQPScheme::AMQP,
        authority: lapin::uri::AMQPAuthority {
            userinfo: lapin::uri::AMQPUserInfo {
                username: amqp_params.user.clone(),
                password: amqp_params.pass.clone(),
            },
            host: amqp_params.host.clone(),
            port: amqp_params.port,
        },
        vhost: "/".to_string(),
        query: lapin::uri::AMQPQueryString {
            frame_max: amqp_params.frame_max,
            channel_max: amqp_params.channel_max,
            heartbeat: amqp_params.heartbeat,
        },
    };
    debug!("configured amqp as: {:?}", amqp_uri);

    let future = Client::connect_uri(amqp_uri, Default::default())
        .then(move |connect_result| {
            match connect_result {
                Ok(client) => {
                    Ok(AmqpConnected {
                        init_state: InitState { amqp_params, },
                        client,
                    })
                },
                Err(error) => {
                    error!("amqp connection error: {:?}, retrying", error);
                    Err(ErrorSeverity::Recoverable { state: InitState { amqp_params, }, })
                },
            }
        });
    Box::new(future)
}

fn aquire(
    connected: AmqpConnected,
)
    -> Box<dyn Future<Item = (Channel, AmqpConnected), Error = ErrorSeverity<InitState, ()>> + Send + 'static>
{
    let AmqpConnected { init_state, client, } = connected;
    let future = client.create_channel()
        .then(move |create_result| {
            match create_result {
                Ok(channel) => {
                    debug!("channel id = {} created", channel.id());
                    Ok((channel, AmqpConnected { init_state, client, }))
                },
                Err(error) => {
                    error!("create_channel error: {:?}, restarting connection", error);
                    Err(ErrorSeverity::Recoverable { state: init_state, })
                },
            }
        })
        .and_then(|(channel, connected)| {
            channel
                .basic_qos(
                    connected.init_state.amqp_params.prefetch_count,
                    Default::default(),
                )
                .then(|qos_result| {
                    match qos_result {
                        Ok(()) => {
                            debug!("qos set for channel id = {}", channel.id());
                            Ok((channel, connected))
                        },
                        Err(error) => {
                            error!("basic_qos error: {:?}, restarting connection", error);
                            Err(ErrorSeverity::Recoverable { state: connected.init_state, })
                        },
                    }
                })
        });
    Box::new(future)
}

fn release(
    connected: AmqpConnected,
    _maybe_session: Option<Channel>,
)
    -> impl Future<Item = AmqpConnected, Error = ErrorSeverity<InitState, ()>>
{
    result(Ok(connected))
}

fn close(
    connected: AmqpConnected,
)
    -> impl Future<Item = InitState, Error = ()>
{
    result(Ok(connected.init_state))
}
