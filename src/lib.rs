use std::sync::{
    Arc,
    atomic::{
        Ordering,
        AtomicBool,
    },
};

use futures::{
    Future,
    future::{
        lazy,
        result,
    },
};

use tokio::net::TcpStream;

use lapin_futures::{
    client::{
        self,
        ConnectionOptions,
    },
    channel::{
        Channel,
        BasicQosOptions,
    },
};

use log::{
    debug,
    error,
};

use ero::{
    ErrorSeverity,
    lode::{
        self,
        Lode,
        LodeResource,
    },
};

#[derive(Debug)]
pub struct AmqpParams {
    pub user: String,
    pub pass: String,
    pub prefetch_count: u16,
}

impl Default for AmqpParams {
    fn default() -> AmqpParams {
        AmqpParams {
            user: "guest".to_string(),
            pass: "guest".to_string(),
            prefetch_count: 32,
        }
    }
}

pub struct Params<N> {
    pub amqp_params: AmqpParams,
    pub lode_params: lode::Params<N>,
}

pub fn spawn<N>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    tcp_lode: LodeResource<TcpStream>,
)
    -> Lode<Channel<TcpStream>>
where N: AsRef<str> + Send + 'static,
{
    let Params { amqp_params, lode_params, } = params;

    let init_state = InitState {
        amqp_params,
        tcp_lode,
        executor: executor.clone(),
    };

    lode::shared::spawn(
        executor,
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
    tcp_lode: LodeResource<TcpStream>,
    executor: tokio::runtime::TaskExecutor,
}

struct AmqpConnected {
    init_state: InitState,
    client: client::Client<TcpStream>,
    heartbeat_gone: Arc<AtomicBool>,
}

fn init(
    init_state: InitState,
)
    -> impl Future<Item = AmqpConnected, Error = ErrorSeverity<InitState, ()>>
{
    let InitState { amqp_params, tcp_lode, executor, } = init_state;

    tcp_lode
        .steal_resource()
        .map_err(|()| {
            debug!("tcp_stream lode is gone, terminating");
            ErrorSeverity::Fatal(())
        })
        .and_then(move |(tcp_stream, tcp_lode)| {
            let options = ConnectionOptions {
                username: amqp_params.user.clone(),
                password: amqp_params.pass.clone(),
                frame_max: 262144,
                ..Default::default()
            };
            debug!("tcp connection obtained, authorizing as {:?}", amqp_params);
            client::Client::connect(tcp_stream, options)
                .then(move |connect_result| {
                    match connect_result {
                        Ok((client, heartbeat_future)) => {
                            let heartbeat_gone_lode = Arc::new(AtomicBool::new(false));
                            let heartbeat_gone_task = heartbeat_gone_lode.clone();
                            executor.spawn(
                                heartbeat_future
                                    .then(move |heartbeat_result| {
                                        heartbeat_gone_task.store(true, Ordering::SeqCst);
                                        match heartbeat_result {
                                            Ok(()) =>
                                                Ok(()),
                                            Err(error) => {
                                                error!("heartbeat task error: {:?}", error);
                                                Err(())
                                            },
                                        }
                                    })
                            );
                            Ok(AmqpConnected {
                                init_state: InitState { amqp_params, tcp_lode, executor, },
                                heartbeat_gone: heartbeat_gone_lode,
                                client,
                            })
                        },
                        Err(error) => {
                            error!("amqp connection error: {:?}, retrying", error);
                            Err(ErrorSeverity::Recoverable { state: InitState { amqp_params, tcp_lode, executor, }, })
                        },
                    }
                })
        })
}

fn aquire(
    connected: AmqpConnected,
)
    -> impl Future<Item = (Channel<TcpStream>, AmqpConnected), Error = ErrorSeverity<InitState, ()>>
{
    let AmqpConnected { init_state, client, heartbeat_gone, } = connected;
    lazy(move || if heartbeat_gone.load(Ordering::SeqCst) {
        error!("heartbeat task is gone, restarting connection");
        Err(ErrorSeverity::Recoverable { state: init_state, })
    } else {
        Ok((init_state, heartbeat_gone))
    })
        .and_then(move |(init_state, heartbeat_gone)| {
            client.create_channel()
                .then(move |create_result| {
                    match create_result {
                        Ok(channel) => {
                            debug!("channel id = {} created", channel.id);
                            Ok((channel, AmqpConnected { init_state, client, heartbeat_gone, }))
                        },
                        Err(error) => {
                            error!("create_channel error: {:?}, restarting connection", error);
                            Err(ErrorSeverity::Recoverable { state: init_state, })
                        },
                    }
                })
        })
        .and_then(|(channel, connected)| {
            channel
                .basic_qos(BasicQosOptions {
                    prefetch_count: connected.init_state.amqp_params.prefetch_count,
                    ..Default::default()
                })
                .then(|qos_result| {
                    match qos_result {
                        Ok(()) => {
                            debug!("qos set for channel id = {}", channel.id);
                            Ok((channel, connected))
                        },
                        Err(error) => {
                            error!("basic_qos error: {:?}, restarting connection", error);
                            Err(ErrorSeverity::Recoverable { state: connected.init_state, })
                        },
                    }
                })
        })
}

fn release(
    connected: AmqpConnected,
    _maybe_session: Option<Channel<TcpStream>>,
)
    -> impl Future<Item = AmqpConnected, Error = ErrorSeverity<InitState, ()>>
{
    lazy(move || if connected.heartbeat_gone.load(Ordering::SeqCst) {
        error!("heartbeat task is gone, restarting connection");
        Err(ErrorSeverity::Recoverable { state: connected.init_state, })
    } else {
        Ok(connected)
    })
}

fn close(
    connected: AmqpConnected,
)
    -> impl Future<Item = InitState, Error = ()>
{
    result(Ok(connected.init_state))
}
