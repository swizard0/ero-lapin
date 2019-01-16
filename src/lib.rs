use futures::{
    Future,
    future::{
        lazy,
        result,
        Either,
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
    types::{
        FieldTable,
        AMQPValue,
    },
};

use log::{
    debug,
    error,
};

use ero::{
    ErrorSeverity,
    lode::{self, Lode},
};

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
    tcp_lode: Lode<TcpStream>,
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
    tcp_lode: Lode<TcpStream>,
    executor: tokio::runtime::TaskExecutor,
}

struct AmqpConnected {
    init_state: InitState,
}

fn init(
    init_state: InitState,
)
    -> impl Future<Item = AmqpConnected, Error = ErrorSeverity<InitState, ()>>
{

    result(Err(ErrorSeverity::Fatal(())))
}

fn aquire(
    connected: AmqpConnected,
)
    -> impl Future<Item = (Channel<TcpStream>, AmqpConnected), Error = ErrorSeverity<InitState, ()>>
{

    result(Err(ErrorSeverity::Fatal(())))
}

fn release(
    connected: AmqpConnected,
    _maybe_session: Option<Channel<TcpStream>>,
)
    -> impl Future<Item = AmqpConnected, Error = ErrorSeverity<InitState, ()>>
{

    result(Err(ErrorSeverity::Fatal(())))
}

fn close(
    connected: AmqpConnected,
)
    -> impl Future<Item = InitState, Error = ()>
{
    result(Ok(connected.init_state))
}
