
use futures::{
    Future,
    future::{
        lazy,
        result,
        Either,
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
