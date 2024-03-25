mod echo;
mod query;

use axum::Router;

use crate::{
    net::{http_serde, HttpTransport},
    sync::Arc,
};
use crate::helpers::{HelperIdentity, RequestHandler};

pub fn router<H: RequestHandler<Identity = HelperIdentity>>(transport: Arc<HttpTransport<H>>) -> Router {
    echo::router().nest(
        http_serde::query::BASE_AXUM_PATH,
        Router::new()
            .merge(query::query_router(Arc::clone(&transport)))
            .merge(query::h2h_router(transport)),
    )
}
