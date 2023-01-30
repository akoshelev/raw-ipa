use crate::{
    helpers::{query::QueryCommand, transport::TransportCommand, CommandEnvelope, CommandOrigin},
    net::{http_serde, server::Error},
};
use axum::{routing::post, Extension, Router};
use tokio::sync::{mpsc, oneshot};

async fn handler(
    req: http_serde::query::input::Request,
    transport_sender: Extension<mpsc::Sender<CommandEnvelope>>,
) -> Result<(), Error> {
    let permit = transport_sender.reserve().await?;

    let (tx, rx) = oneshot::channel();
    let command = CommandEnvelope {
        origin: CommandOrigin::Other,
        payload: TransportCommand::Query(QueryCommand::Input(req.query_input, tx)),
    };
    permit.send(command);
    rx.await?;
    Ok(())
}

pub fn router(transport_sender: mpsc::Sender<CommandEnvelope>) -> Router {
    Router::new()
        .route(http_serde::query::input::AXUM_PATH, post(handler))
        .layer(Extension(transport_sender))
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::{
        helpers::{query::QueryInput, transport::ByteArrStream},
        net::server::handlers::query::test_helpers::{resp_eq, IntoReq},
        protocol::QueryId,
    };
    use axum::http::Request;
    use futures::pin_mut;
    use futures_util::{future::poll_immediate, TryStreamExt};
    use hyper::{Body, StatusCode};

    async fn collect_bytes(stream: ByteArrStream) -> Vec<u8> {
        stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|bytes| bytes.to_vec())
            .collect()
    }

    #[tokio::test]
    async fn input_test() {
        let expected_query_id = QueryId;
        let expected_input = [4u8; 4].as_slice();
        let req = http_serde::query::input::Request::new(QueryInput {
            query_id: expected_query_id,
            input_stream: expected_input.into(),
        });
        let (tx, mut rx) = mpsc::channel(1);
        let handle = handler(req, Extension(tx));
        pin_mut!(handle);
        // should be pending while waiting for `rx`
        assert!(matches!(poll_immediate(&mut handle).await, None));
        let res = poll_immediate(rx.recv()).await.unwrap().unwrap();
        assert_eq!(res.origin, CommandOrigin::Other);
        match res.payload {
            TransportCommand::Query(QueryCommand::Input(
                QueryInput {
                    query_id,
                    input_stream,
                },
                responder,
            )) => {
                assert_eq!(query_id, expected_query_id);
                let input = collect_bytes(input_stream).await;
                assert_eq!(input.as_slice(), expected_input);
                responder.send(()).unwrap();
            }
            other => panic!("expected input command, but got {other:?}"),
        }

        poll_immediate(handle).await.unwrap().unwrap();
    }

    struct OverrideReq {
        query_id: String,
        input_stream: Vec<u8>,
    }

    impl IntoReq for OverrideReq {
        fn into_req(self, port: u16) -> Request<Body> {
            let uri = format!(
                "http://127.0.0.1:{}{}/input?query_id={}",
                port,
                http_serde::query::BASE_AXUM_PATH,
                self.query_id
            );
            hyper::Request::post(uri)
                .body(hyper::Body::from(self.input_stream))
                .unwrap()
        }
    }

    impl Default for OverrideReq {
        fn default() -> Self {
            Self {
                query_id: QueryId.as_ref().to_string(),
                input_stream: vec![4; 4],
            }
        }
    }

    #[tokio::test]
    async fn malformed_query_id() {
        let req = OverrideReq {
            query_id: "not_a_query_id".into(),
            ..Default::default()
        };
        resp_eq(req, StatusCode::UNPROCESSABLE_ENTITY).await;
    }
}
