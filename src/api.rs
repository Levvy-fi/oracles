use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{net::TcpListener, sync::watch, task::JoinSet};
use tracing::{info, warn};

use crate::{
    config::OracleConfig,
    network::NodeId,
    price_aggregator::TokenPrice,
    signature_aggregator::{GenericPayloadEntry, Payload, SyntheticPayloadEntry},
};

/// Test price override for liquidation testing
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestPriceOverride {
    pub token: String,
    pub unit: String,
    pub value: Decimal,
}

pub type TestPriceOverrides = Arc<DashMap<String, TestPriceOverride>>;

#[derive(Clone, Serialize)]
struct OracleIdentifiers {
    id: NodeId,
    name: String,
}

#[derive(Clone)]
pub struct APIState {
    payload_source: Arc<watch::Receiver<Payload>>,
    prices_source: Arc<watch::Receiver<Vec<TokenPrice>>>,
    oracle: OracleIdentifiers,
    test_price_overrides: TestPriceOverrides,
}

pub struct APIServer {
    state: APIState,
}
impl APIServer {
    pub fn new(
        config: &OracleConfig,
        payload_source: watch::Receiver<Payload>,
        audit_source: watch::Receiver<Vec<TokenPrice>>,
        test_price_overrides: TestPriceOverrides,
    ) -> Self {
        Self {
            state: APIState {
                payload_source: Arc::new(payload_source),
                oracle: OracleIdentifiers {
                    id: config.id.clone(),
                    name: config.label.clone(),
                },
                prices_source: Arc::new(audit_source),
                test_price_overrides,
            },
        }
    }

    pub async fn run(self, port: u16) {
        let mut set = JoinSet::new();

        let app = Router::new()
            .route("/payload", get(report_all_payloads))
            .route("/payload/{feed}", get(report_payload))
            .route("/prices", get(report_all_prices))
            // Test endpoints for liquidation testing (temporary)
            .route("/test/set-price", post(set_test_price))
            .route("/test/prices", get(get_test_prices))
            .route("/test/clear-prices", delete(clear_test_prices))
            .with_state(self.state);
        set.spawn(async move {
            info!("API server starting on port {}", port);
            let listener = match TcpListener::bind(("::", port)).await {
                Ok(l) => l,
                Err(error) => {
                    warn!("Could not start API server: {}", error);
                    return;
                }
            };
            if let Err(error) = axum::serve(listener, app).await {
                warn!("API server stopped: {}", error);
            }
        });

        while let Some(res) = set.join_next().await {
            if let Err(error) = res {
                warn!("{:?}", error);
            }
        }
    }
}

async fn report_all_payloads(State(state): State<APIState>) -> impl IntoResponse {
    let payload = state.payload_source.borrow().clone();
    (StatusCode::OK, Json(payload))
}

#[derive(Serialize)]
struct AllPricesResponse {
    oracle: OracleIdentifiers,
    prices: Vec<TokenPrice>,
}

async fn report_all_prices(State(state): State<APIState>) -> impl IntoResponse {
    let prices = state.prices_source.borrow().clone();
    let response = AllPricesResponse {
        oracle: state.oracle.clone(),
        prices,
    };
    (StatusCode::OK, Json(response))
}

#[allow(clippy::large_enum_variant)]
pub enum Response {
    Synthetic(SyntheticPayloadEntry),
    Generic(GenericPayloadEntry),
    NotFound,
}
impl IntoResponse for Response {
    fn into_response(self) -> axum::response::Response {
        match self {
            Response::Synthetic(entry) => (StatusCode::OK, Json(entry)).into_response(),
            Response::Generic(entry) => (StatusCode::OK, Json(entry)).into_response(),
            Response::NotFound => {
                (StatusCode::NOT_FOUND, Json(json!({ "error": "Not Found" }))).into_response()
            }
        }
    }
}

async fn report_payload(
    Path(feed): Path<String>,
    State(state): State<APIState>,
) -> impl IntoResponse {
    let payload = state.payload_source.borrow().clone();
    if let Some(entry) = payload.synthetics.into_iter().find(|p| p.synthetic == feed) {
        return Response::Synthetic(entry);
    }
    if let Some(entry) = payload.generics.into_iter().find(|p| p.name == feed) {
        return Response::Generic(entry);
    }
    Response::NotFound
}

// ============================================================================
// Test endpoints for liquidation testing (TEMPORARY - remove before production)
// ============================================================================

/// Set a custom test price for a token
async fn set_test_price(
    State(state): State<APIState>,
    Json(override_req): Json<TestPriceOverride>,
) -> impl IntoResponse {
    let key = format!("{}-{}", override_req.token, override_req.unit);
    info!(
        "Setting test price override: {} = {} {}",
        override_req.token, override_req.value, override_req.unit
    );
    state.test_price_overrides.insert(key.clone(), override_req.clone());
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "message": format!("Test price set for {}", key),
            "override": override_req
        })),
    )
}

/// Get all currently active test price overrides
async fn get_test_prices(State(state): State<APIState>) -> impl IntoResponse {
    let overrides: Vec<TestPriceOverride> = state
        .test_price_overrides
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    (StatusCode::OK, Json(json!({ "overrides": overrides })))
}

/// Clear all test price overrides
async fn clear_test_prices(State(state): State<APIState>) -> impl IntoResponse {
    let count = state.test_price_overrides.len();
    state.test_price_overrides.clear();
    info!("Cleared {} test price overrides", count);
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "message": format!("Cleared {} test price overrides", count)
        })),
    )
}
