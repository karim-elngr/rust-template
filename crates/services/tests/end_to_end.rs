//! Real HTTP, gRPC, PostgreSQL and Kafka pipeline tests; requires Docker.
mod support;
use anyhow::{Context, Result};
use commerce::{AddCartItem, CheckoutCommand, CheckoutReceipt, CreateCart, TraceContext};
use contracts::commerce::v1::{
    self, cart_service_client::CartServiceClient, cart_service_server::CartServiceServer,
    checkout_service_client::CheckoutServiceClient, checkout_service_server::CheckoutServiceServer,
    order_service_client::OrderServiceClient, order_service_server::OrderServiceServer,
    payment_service_client::PaymentServiceClient, payment_service_server::PaymentServiceServer,
};
use rdkafka::{Offset, consumer::Consumer, producer::FutureRecord};
use services::{api, grpc::GrpcCommerce, kafka};
use std::{sync::Arc, time::Duration};
use support::{Fixture, Running, committed_offset, wait_for_offset};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn submit(fixture: &Fixture) -> Result<CheckoutReceipt> {
    let cart = fixture
        .store
        .create_cart(CreateCart {
            customer_id: Uuid::new_v4(),
        })
        .await?;
    let cart = fixture
        .store
        .add_item(
            cart.id,
            AddCartItem {
                sku: "BOOK".into(),
                quantity: 2,
                expected_version: cart.version,
            },
        )
        .await?;
    Ok(fixture
        .store
        .checkout(
            CheckoutCommand {
                idempotency_key: Uuid::new_v4(),
                cart_id: cart.id,
                expected_cart_version: cart.version,
                payment_method_token: "pm_demo_approve".into(),
            },
            TraceContext::new(),
        )
        .await?)
}

async fn send(
    producer: &rdkafka::producer::FutureProducer,
    topic: &str,
    event: &commerce::CommerceEvent,
) -> Result<i64> {
    let bytes = serde_json::to_vec(event)?;
    let key = event.aggregate_id.to_string();
    let delivery = producer
        .send(
            FutureRecord::to(topic).key(&key).payload(&bytes),
            Duration::from_secs(1),
        )
        .await
        .map_err(|(error, _)| error)?;
    Ok(delivery.offset)
}

async fn servers(
    fixture: &Fixture,
) -> Result<(String, tonic::transport::Channel, Running, Running)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let http_base = format!("http://{}", listener.local_addr()?);
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let router = api::router(fixture.store.clone(), Duration::from_secs(10));
    let http = Running {
        cancel,
        task: tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await?;
            Ok(())
        }),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let service = GrpcCommerce {
        store: fixture.store.clone(),
    };
    let grpc = Running {
        cancel,
        task: tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CartServiceServer::new(service.clone()))
                .add_service(CheckoutServiceServer::new(service.clone()))
                .add_service(OrderServiceServer::new(service.clone()))
                .add_service(PaymentServiceServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                )
                .await?;
            Ok(())
        }),
    };
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .connect()
        .await?;
    Ok((http_base, channel, http, grpc))
}

async fn wait_for_summary(fixture: &Fixture, order_id: Uuid, expected: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(summary) = fixture.store.get_order_summary(order_id).await
                && summary.status.as_str() == expected
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("saga projection did not reach expected terminal state")?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn http_and_grpc_drive_cart_authorization_capture_confirmation_and_projection() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let (base, channel, http, grpc) = servers(&fixture).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let response = client
        .post(format!("{base}/v1/carts"))
        .json(&serde_json::json!({"customer_id":Uuid::new_v4()}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let empty: serde_json::Value = response.json().await?;
    assert_eq!(empty["items"], serde_json::json!([]));
    let cart_id = empty["cart_id"].as_str().context("cart id")?.to_owned();
    let mut carts = CartServiceClient::new(channel.clone());
    let cart = carts
        .add_item(v1::AddCartItemRequest {
            cart_id: cart_id.clone(),
            sku: "BOOK".into(),
            quantity: 2,
            expected_version: empty["version"].as_i64().unwrap(),
        })
        .await?
        .into_inner();
    assert_eq!(
        cart.total_minor, 5000,
        "prices come from the server catalog"
    );
    let stale = client
        .put(format!("{base}/v1/carts/{cart_id}/items"))
        .json(&serde_json::json!({"sku":"BOOK","quantity":1,"expected_version":empty["version"]}))
        .send()
        .await?;
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
    let updated = client
        .put(format!("{base}/v1/carts/{cart_id}/items"))
        .json(&serde_json::json!({"sku":"BOOK","quantity":2,"expected_version":cart.version}))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(updated["total_minor"], 5000, "PUT sets the quantity");
    let expected_version = updated["version"].as_i64().unwrap();
    let removed = client
        .delete(format!(
            "{base}/v1/carts/{cart_id}/items/BOOK?expected_version={expected_version}"
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(removed["items"], serde_json::json!([]));
    let cart = carts
        .add_item(v1::AddCartItemRequest {
            cart_id: cart_id.clone(),
            sku: "BOOK".into(),
            quantity: 2,
            expected_version: removed["version"].as_i64().unwrap(),
        })
        .await?
        .into_inner();
    let key = Uuid::new_v4();
    let body = serde_json::json!({"cart_id":cart_id,"expected_cart_version":cart.version,"payment_method_token":"pm_demo_approve"});
    let response = client
        .post(format!("{base}/v1/checkouts"))
        .header("Idempotency-Key", key.to_string())
        .json(&body)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    let receipt: serde_json::Value = response.json().await?;
    let checkout_id = receipt["checkout_id"].as_str().unwrap().to_owned();
    let order_id = Uuid::parse_str(receipt["order_id"].as_str().unwrap())?;
    let mut checkouts = CheckoutServiceClient::new(channel.clone());
    let replay_request = v1::CheckoutRequest {
        idempotency_key: key.to_string(),
        cart_id: cart_id.clone(),
        expected_cart_version: cart.version,
        payment_method_token: "pm_demo_approve".into(),
    };
    let repeated = checkouts
        .checkout(replay_request.clone())
        .await?
        .into_inner();
    assert_eq!(repeated.order_id, order_id.to_string());
    let producer = kafka::producer(&fixture.config)?;
    let consumer = Arc::new(kafka::consumer(&fixture.config)?);
    let worker = fixture.consume(consumer.clone());
    let poller = fixture.poll()?;
    wait_for_summary(&fixture, order_id, "confirmed").await?;
    let live = checkouts
        .get_checkout(v1::GetCheckoutRequest {
            checkout_id: checkout_id.clone(),
        })
        .await?
        .into_inner();
    assert_eq!(live.status, "completed");
    let replay = checkouts.checkout(replay_request).await?.into_inner();
    assert_eq!(
        replay, repeated,
        "accepted idempotency receipt is immutable after completion"
    );
    let mut orders = OrderServiceClient::new(channel.clone());
    let mut payments = PaymentServiceClient::new(channel);
    let order = orders
        .get_order(v1::GetOrderRequest {
            order_id: order_id.to_string(),
        })
        .await?
        .into_inner();
    assert_eq!(order.status, "confirmed");
    let payment = payments
        .get_payment(v1::GetPaymentRequest {
            payment_id: receipt["payment_id"].as_str().unwrap().into(),
        })
        .await?
        .into_inner();
    assert_eq!(payment.status, "captured");
    let public_payment = client
        .get(format!("{base}/v1/payments/{}", payment.payment_id))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert!(!public_payment.contains("pm_demo"));
    assert!(!public_payment.contains("payment_method_token"));
    let summary = client
        .get(format!("{base}/v1/orders/{order_id}/summary"))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(summary["status"], "confirmed");
    assert_eq!(summary["payment_status"], "captured");
    // Replaying the actual capture request must not issue another durable operation.
    let event = fixture
        .event(Uuid::parse_str(&checkout_id)?, "payment.capture_requested")
        .await?;
    let before = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM payment_operations")
        .fetch_one(fixture.store.pool())
        .await?;
    let offset = send(&producer, &fixture.config.topic, &event).await?;
    wait_for_offset(&consumer, &fixture.config.topic, offset + 1).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM payment_operations")
            .fetch_one(fixture.store.pool())
            .await?,
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM inbox WHERE event_id=$1")
            .bind(event.event_id)
            .fetch_one(fixture.store.pool())
            .await?,
        1
    );
    poller.stop().await?;
    worker.stop().await?;
    http.stop().await?;
    grpc.stop().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn poison_record_is_not_committed_and_cannot_skip_following_valid_event() -> Result<()> {
    let fixture = Fixture::new().await?;
    let receipt = submit(&fixture).await?;
    let event = fixture
        .event(receipt.checkout_id, "checkout.started")
        .await?;
    let producer = kafka::producer(&fixture.config)?;
    producer
        .send(
            FutureRecord::to(&fixture.config.topic)
                .key("poison")
                .payload("not-json"),
            Duration::from_secs(1),
        )
        .await
        .map_err(|(error, _)| error)?;
    send(&producer, &fixture.config.topic, &event).await?;
    for attempt in 1..=2 {
        let consumer = Arc::new(kafka::consumer(&fixture.config)?);
        eprintln!("starting poison replay attempt {attempt}");
        let failure = fixture
            .consume(consumer.clone())
            .expect_failure()
            .await
            .with_context(|| format!("poison replay attempt {attempt}"))?;
        assert!(format!("{failure:#}").contains("invalid commerce event encoding"));
        eprintln!("poison replay attempt {attempt} stopped at the failed record");
        assert_eq!(
            committed_offset(&consumer, &fixture.config.topic).await?,
            Offset::Invalid
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM inbox")
                .fetch_one(fixture.store.pool())
                .await?,
            0
        );
        assert_eq!(
            fixture
                .store
                .get_order(receipt.order_id)
                .await?
                .status
                .as_str(),
            "pending_payment"
        );
        consumer.unsubscribe();
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn database_failure_preserves_offset_and_replays_payment_operation_after_repair() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let receipt = submit(&fixture).await?;
    let started = fixture
        .event(receipt.checkout_id, "checkout.started")
        .await?;
    fixture.store.apply_event(&started).await?;
    let event = fixture
        .event(receipt.checkout_id, "payment.authorization_requested")
        .await?;
    let authorization: (String, serde_json::Value) = sqlx::query_as(
        "SELECT operation_key,request FROM payment_operations WHERE payment_id=$1 AND kind='authorize'",
    )
    .bind(receipt.payment_id)
    .fetch_one(fixture.store.pool())
    .await?;
    sqlx::raw_sql("CREATE FUNCTION fail_payment() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected payment failure'; END $$; CREATE TRIGGER fail_payment BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION fail_payment();").execute(fixture.store.pool()).await?;
    let producer = kafka::producer(&fixture.config)?;
    send(&producer, &fixture.config.topic, &event).await?;
    let consumer = Arc::new(kafka::consumer(&fixture.config)?);
    let failure = fixture.consume(consumer.clone()).expect_failure().await?;
    assert!(format!("{failure:#}").contains("event processing failed; offset was not committed"));
    assert_eq!(
        committed_offset(&consumer, &fixture.config.topic).await?,
        Offset::Invalid
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM inbox WHERE event_id=$1")
            .bind(event.event_id)
            .fetch_one(fixture.store.pool())
            .await?,
        0
    );
    let failed_status: (String, i32) =
        sqlx::query_as("SELECT status,attempts FROM payment_operations WHERE operation_key=$1")
            .bind(&authorization.0)
            .fetch_one(fixture.store.pool())
            .await?;
    assert_eq!(failed_status, ("pending".into(), 1));
    consumer.unsubscribe();
    drop(consumer);
    sqlx::query("DROP TRIGGER fail_payment ON payments")
        .execute(fixture.store.pool())
        .await?;
    let consumer = Arc::new(kafka::consumer(&fixture.config)?);
    let worker = fixture.consume(consumer.clone());
    wait_for_offset(&consumer, &fixture.config.topic, 1).await?;
    worker.stop().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM inbox WHERE event_id=$1")
            .bind(event.event_id)
            .fetch_one(fixture.store.pool())
            .await?,
        1
    );
    let replayed: (String, serde_json::Value, String, i32) = sqlx::query_as(
        "SELECT operation_key,request,status,attempts FROM payment_operations WHERE payment_id=$1 AND kind='authorize'",
    )
    .bind(receipt.payment_id)
    .fetch_one(fixture.store.pool())
    .await?;
    assert_eq!((replayed.0, replayed.1), authorization);
    assert_eq!((replayed.2.as_str(), replayed.3), ("completed", 2));
    let capture: (String, i32) = sqlx::query_as(
        "SELECT status,attempts FROM payment_operations WHERE payment_id=$1 AND kind='capture'",
    )
    .bind(receipt.payment_id)
    .fetch_one(fixture.store.pool())
    .await?;
    assert_eq!(capture, ("pending".into(), 0));
    assert_eq!(
        fixture
            .store
            .get_payment(receipt.payment_id)
            .await?
            .status
            .as_str(),
        "capture_pending"
    );
    Ok(())
}
