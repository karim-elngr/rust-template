//! Real PostgreSQL contract tests. Run with Docker: cargo test -p commerce --test postgres -- --ignored

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use commerce::{
    AddCartItem, AppError, ApplyOutcome, CheckoutCommand, CheckoutReceipt, CommerceEvent,
    CreateCart, RemoveCartItem, Store, TraceContext,
};
use payments::{DemoGateway, OperationKind, PaymentGateway, PaymentOperation, ProviderOutcome};
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn image_from_reference(reference: &str) -> Result<GenericImage, Box<dyn Error + Send + Sync>> {
    let (name, tag) = reference
        .rsplit_once(':')
        .ok_or("test image requires an explicit tag or digest")?;
    if name.is_empty()
        || tag.is_empty()
        || tag.contains('/')
        || reference.chars().any(char::is_whitespace)
    {
        return Err("invalid test image reference".into());
    }
    // GenericImage rejoins these fields with ':', preserving repo@sha256:digest.
    Ok(GenericImage::new(name, tag))
}

fn configured_image(
    variable: &str,
    default: &str,
) -> Result<GenericImage, Box<dyn Error + Send + Sync>> {
    let reference = match std::env::var(variable) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => default.into(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{variable} must contain valid Unicode").into());
        }
    };
    image_from_reference(&reference)
}

#[test]
fn container_image_references_preserve_tags_ports_and_digests() {
    for reference in [
        "postgres:18.6-bookworm",
        "registry.example:5000/company/postgres:18.6",
        "registry.example/company/postgres@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ] {
        let image = image_from_reference(reference).expect("valid explicit image reference");
        assert_eq!(
            testcontainers::ContainerRequest::from(image).descriptor(),
            reference
        );
    }
    for invalid in [
        "postgres",
        "postgres:",
        ":18.6",
        "registry.example:5000/postgres",
    ] {
        assert!(image_from_reference(invalid).is_err());
    }
}

struct Database {
    store: Store,
    _container: ContainerAsync<GenericImage>,
}

impl Database {
    async fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let database = Self::unmigrated().await?;
        database.store.migrate().await?;
        Ok(database)
    }

    async fn unmigrated() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let container = configured_image("TEST_POSTGRES_IMAGE", "postgres:18.6-bookworm")?
            .with_exposed_port(5432.tcp())
            // The temporary init server logs to stdout; final TCP server readiness is stderr.
            .with_wait_for(WaitFor::log(LogWaitStrategy::stderr(
                "database system is ready to accept connections",
            )))
            .with_env_var("POSTGRES_PASSWORD", "test-only-password")
            .with_env_var("POSTGRES_DB", "commerce")
            .with_startup_timeout(std::time::Duration::from_secs(90))
            .start()
            .await?;
        let url = format!(
            "postgres://postgres:test-only-password@{}:{}/commerce",
            container.get_host().await?,
            container.get_host_port_ipv4(5432).await?
        );
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&url)
            .await?;
        let store = Store::new(pool);
        Ok(Self {
            store,
            _container: container,
        })
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn readiness_requires_the_embedded_schema_migration() -> TestResult {
    let db = Database::unmigrated().await?;
    assert!(db.store.check_ready().await.is_err());
    db.store.migrate().await?;
    db.store.check_ready().await?;
    sqlx::query("UPDATE _sqlx_migrations SET success = false")
        .execute(db.store.pool())
        .await?;
    assert!(db.store.check_ready().await.is_err());
    sqlx::query("UPDATE _sqlx_migrations SET success=true,checksum=decode('00','hex')")
        .execute(db.store.pool())
        .await?;
    assert!(db.store.check_ready().await.is_err());
    Ok(())
}

async fn command(store: &Store) -> Result<CheckoutCommand, Box<dyn Error + Send + Sync>> {
    let cart = store
        .create_cart(CreateCart {
            customer_id: Uuid::new_v4(),
        })
        .await?;
    let cart = store
        .add_item(
            cart.id,
            AddCartItem {
                sku: "BOOK".into(),
                quantity: 2,
                expected_version: cart.version,
            },
        )
        .await?;
    let cart = store
        .add_item(
            cart.id,
            AddCartItem {
                sku: "COFFEE".into(),
                quantity: 1,
                expected_version: cart.version,
            },
        )
        .await?;
    Ok(CheckoutCommand {
        idempotency_key: Uuid::new_v4(),
        cart_id: cart.id,
        expected_cart_version: cart.version,
        payment_method_token: "pm_demo_approve".into(),
    })
}

async fn submit(store: &Store) -> Result<CheckoutReceipt, Box<dyn Error + Send + Sync>> {
    Ok(store
        .checkout(command(store).await?, TraceContext::new())
        .await?)
}

async fn event(
    pool: &PgPool,
    checkout_id: Uuid,
    kind: &str,
) -> Result<CommerceEvent, Box<dyn Error + Send + Sync>> {
    let payload: serde_json::Value = sqlx::query_scalar("SELECT payload FROM outbox WHERE checkout_id=$1 AND event_type=$2 ORDER BY created_at,id LIMIT 1")
        .bind(checkout_id).bind(kind).fetch_one(pool).await?;
    Ok(serde_json::from_value(payload)?)
}

async fn apply(store: &Store, checkout_id: Uuid, kind: &str) -> TestResult {
    store
        .apply_event(&event(store.pool(), checkout_id, kind).await?)
        .await?;
    Ok(())
}

/// Deliver persisted envelopes, including newly emitted saga steps, until quiescent.
async fn drain(store: &Store) -> TestResult {
    for _ in 0..100 {
        let payload: Option<serde_json::Value> = sqlx::query_scalar("SELECT payload FROM outbox o WHERE NOT EXISTS(SELECT 1 FROM inbox i WHERE i.consumer_name='commerce-saga-v1' AND i.event_id=o.id) ORDER BY created_at,id LIMIT 1")
            .fetch_optional(store.pool()).await?;
        let Some(payload) = payload else {
            return Ok(());
        };
        store.apply_event(&serde_json::from_value(payload)?).await?;
    }
    Err("saga did not become quiescent after 100 envelopes".into())
}

async fn count(pool: &PgPool, query: &'static str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(query).fetch_one(pool).await
}

async fn expire(pool: &PgPool, checkout_id: Uuid) -> TestResult {
    // Keep the aggregate's valid created-before-expiry invariant while ageing it.
    sqlx::query("WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now) UPDATE checkouts SET expires_at=observed.now-interval '1 second', data=jsonb_set(jsonb_set(data,'{created_at}',to_jsonb(observed.now-interval '31 minutes')),'{expires_at}',to_jsonb(observed.now-interval '1 second')) FROM observed WHERE id=$1")
        .bind(checkout_id).execute(pool).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn empty_cart_uses_server_prices_and_optimistic_set_and_remove_commands() -> TestResult {
    let db = Database::new().await?;
    let cart = db
        .store
        .create_cart(CreateCart {
            customer_id: Uuid::new_v4(),
        })
        .await?;
    assert!(cart.items.is_empty());
    assert_eq!(cart.total_minor, 0);
    let add = AddCartItem {
        sku: "BOOK".into(),
        quantity: 2,
        expected_version: cart.version,
    };
    let (first, second) = tokio::join!(
        db.store.add_item(cart.id, add.clone()),
        db.store.add_item(cart.id, add)
    );
    let added = match (first, second) {
        (Ok(cart), Err(AppError::VersionConflict)) | (Err(AppError::VersionConflict), Ok(cart)) => {
            cart
        }
        other => panic!("expected one version winner: {other:?}"),
    };
    assert_eq!(added.total_minor, 5000);
    let set = db
        .store
        .add_item(
            cart.id,
            AddCartItem {
                sku: "BOOK".into(),
                quantity: 1,
                expected_version: added.version,
            },
        )
        .await?;
    assert_eq!(set.total_minor, 2500);
    assert_eq!(set.items[0].quantity, 1);
    assert!(
        db.store
            .add_item(
                cart.id,
                AddCartItem {
                    sku: "UNKNOWN".into(),
                    quantity: 1,
                    expected_version: set.version
                }
            )
            .await
            .is_err()
    );
    assert_eq!(db.store.get_cart(cart.id).await?.version, set.version);
    let empty = db
        .store
        .remove_item(
            cart.id,
            RemoveCartItem {
                sku: "BOOK".into(),
                expected_version: set.version,
            },
        )
        .await?;
    assert!(empty.items.is_empty());
    assert!(
        db.store
            .checkout(
                CheckoutCommand {
                    idempotency_key: Uuid::new_v4(),
                    cart_id: cart.id,
                    expected_cart_version: empty.version,
                    payment_method_token: "pm_demo_approve".into()
                },
                TraceContext::new()
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn checkout_capture_and_order_confirmation_are_separate_and_receipt_is_immutable()
-> TestResult {
    let db = Database::new().await?;
    let cmd = command(&db.store).await?;
    let receipt = db.store.checkout(cmd.clone(), TraceContext::new()).await?;
    assert_eq!(receipt.total_minor, 5700);
    assert_eq!(
        db.store.get_cart(receipt.cart_id).await?.status.as_str(),
        "checked_out"
    );
    assert!(matches!(
        db.store.get_order_summary(receipt.order_id).await,
        Err(AppError::NotFound)
    ));
    apply(
        &db.store,
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    assert_eq!(
        db.store
            .get_payment(receipt.payment_id)
            .await?
            .status
            .as_str(),
        "capture_pending"
    );
    apply(&db.store, receipt.checkout_id, "payment.capture_requested").await?;
    assert_eq!(
        db.store
            .get_payment(receipt.payment_id)
            .await?
            .status
            .as_str(),
        "captured"
    );
    assert_eq!(
        db.store.get_order(receipt.order_id).await?.status.as_str(),
        "pending_payment"
    );
    assert_eq!(
        db.store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "awaiting_order_confirmation"
    );
    apply(
        &db.store,
        receipt.checkout_id,
        "order.confirmation_requested",
    )
    .await?;
    assert_eq!(
        db.store.get_order(receipt.order_id).await?.status.as_str(),
        "confirmed"
    );
    assert_eq!(db.store.checkout(cmd, TraceContext::new()).await?, receipt);
    assert!(matches!(
        db.store.cancel_checkout(receipt.checkout_id).await,
        Err(AppError::InvalidState(_))
    ));
    drain(&db.store).await?;
    let summary = db.store.get_order_summary(receipt.order_id).await?;
    assert_eq!(summary.status.as_str(), "confirmed");
    assert_eq!(summary.payment_status, "captured");
    for query in [
        "SELECT count(*) FROM orders",
        "SELECT count(*) FROM payments",
        "SELECT count(*) FROM checkouts",
        "SELECT count(*) FROM checkout_requests",
    ] {
        assert_eq!(count(db.store.pool(), query).await?, 1);
    }
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM payment_operations").await?,
        2
    );
    let envelopes: String = sqlx::query_scalar("SELECT string_agg(payload::text,'') FROM outbox")
        .fetch_one(db.store.pool())
        .await?;
    assert!(!envelopes.contains("pm_demo"));
    assert!(!envelopes.contains("payment_method_token"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn final_write_failure_rolls_back_entire_checkout_and_cart_transition() -> TestResult {
    let db = Database::new().await?;
    let cmd = command(&db.store).await?;
    sqlx::raw_sql("CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$; CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox();").execute(db.store.pool()).await?;
    assert!(
        db.store
            .checkout(cmd.clone(), TraceContext::new())
            .await
            .is_err()
    );
    for query in [
        "SELECT count(*) FROM orders",
        "SELECT count(*) FROM payments",
        "SELECT count(*) FROM checkouts",
        "SELECT count(*) FROM outbox",
        "SELECT count(*) FROM checkout_requests",
        "SELECT count(*) FROM payment_operations",
    ] {
        assert_eq!(
            count(db.store.pool(), query).await?,
            0,
            "{query} must return zero after rollback"
        );
    }
    let cart = db.store.get_cart(cmd.cart_id).await?;
    assert_eq!(cart.status.as_str(), "open");
    assert_eq!(cart.version, cmd.expected_cart_version);
    sqlx::query("DROP TRIGGER reject_outbox ON outbox")
        .execute(db.store.pool())
        .await?;
    db.store.checkout(cmd, TraceContext::new()).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn concurrent_identical_retries_create_one_order_and_changed_request_conflicts() -> TestResult
{
    let db = Database::new().await?;
    let cmd = command(&db.store).await?;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let store = db.store.clone();
        let request = cmd.clone();
        tasks.spawn(async move { store.checkout(request, TraceContext::new()).await });
    }
    let mut receipts = Vec::new();
    while let Some(result) = tasks.join_next().await {
        receipts.push(result??);
    }
    assert!(receipts.iter().all(|value| value == &receipts[0]));
    let mut changed = cmd;
    changed.payment_method_token = "pm_demo_decline".into();
    assert!(matches!(
        db.store.checkout(changed, TraceContext::new()).await,
        Err(AppError::IdempotencyConflict)
    ));
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM orders").await?,
        1
    );
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM payment_operations").await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn simultaneous_different_requests_with_same_key_have_one_winner() -> TestResult {
    let db = Database::new().await?;
    let first = command(&db.store).await?;
    let mut second = first.clone();
    second.payment_method_token = "pm_demo_decline".into();
    let (first, second) = tokio::join!(
        db.store.checkout(first, TraceContext::new()),
        db.store.checkout(second, TraceContext::new())
    );
    match (first, second) {
        (Ok(_), Err(AppError::IdempotencyConflict))
        | (Err(AppError::IdempotencyConflict), Ok(_)) => {}
        outcomes => panic!("expected one winner and one idempotency conflict: {outcomes:?}"),
    }
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM checkouts").await?,
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn provider_finalization_rolls_back_inbox_and_replays_duplicate_once() -> TestResult {
    let db = Database::new().await?;
    let gateway = Arc::new(RetryOnceGateway {
        calls: Mutex::new(Vec::new()),
        count: AtomicUsize::new(1), // Record every request and succeed immediately.
    });
    let store = Store::with_gateway(db.store.pool().clone(), gateway.clone());
    let receipt = submit(&store).await?;
    let message = event(
        store.pool(),
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    sqlx::raw_sql("CREATE FUNCTION reject_settlement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected settlement failure'; END $$; CREATE TRIGGER reject_settlement BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION reject_settlement();").execute(store.pool()).await?;
    assert!(store.apply_event(&message).await.is_err());
    assert_eq!(count(store.pool(), "SELECT count(*) FROM inbox").await?, 0);
    assert_eq!(
        store.get_payment(receipt.payment_id).await?.status.as_str(),
        "authorization_pending"
    );
    assert_eq!(
        count(
            store.pool(),
            "SELECT count(*) FROM payment_operations WHERE status='completed'"
        )
        .await?,
        0
    );
    sqlx::query("DROP TRIGGER reject_settlement ON payments")
        .execute(store.pool())
        .await?;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = store.clone();
        let message = message.clone();
        tasks.spawn(async move { store.apply_event(&message).await });
    }
    let mut applied = 0;
    while let Some(result) = tasks.join_next().await {
        if result?? == ApplyOutcome::Applied {
            applied += 1;
        }
    }
    assert_eq!(applied, 1);
    assert_eq!(
        count(
            store.pool(),
            "SELECT count(*) FROM payment_operations WHERE kind='authorize' AND status='completed'"
        )
        .await?,
        1
    );
    assert_eq!(
        store.get_payment(receipt.payment_id).await?.status.as_str(),
        "capture_pending"
    );
    let calls = gateway.calls.lock().unwrap();
    assert!(calls.len() >= 2);
    assert!(calls.iter().all(|operation| operation == &calls[0]));
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn expired_checkout_cron_and_consumer_race_cannot_confirm_or_keep_authorization() -> TestResult
{
    let db = Database::new().await?;
    let receipt = submit(&db.store).await?;
    let message = event(
        db.store.pool(),
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    expire(db.store.pool(), receipt.checkout_id).await?;
    let (applied, expired) =
        tokio::join!(db.store.apply_event(&message), db.store.expire_pending(100));
    applied?;
    expired?;
    drain(&db.store).await?;
    assert_eq!(
        db.store.get_order(receipt.order_id).await?.status.as_str(),
        "cancelled"
    );
    assert_eq!(
        db.store
            .get_payment(receipt.payment_id)
            .await?
            .status
            .as_str(),
        "voided"
    );
    assert_eq!(
        db.store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "cancelled"
    );
    assert_eq!(
        count(
            db.store.pool(),
            "SELECT count(*) FROM payment_operations WHERE kind='capture'"
        )
        .await?,
        0
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn stale_outbox_owner_cannot_ack_or_release_new_lease() -> TestResult {
    let db = Database::new().await?;
    submit(&db.store).await?;
    let claimed = db.store.claim_outbox(100, 30).await?;
    let first = &claimed[0];
    assert!(db.store.claim_outbox(100, 30).await?.is_empty());
    sqlx::query("UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(first.id)
        .execute(db.store.pool())
        .await?;
    let second = db.store.claim_outbox(1, 30).await?.remove(0);
    assert_eq!(second.id, first.id);
    assert_ne!(first.lease_token, second.lease_token);
    assert_eq!(second.attempts, 2);
    assert!(!db.store.mark_published(first.id, first.lease_token).await?);
    assert!(
        !db.store
            .release_outbox(first.id, first.lease_token, "stale")
            .await?
    );
    assert!(
        db.store
            .mark_published(second.id, second.lease_token)
            .await?
    );
    assert!(db.store.claim_outbox(100, 30).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn publishers_claim_disjoint_batches_and_failed_send_backs_off() -> TestResult {
    let db = Database::new().await?;
    for _ in 0..6 {
        submit(&db.store).await?;
    }
    let (first, second) = tokio::join!(db.store.claim_outbox(3, 30), db.store.claim_outbox(3, 30));
    let first = first?;
    let second = second?;
    assert_eq!(first.len() + second.len(), 6);
    assert!(
        first
            .iter()
            .all(|item| second.iter().all(|other| item.id != other.id))
    );
    db.store.claim_outbox(100, 30).await?; // Lease the remaining independent saga envelopes.
    let failed = &first[0];
    assert!(
        db.store
            .release_outbox(failed.id, failed.lease_token, "broker unavailable")
            .await?
    );
    assert!(
        db.store.claim_outbox(100, 30).await?.is_empty(),
        "released item must back off"
    );
    sqlx::query("UPDATE outbox SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(failed.id)
        .execute(db.store.pool())
        .await?;
    let retry = db.store.claim_outbox(1, 30).await?.remove(0);
    assert_eq!(retry.id, failed.id);
    assert_eq!(retry.attempts, 2);
    assert_ne!(retry.lease_token, failed.lease_token);
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn tampered_envelope_is_rejected_and_declined_authorization_never_captures() -> TestResult {
    let db = Database::new().await?;
    let mut cmd = command(&db.store).await?;
    cmd.payment_method_token = "pm_demo_decline".into();
    let receipt = db.store.checkout(cmd, TraceContext::new()).await?;
    let original = event(
        db.store.pool(),
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    let mut tampered = original.clone();
    tampered.data["payment_id"] = serde_json::json!(Uuid::new_v4());
    assert!(matches!(
        db.store.apply_event(&tampered).await,
        Err(AppError::InvalidEvent(_))
    ));
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM inbox").await?,
        0
    );
    drain(&db.store).await?;
    assert_eq!(
        db.store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "failed"
    );
    assert_eq!(
        db.store.get_order(receipt.order_id).await?.status.as_str(),
        "cancelled"
    );
    assert_eq!(
        count(
            db.store.pool(),
            "SELECT count(*) FROM payment_operations WHERE kind='capture'"
        )
        .await?,
        0
    );
    Ok(())
}

#[derive(Default)]
struct RetryOnceGateway {
    calls: Mutex<Vec<PaymentOperation>>,
    count: AtomicUsize,
}
#[async_trait::async_trait]
impl PaymentGateway for RetryOnceGateway {
    async fn execute(&self, operation: &PaymentOperation) -> ProviderOutcome {
        self.calls.lock().unwrap().push(operation.clone());
        if self.count.fetch_add(1, Ordering::SeqCst) == 0 {
            return ProviderOutcome::Retryable {
                code: "transport_uncertain".into(),
            };
        }
        DemoGateway.execute(operation).await
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn uncertain_provider_outcome_retains_operation_identity_and_inbox_is_unacknowledged()
-> TestResult {
    let db = Database::new().await?;
    let gateway = Arc::new(RetryOnceGateway::default());
    let store = Store::with_gateway(db.store.pool().clone(), gateway.clone());
    let receipt = submit(&store).await?;
    let message = event(
        store.pool(),
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    assert!(matches!(
        store.apply_event(&message).await,
        Err(AppError::RetryableProvider)
    ));
    assert_eq!(count(store.pool(), "SELECT count(*) FROM inbox").await?, 0);
    assert_eq!(
        store.get_payment(receipt.payment_id).await?.status.as_str(),
        "authorization_pending"
    );
    assert_eq!(
        count(
            store.pool(),
            "SELECT count(*) FROM payment_operations WHERE status='pending' AND attempts=1"
        )
        .await?,
        1
    );
    store.apply_event(&message).await?;
    {
        let calls = gateway.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], calls[1]);
    }
    assert_eq!(count(store.pool(), "SELECT count(*) FROM payment_operations WHERE kind='authorize' AND status='completed' AND attempts=2").await?, 1);
    drain(&store).await?;
    assert_eq!(
        store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "completed"
    );
    Ok(())
}

struct GatedGateway {
    kind: OperationKind,
    fail: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
impl GatedGateway {
    fn new(kind: OperationKind) -> Self {
        Self {
            kind,
            fail: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}
#[async_trait::async_trait]
impl PaymentGateway for GatedGateway {
    async fn execute(&self, operation: &PaymentOperation) -> ProviderOutcome {
        if operation.kind == self.kind {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            if self.fail {
                return ProviderOutcome::DefinitiveFailure {
                    code: "authorization_declined".into(),
                };
            }
        }
        DemoGateway.execute(operation).await
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn cancellation_during_provider_io_takes_no_database_lock_and_compensates_late_success()
-> TestResult {
    let db = Database::new().await?;
    for (kind, request, terminal, fail) in [
        (
            OperationKind::Authorize,
            "payment.authorization_requested",
            "voided",
            false,
        ),
        (
            OperationKind::Capture,
            "payment.capture_requested",
            "refunded",
            false,
        ),
        (
            OperationKind::Authorize,
            "payment.authorization_requested",
            "cancelled",
            true,
        ),
    ] {
        let mut gated = GatedGateway::new(kind);
        gated.fail = fail;
        let gateway = Arc::new(gated);
        let store = Store::with_gateway(db.store.pool().clone(), gateway.clone());
        let receipt = submit(&store).await?;
        if kind == OperationKind::Capture {
            apply(
                &store,
                receipt.checkout_id,
                "payment.authorization_requested",
            )
            .await?;
        }
        let message = event(store.pool(), receipt.checkout_id, request).await?;
        let processing = store.clone();
        let task = tokio::spawn(async move { processing.apply_event(&message).await });
        tokio::time::timeout(Duration::from_secs(2), gateway.entered.notified()).await?;
        let cancelled = tokio::time::timeout(
            Duration::from_secs(1),
            store.cancel_checkout(receipt.checkout_id),
        )
        .await;
        gateway.release.add_permits(1);
        cancelled??; // This would time out if the provider call held the saga row lock.
        task.await??;
        drain(&store).await?;
        assert_eq!(
            store.get_payment(receipt.payment_id).await?.status.as_str(),
            terminal
        );
        assert_eq!(
            store.get_order(receipt.order_id).await?.status.as_str(),
            "cancelled"
        );
        assert_eq!(
            store
                .get_checkout(receipt.checkout_id)
                .await?
                .status
                .as_str(),
            "cancelled"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn expiry_during_capture_schedules_refund_instead_of_confirming_order() -> TestResult {
    let db = Database::new().await?;
    let gateway = Arc::new(GatedGateway::new(OperationKind::Capture));
    let store = Store::with_gateway(db.store.pool().clone(), gateway.clone());
    let receipt = submit(&store).await?;
    apply(
        &store,
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    let message = event(
        store.pool(),
        receipt.checkout_id,
        "payment.capture_requested",
    )
    .await?;
    let processing = store.clone();
    let task = tokio::spawn(async move { processing.apply_event(&message).await });
    tokio::time::timeout(Duration::from_secs(2), gateway.entered.notified()).await?;
    expire(store.pool(), receipt.checkout_id).await?;
    gateway.release.add_permits(1);
    task.await??;
    drain(&store).await?;
    assert_eq!(
        store.get_payment(receipt.payment_id).await?.status.as_str(),
        "refunded"
    );
    assert_eq!(
        store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "cancelled"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn cancellation_after_capture_before_confirmation_refunds_and_stale_confirmation_cannot_confirm()
-> TestResult {
    let db = Database::new().await?;
    let receipt = submit(&db.store).await?;
    apply(
        &db.store,
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    apply(&db.store, receipt.checkout_id, "payment.capture_requested").await?;
    db.store.cancel_checkout(receipt.checkout_id).await?;
    apply(
        &db.store,
        receipt.checkout_id,
        "order.confirmation_requested",
    )
    .await?;
    drain(&db.store).await?;
    assert_eq!(
        db.store
            .get_payment(receipt.payment_id)
            .await?
            .status
            .as_str(),
        "refunded"
    );
    assert_eq!(
        db.store.get_order(receipt.order_id).await?.status.as_str(),
        "cancelled"
    );
    assert_eq!(
        db.store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "cancelled"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn domain_rejection_after_capture_refunds_and_marks_checkout_failed() -> TestResult {
    let db = Database::new().await?;
    let receipt = submit(&db.store).await?;
    apply(
        &db.store,
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    apply(&db.store, receipt.checkout_id, "payment.capture_requested").await?;
    // Model an order-side rejection committed before its confirmation handler.
    sqlx::query(
        "UPDATE orders SET data=jsonb_set(data,'{status}','\"cancelled\"'::jsonb) WHERE id=$1",
    )
    .bind(receipt.order_id)
    .execute(db.store.pool())
    .await?;
    apply(
        &db.store,
        receipt.checkout_id,
        "order.confirmation_requested",
    )
    .await?;
    drain(&db.store).await?;
    assert_eq!(
        db.store
            .get_payment(receipt.payment_id)
            .await?
            .status
            .as_str(),
        "refunded"
    );
    assert_eq!(
        db.store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "failed"
    );
    assert_eq!(
        db.store.get_order(receipt.order_id).await?.status.as_str(),
        "cancelled"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn declined_capture_voids_authorization_and_projection_cannot_regress() -> TestResult {
    let db = Database::new().await?;
    let mut cmd = command(&db.store).await?;
    cmd.payment_method_token = "pm_demo_capture_decline".into();
    let receipt = db.store.checkout(cmd, TraceContext::new()).await?;
    let oldest = event(db.store.pool(), receipt.checkout_id, "order.changed").await?;
    apply(
        &db.store,
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    apply(&db.store, receipt.checkout_id, "payment.capture_requested").await?;
    apply(&db.store, receipt.checkout_id, "payment.void_requested").await?;
    let newest: serde_json::Value = sqlx::query_scalar("SELECT payload FROM outbox WHERE checkout_id=$1 AND event_type='order.changed' ORDER BY (payload->'data'->>'version')::bigint DESC LIMIT 1").bind(receipt.checkout_id).fetch_one(db.store.pool()).await?;
    db.store
        .apply_event(&serde_json::from_value(newest)?)
        .await?;
    let summary = db.store.get_order_summary(receipt.order_id).await?;
    assert_eq!(summary.status.as_str(), "cancelled");
    assert_eq!(summary.payment_status, "voided");
    db.store.apply_event(&oldest).await?;
    let after = db.store.get_order_summary(receipt.order_id).await?;
    assert_eq!(after.version, summary.version);
    assert_eq!(after.payment_status, "voided");
    assert_eq!(
        db.store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "failed"
    );
    Ok(())
}

struct FailingCompensationGateway;
#[async_trait::async_trait]
impl PaymentGateway for FailingCompensationGateway {
    async fn execute(&self, operation: &PaymentOperation) -> ProviderOutcome {
        if matches!(operation.kind, OperationKind::Void | OperationKind::Refund) {
            ProviderOutcome::DefinitiveFailure {
                code: "compensation_rejected".into(),
            }
        } else {
            DemoGateway.execute(operation).await
        }
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn definitive_compensation_failure_requires_attention_instead_of_claiming_refund()
-> TestResult {
    let db = Database::new().await?;
    let store = Store::with_gateway(
        db.store.pool().clone(),
        Arc::new(FailingCompensationGateway),
    );
    let receipt = submit(&store).await?;
    apply(
        &store,
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    apply(&store, receipt.checkout_id, "payment.capture_requested").await?;
    store.cancel_checkout(receipt.checkout_id).await?;
    drain(&store).await?;
    assert_eq!(
        store
            .get_checkout(receipt.checkout_id)
            .await?
            .status
            .as_str(),
        "needs_attention"
    );
    assert_eq!(
        store.get_payment(receipt.payment_id).await?.status.as_str(),
        "needs_attention"
    );
    assert_eq!(
        store.get_order(receipt.order_id).await?.status.as_str(),
        "cancelled"
    );
    assert_eq!(
        store
            .get_order_summary(receipt.order_id)
            .await?
            .payment_status,
        "needs_attention"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn corrupted_snapshot_identity_is_rejected_before_commands_or_provider_io() -> TestResult {
    let db = Database::new().await?;
    let gateway = Arc::new(RetryOnceGateway::default());
    let store = Store::with_gateway(db.store.pool().clone(), gateway.clone());
    let receipt = submit(&store).await?;
    let replacement = Uuid::new_v4();

    let cart: serde_json::Value = sqlx::query_scalar("SELECT data FROM carts WHERE id=$1")
        .bind(receipt.cart_id)
        .fetch_one(store.pool())
        .await?;
    sqlx::query("UPDATE carts SET data=jsonb_set(data,'{id}',to_jsonb($2::uuid)) WHERE id=$1")
        .bind(receipt.cart_id)
        .bind(replacement)
        .execute(store.pool())
        .await?;
    assert!(matches!(
        store.get_cart(receipt.cart_id).await,
        Err(AppError::CorruptData)
    ));
    sqlx::query("UPDATE carts SET data=$2 WHERE id=$1")
        .bind(receipt.cart_id)
        .bind(cart)
        .execute(store.pool())
        .await?;

    let order: serde_json::Value = sqlx::query_scalar("SELECT data FROM orders WHERE id=$1")
        .bind(receipt.order_id)
        .fetch_one(store.pool())
        .await?;
    sqlx::query(
        "UPDATE orders SET data=jsonb_set(data,'{customer_id}',to_jsonb($2::uuid)) WHERE id=$1",
    )
    .bind(receipt.order_id)
    .bind(replacement)
    .execute(store.pool())
    .await?;
    assert!(matches!(
        store.get_order(receipt.order_id).await,
        Err(AppError::CorruptData)
    ));
    sqlx::query("UPDATE orders SET data=$2 WHERE id=$1")
        .bind(receipt.order_id)
        .bind(order)
        .execute(store.pool())
        .await?;

    let payment: serde_json::Value = sqlx::query_scalar("SELECT data FROM payments WHERE id=$1")
        .bind(receipt.payment_id)
        .fetch_one(store.pool())
        .await?;
    sqlx::query("UPDATE payments SET data=replace(data::text,$2,$3)::jsonb WHERE id=$1")
        .bind(receipt.payment_id)
        .bind(receipt.payment_id.to_string())
        .bind(replacement.to_string())
        .execute(store.pool())
        .await?;
    assert!(matches!(
        store.get_payment(receipt.payment_id).await,
        Err(AppError::CorruptData)
    ));
    sqlx::query("UPDATE payments SET data=$2 WHERE id=$1")
        .bind(receipt.payment_id)
        .bind(payment)
        .execute(store.pool())
        .await?;

    let checkout: serde_json::Value = sqlx::query_scalar("SELECT data FROM checkouts WHERE id=$1")
        .bind(receipt.checkout_id)
        .fetch_one(store.pool())
        .await?;
    sqlx::query(
        "UPDATE checkouts SET data=jsonb_set(data,'{links,id}',to_jsonb($2::uuid)) WHERE id=$1",
    )
    .bind(receipt.checkout_id)
    .bind(replacement)
    .execute(store.pool())
    .await?;
    assert!(matches!(
        store.get_checkout(receipt.checkout_id).await,
        Err(AppError::CorruptData)
    ));
    sqlx::query("UPDATE checkouts SET data=$2 WHERE id=$1")
        .bind(receipt.checkout_id)
        .bind(checkout)
        .execute(store.pool())
        .await?;

    sqlx::query("UPDATE payment_operations SET request=jsonb_set(request,'{payment_method_token}',to_jsonb('pm_demo_decline'::text)) WHERE payment_id=$1").bind(receipt.payment_id).execute(store.pool()).await?;
    let request = event(
        store.pool(),
        receipt.checkout_id,
        "payment.authorization_requested",
    )
    .await?;
    assert!(matches!(
        store.apply_event(&request).await,
        Err(AppError::CorruptData)
    ));
    assert!(
        gateway.calls.lock().unwrap().is_empty(),
        "corrupted operations must never reach the provider"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn checkout_rejects_changed_or_inactive_catalog_quotes_until_cart_is_refreshed() -> TestResult
{
    let db = Database::new().await?;
    let mut request = command(&db.store).await?;
    sqlx::query("UPDATE catalog SET unit_price_minor=3000 WHERE sku='BOOK'")
        .execute(db.store.pool())
        .await?;
    assert!(matches!(
        db.store
            .checkout(request.clone(), TraceContext::new())
            .await,
        Err(AppError::InvalidState(_))
    ));
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM checkouts").await?,
        0
    );
    assert_eq!(
        db.store.get_cart(request.cart_id).await?.status.as_str(),
        "open"
    );

    let refreshed = db
        .store
        .add_item(
            request.cart_id,
            AddCartItem {
                sku: "BOOK".into(),
                quantity: 2,
                expected_version: request.expected_cart_version,
            },
        )
        .await?;
    request.expected_cart_version = refreshed.version;
    sqlx::query("UPDATE catalog SET active=false WHERE sku='COFFEE'")
        .execute(db.store.pool())
        .await?;
    assert!(matches!(
        db.store
            .checkout(request.clone(), TraceContext::new())
            .await,
        Err(AppError::InvalidState(_))
    ));
    assert_eq!(
        count(db.store.pool(), "SELECT count(*) FROM checkout_requests").await?,
        0
    );
    sqlx::query("UPDATE catalog SET active=true WHERE sku='COFFEE'")
        .execute(db.store.pool())
        .await?;
    let refreshed = db
        .store
        .add_item(
            request.cart_id,
            AddCartItem {
                sku: "COFFEE".into(),
                quantity: 1,
                expected_version: refreshed.version,
            },
        )
        .await?;
    request.expected_cart_version = refreshed.version;
    let receipt = db
        .store
        .checkout(request.clone(), TraceContext::new())
        .await?;
    assert_eq!(receipt.total_minor, 6700);
    // A completed checkout still replays its immutable receipt after catalog changes.
    sqlx::query("UPDATE catalog SET active=false")
        .execute(db.store.pool())
        .await?;
    let replay = db.store.checkout(request, TraceContext::new()).await?;
    assert_eq!(replay.checkout_id, receipt.checkout_id);
    assert_eq!(replay.total_minor, receipt.total_minor);
    Ok(())
}
