//! Command handlers enforce optimistic cart writes and transactional checkout creation.
use super::saga::{cancel_locked, emit, emit_summary, enqueue_operation};
use crate::{
    infrastructure::{UnitOfWork, carts, checkouts, orders, payments},
    *,
};
use ::orders::{Order, OrderLine};
use ::payments::Payment;
use cart::Cart;
use checkout::{CheckoutLinks, CheckoutSaga};
use shared_kernel::Money;
use uuid::Uuid;
impl Store {
    pub async fn create_cart(&self, command: CreateCart) -> Result<CartView, AppError> {
        let cart = Cart::new(Uuid::new_v4(), command.customer_id);
        cart.validate()?;
        let mut w = UnitOfWork::begin(&self.pool).await?;
        carts::insert(w.conn(), &cart).await?;
        w.commit().await?;
        Ok(CartView::try_from(&cart)?)
    }
    pub async fn add_item(&self, id: Uuid, command: AddCartItem) -> Result<CartView, AppError> {
        let mut w = UnitOfWork::begin(&self.pool).await?;
        let mut cart = carts::load(w.conn(), id, true).await?;
        if cart.version() != command.expected_version {
            return Err(AppError::VersionConflict);
        }
        let price = carts::price(w.conn(), &command.sku).await?;
        cart.add(
            command.sku,
            command.quantity,
            price,
            command.expected_version,
        )?;
        carts::save(w.conn(), &cart).await?;
        w.commit().await?;
        Ok(CartView::try_from(&cart)?)
    }
    pub async fn remove_item(
        &self,
        id: Uuid,
        command: RemoveCartItem,
    ) -> Result<CartView, AppError> {
        let mut w = UnitOfWork::begin(&self.pool).await?;
        let mut cart = carts::load(w.conn(), id, true).await?;
        if cart.version() != command.expected_version {
            return Err(AppError::VersionConflict);
        }
        cart.remove(&command.sku, command.expected_version)?;
        carts::save(w.conn(), &cart).await?;
        w.commit().await?;
        Ok(CartView::try_from(&cart)?)
    }
    #[tracing::instrument(name = "checkout.create", skip_all)]
    pub async fn checkout(
        &self,
        command: CheckoutCommand,
        trace: TraceContext,
    ) -> Result<CheckoutReceipt, AppError> {
        if command.idempotency_key.is_nil() || command.cart_id.is_nil() {
            return Err(AppError::Validation("checkout IDs must not be nil".into()));
        }
        let canonical = serde_json::to_value(&command)?;
        let mut w = UnitOfWork::begin(&self.pool).await?;
        checkouts::lock_key(w.conn(), command.idempotency_key).await?;
        if let Some((old, receipt)) = checkouts::receipt(w.conn(), command.idempotency_key).await? {
            if old != canonical {
                return Err(AppError::IdempotencyConflict);
            }
            w.commit().await?;
            return Ok(receipt);
        }
        let mut cart = carts::load(w.conn(), command.cart_id, true).await?;
        if cart.version() != command.expected_cart_version {
            return Err(AppError::VersionConflict);
        }
        carts::validate_quote(w.conn(), &cart).await?;
        cart.checkout(command.expected_cart_version)?;
        let links = CheckoutLinks {
            id: Uuid::new_v4(),
            cart_id: cart.id(),
            order_id: Uuid::new_v4(),
            payment_id: Uuid::new_v4(),
        };
        let order = Order::new(
            links.order_id,
            links.id,
            cart.id(),
            cart.customer_id(),
            links.payment_id,
            cart.items()
                .iter()
                .map(|i| OrderLine {
                    sku: i.sku.clone(),
                    quantity: i.quantity,
                    unit_price_minor: i.unit_price_minor,
                })
                .collect(),
        )?;
        let mut payment = Payment::new(
            links.payment_id,
            order.id(),
            links.id,
            Money::new(order.total_minor(), "USD")?,
            command.payment_method_token,
        )?;
        payment.begin_authorization()?;
        let saga = CheckoutSaga::new(links, w.now().await?);
        let receipt = CheckoutReceipt {
            checkout_id: saga.links().id,
            cart_id: cart.id(),
            order_id: order.id(),
            payment_id: payment.id(),
            status: saga.status(),
            total_minor: order.total_minor(),
            currency: "USD".into(),
        };
        carts::save(w.conn(), &cart).await?;
        orders::insert(w.conn(), &order).await?;
        payments::insert(w.conn(), &payment).await?;
        checkouts::insert(w.conn(), &saga).await?;
        checkouts::remember(w.conn(), command.idempotency_key, canonical, &receipt).await?;
        enqueue_operation(&mut w, &payment, &trace).await?;
        emit(
            &mut w,
            &saga,
            "checkout.started",
            saga.links().id,
            serde_json::json!({}),
            &trace,
            &format!("checkout:{}:started", saga.links().id),
        )
        .await?;
        emit_summary(&mut w, &saga, &order, &payment, &trace).await?;
        w.commit().await?;
        Ok(receipt)
    }
    pub async fn cancel_checkout(&self, id: Uuid) -> Result<CheckoutView, AppError> {
        self.cancel_checkout_with_context(id, TraceContext::new())
            .await
    }
    pub async fn cancel_checkout_with_context(
        &self,
        id: Uuid,
        trace: TraceContext,
    ) -> Result<CheckoutView, AppError> {
        let mut w = UnitOfWork::begin(&self.pool).await?;
        let mut saga = checkouts::load(w.conn(), id, true).await?;
        let mut order = orders::load(w.conn(), saga.links().order_id).await?;
        let mut payment = payments::load(w.conn(), saga.links().payment_id).await?;
        cancel_locked(&mut w, &mut saga, &mut order, &mut payment, &trace).await?;
        let view = CheckoutView::new(&saga, &payment);
        w.commit().await?;
        Ok(view)
    }
    pub async fn expire_pending(&self, batch: i64) -> Result<u64, AppError> {
        if !(1..=1000).contains(&batch) {
            return Err(AppError::Validation("batch size must be 1..1000".into()));
        }
        let mut w = UnitOfWork::begin(&self.pool).await?;
        let ids = checkouts::expired(w.conn(), batch).await?;
        for id in &ids {
            let mut saga = checkouts::load(w.conn(), *id, true).await?;
            if !saga.is_pending() || saga.expires_at() > w.now().await? {
                continue;
            }
            let mut order = orders::load(w.conn(), saga.links().order_id).await?;
            let mut payment = payments::load(w.conn(), saga.links().payment_id).await?;
            cancel_locked(
                &mut w,
                &mut saga,
                &mut order,
                &mut payment,
                &TraceContext::new(),
            )
            .await?;
        }
        w.commit().await?;
        Ok(ids.len() as u64)
    }
}
