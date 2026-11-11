use super::super::{GrpcCommerce, errors::status, mapping, middleware::server_span};
use contracts::commerce::v1::{self, checkout_service_server::CheckoutService};
use tonic::{Request, Response, Status};
use tracing::Instrument;
#[tonic::async_trait]
impl CheckoutService for GrpcCommerce {
    async fn checkout(
        &self,
        request: Request<v1::CheckoutRequest>,
    ) -> Result<Response<v1::CheckoutResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CheckoutService", "Checkout");
        async {
            let command = mapping::checkout(request.into_inner())?;
            let receipt = self
                .store
                .checkout(command, platform::capture_trace_context())
                .await
                .map_err(status)?;
            tracing::info!(
                event.name = "checkout.accepted",
                checkout_id = %receipt.checkout_id,
                order_id = %receipt.order_id,
                "checkout accepted"
            );
            opentelemetry::global::meter("commerce")
                .u64_counter("checkout.accepted")
                .build()
                .add(1, &[]);
            Ok(Response::new(mapping::receipt(receipt)))
        }
        .instrument(span)
        .await
    }
    async fn get_checkout(
        &self,
        request: Request<v1::GetCheckoutRequest>,
    ) -> Result<Response<v1::CheckoutViewResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CheckoutService", "GetCheckout");
        async {
            let id = mapping::id(&request.into_inner().checkout_id)?;
            Ok(Response::new(mapping::checkout_view(
                self.store.get_checkout(id).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
    async fn cancel_checkout(
        &self,
        request: Request<v1::GetCheckoutRequest>,
    ) -> Result<Response<v1::CheckoutViewResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CheckoutService", "CancelCheckout");
        async {
            let id = mapping::id(&request.into_inner().checkout_id)?;
            Ok(Response::new(mapping::checkout_view(
                self.store
                    .cancel_checkout_with_context(id, platform::capture_trace_context())
                    .await
                    .map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
}
