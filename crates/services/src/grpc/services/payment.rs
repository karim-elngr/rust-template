use super::super::{GrpcCommerce, errors::status, mapping, middleware::server_span};
use contracts::commerce::v1::{self, payment_service_server::PaymentService};
use tonic::{Request, Response, Status};
use tracing::Instrument;
#[tonic::async_trait]
impl PaymentService for GrpcCommerce {
    async fn get_payment(
        &self,
        request: Request<v1::GetPaymentRequest>,
    ) -> Result<Response<v1::PaymentResponse>, Status> {
        let span = server_span(&request, "commerce.v1.PaymentService", "GetPayment");
        async {
            let id = mapping::id(&request.into_inner().payment_id)?;
            Ok(Response::new(mapping::payment(
                self.store.get_payment(id).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
}
