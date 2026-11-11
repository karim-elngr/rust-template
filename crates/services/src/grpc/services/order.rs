use super::super::{GrpcCommerce, errors::status, mapping, middleware::server_span};
use contracts::commerce::v1::{self, order_service_server::OrderService};
use tonic::{Request, Response, Status};
use tracing::Instrument;
#[tonic::async_trait]
impl OrderService for GrpcCommerce {
    async fn get_order(
        &self,
        request: Request<v1::GetOrderRequest>,
    ) -> Result<Response<v1::OrderResponse>, Status> {
        let span = server_span(&request, "commerce.v1.OrderService", "GetOrder");
        async {
            let id = mapping::id(&request.into_inner().order_id)?;
            Ok(Response::new(mapping::order(
                self.store.get_order(id).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
    async fn get_order_summary(
        &self,
        request: Request<v1::GetOrderRequest>,
    ) -> Result<Response<v1::OrderSummaryResponse>, Status> {
        let span = server_span(&request, "commerce.v1.OrderService", "GetOrderSummary");
        async {
            let id = mapping::id(&request.into_inner().order_id)?;
            Ok(Response::new(mapping::order_summary(
                self.store.get_order_summary(id).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
}
