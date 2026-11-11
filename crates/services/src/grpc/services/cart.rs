use super::super::{GrpcCommerce, errors::status, mapping, middleware::server_span};
use contracts::commerce::v1::{self, cart_service_server::CartService};
use tonic::{Request, Response, Status};
use tracing::Instrument;
#[tonic::async_trait]
impl CartService for GrpcCommerce {
    async fn create_cart(
        &self,
        request: Request<v1::CreateCartRequest>,
    ) -> Result<Response<v1::CartResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CartService", "CreateCart");
        async {
            let command = mapping::create_cart(request.into_inner())?;
            Ok(Response::new(mapping::cart(
                self.store.create_cart(command).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
    async fn add_item(
        &self,
        request: Request<v1::AddCartItemRequest>,
    ) -> Result<Response<v1::CartResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CartService", "AddItem");
        async {
            let (id, command) = mapping::add_item(request.into_inner())?;
            Ok(Response::new(mapping::cart(
                self.store.add_item(id, command).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
    async fn remove_item(
        &self,
        request: Request<v1::RemoveCartItemRequest>,
    ) -> Result<Response<v1::CartResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CartService", "RemoveItem");
        async {
            let (id, command) = mapping::remove_item(request.into_inner())?;
            Ok(Response::new(mapping::cart(
                self.store.remove_item(id, command).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
    async fn get_cart(
        &self,
        request: Request<v1::GetCartRequest>,
    ) -> Result<Response<v1::CartResponse>, Status> {
        let span = server_span(&request, "commerce.v1.CartService", "GetCart");
        async {
            let id = mapping::id(&request.into_inner().cart_id)?;
            Ok(Response::new(mapping::cart(
                self.store.get_cart(id).await.map_err(status)?,
            )))
        }
        .instrument(span)
        .await
    }
}
