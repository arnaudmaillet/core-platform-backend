use tonic::{Request, Response, Status};

use super::handler::{SearchServiceHandler, proto};
use proto::search_service_server::SearchService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `search-api`'s `build.rs`.
pub const FILE_DESCRIPTOR_SET: &[u8] = search_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl SearchService for SearchServiceHandler {
    async fn search(
        &self,
        request: Request<proto::SearchRequest>,
    ) -> Result<Response<proto::SearchResponse>, Status> {
        self.search(request).await
    }

    async fn suggest(
        &self,
        request: Request<proto::SuggestRequest>,
    ) -> Result<Response<proto::SuggestResponse>, Status> {
        self.suggest(request).await
    }

    async fn multi_search(
        &self,
        request: Request<proto::MultiSearchRequest>,
    ) -> Result<Response<proto::MultiSearchResponse>, Status> {
        self.multi_search(request).await
    }

    async fn record_recent_search(
        &self,
        request: Request<proto::RecordRecentSearchRequest>,
    ) -> Result<Response<proto::RecentSearchesResponse>, Status> {
        self.record_recent_search(request).await
    }

    async fn list_recent_searches(
        &self,
        request: Request<proto::ListRecentSearchesRequest>,
    ) -> Result<Response<proto::RecentSearchesResponse>, Status> {
        self.list_recent_searches(request).await
    }

    async fn delete_recent_search(
        &self,
        request: Request<proto::DeleteRecentSearchRequest>,
    ) -> Result<Response<proto::RecentSearchesResponse>, Status> {
        self.delete_recent_search(request).await
    }

    async fn clear_search_history(
        &self,
        request: Request<proto::ClearSearchHistoryRequest>,
    ) -> Result<Response<proto::RecentSearchesResponse>, Status> {
        self.clear_search_history(request).await
    }
}
