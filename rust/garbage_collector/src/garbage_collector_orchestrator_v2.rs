use crate::construct_version_graph_orchestrator::{
    ConstructVersionGraphError, ConstructVersionGraphOrchestrator, ConstructVersionGraphResponse,
};
use crate::operators::compute_versions_to_delete_from_graph::{
    CollectionVersionAction, ComputeVersionsToDeleteError, ComputeVersionsToDeleteInput,
    ComputeVersionsToDeleteOperator, ComputeVersionsToDeleteOutput,
};
use crate::operators::delete_unused_files::{
    DeleteUnusedFilesError, DeleteUnusedFilesInput, DeleteUnusedFilesOperator,
    DeleteUnusedFilesOutput,
};
use crate::operators::list_files_at_version::{
    ListFilesAtVersionError, ListFilesAtVersionInput, ListFilesAtVersionOutput,
    ListFilesAtVersionsOperator,
};
use crate::types::CleanupMode;
use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, System, TaskError, TaskMessage, TaskResult,
};
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::CollectionUuid;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

// todo: cleanup
pub struct GarbageCollectorOrchestrator {
    collection_id: CollectionUuid,
    version_file_path: String,
    absolute_cutoff_time: DateTime<Utc>,
    sysdb_client: SysDb,
    dispatcher: ComponentHandle<Dispatcher>,
    system: System,
    storage: Storage,
    root_manager: RootManager,
    result_channel: Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>>,
    pending_version_file: Option<CollectionVersionFile>,
    pending_versions_to_delete: Option<chroma_types::chroma_proto::VersionListForCollection>,
    pending_epoch_id: Option<i64>,
    num_versions_deleted: u32,
    deletion_list: Vec<String>,
    cleanup_mode: CleanupMode,
    version_files: HashMap<CollectionUuid, CollectionVersionFile>,
    versions_to_delete_output: Option<ComputeVersionsToDeleteOutput>,
    file_ref_counts: HashMap<String, u32>,
}

impl Debug for GarbageCollectorOrchestrator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct GarbageCollectorResponse {
    pub collection_id: CollectionUuid,
    pub version_file_path: String,
    pub num_versions_deleted: u32,
    pub deletion_list: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
impl GarbageCollectorOrchestrator {
    pub fn new(
        collection_id: CollectionUuid,
        version_file_path: String,
        absolute_cutoff_time: DateTime<Utc>,
        sysdb_client: SysDb,
        dispatcher: ComponentHandle<Dispatcher>,
        system: System,
        storage: Storage,
        root_manager: RootManager,
        cleanup_mode: CleanupMode,
    ) -> Self {
        Self {
            collection_id,
            version_file_path,
            absolute_cutoff_time,
            sysdb_client,
            dispatcher,
            system,
            storage,
            root_manager,
            cleanup_mode,
            result_channel: None,
            pending_version_file: None,
            pending_versions_to_delete: None,
            pending_epoch_id: None,
            num_versions_deleted: 0,
            deletion_list: Vec::new(),
            version_files: HashMap::new(),
            file_ref_counts: HashMap::new(),
            versions_to_delete_output: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum GarbageCollectorError {
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("{0}")]
    Generic(#[from] Box<dyn ChromaError>),
    #[error("The task was aborted because resources were exhausted")]
    Aborted,

    #[error("Failed to construct version graph: {0}")]
    ConstructVersionGraph(#[from] ConstructVersionGraphError),
    #[error("Failed to compute versions to delete: {0}")]
    ComputeVersionsToDelete(#[from] ComputeVersionsToDeleteError),
    #[error("Failed to list files at version: {0}")]
    ListFilesAtVersion(#[from] ListFilesAtVersionError),
    #[error("Failed to delete unused files: {0}")]
    DeleteUnusedFiles(#[from] DeleteUnusedFilesError),
}

impl ChromaError for GarbageCollectorError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl<E> From<TaskError<E>> for GarbageCollectorError
where
    E: Into<GarbageCollectorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => GarbageCollectorError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => GarbageCollectorError::Aborted,
        }
    }
}

#[async_trait]
impl Orchestrator for GarbageCollectorOrchestrator {
    type Output = GarbageCollectorResponse;
    type Error = GarbageCollectorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn initial_tasks(&mut self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        tracing::info!(
            path = %self.version_file_path,
            "Creating initial fetch version file task"
        );

        let orchestrator = ConstructVersionGraphOrchestrator::new(
            self.dispatcher(),
            self.storage.clone(),
            self.sysdb_client.clone(),
            self.collection_id,
            self.version_file_path.clone(),
            None, // todo
        );

        vec![wrap(
            orchestrator.to_operator(self.system.clone()),
            (),
            ctx.receiver(),
        )]
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>,
    ) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(
        &mut self,
    ) -> Sender<Result<GarbageCollectorResponse, GarbageCollectorError>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<ConstructVersionGraphResponse, ConstructVersionGraphError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ConstructVersionGraphResponse, ConstructVersionGraphError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => {
                return;
            }
        };

        let task = wrap(
            Box::new(ComputeVersionsToDeleteOperator {}),
            ComputeVersionsToDeleteInput {
                graph: output.graph,
                cutoff_time: self.absolute_cutoff_time,
                min_versions_to_keep: 2,
            },
            ctx.receiver(),
        );

        if let Err(e) = self.dispatcher().send(task, Some(Span::current())).await {
            self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                .await;
            return;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<ComputeVersionsToDeleteOutput, ComputeVersionsToDeleteError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ComputeVersionsToDeleteOutput, ComputeVersionsToDeleteError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        for (collection_id, versions) in &output.versions {
            let version_file = self
                .version_files
                .get(collection_id)
                .expect("Version file should be present"); // todo

            for version in versions.keys() {
                let task = wrap(
                    Box::new(ListFilesAtVersionsOperator {}),
                    ListFilesAtVersionInput::new(
                        self.root_manager.clone(),
                        version_file.clone(),
                        *version,
                    ),
                    ctx.receiver(),
                );
                if let Err(e) = self.dispatcher().send(task, Some(Span::current())).await {
                    self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                        .await;
                    return;
                }
            }
        }

        self.versions_to_delete_output = Some(output);
    }
}

#[async_trait]
impl Handler<TaskResult<ListFilesAtVersionOutput, ListFilesAtVersionError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ListFilesAtVersionOutput, ListFilesAtVersionError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // todo: no panics
        let version_action = self
            .versions_to_delete_output
            .as_ref()
            .unwrap()
            .versions
            .get(&output.collection_id)
            .unwrap()
            .get(&output.version)
            .unwrap();

        match version_action {
            CollectionVersionAction::Keep => {
                tracing::debug!(
                    "Marking {} files as used for collection {} at version {}",
                    output.file_paths.len(),
                    output.collection_id,
                    output.version
                );

                for file_path in output.file_paths {
                    let count = self.file_ref_counts.entry(file_path).or_insert(0);
                    *count += 1;
                }
            }
            CollectionVersionAction::Delete => {
                tracing::debug!(
                    "Marking {} files as unused for collection {} at version {}",
                    output.file_paths.len(),
                    output.collection_id,
                    output.version
                );

                for file_path in output.file_paths {
                    let count = self.file_ref_counts.entry(file_path).or_insert(0);
                    // todo
                    let _ = count.saturating_sub(1);
                }
            }
        }

        // todo: no panics
        let v = self
            .versions_to_delete_output
            .as_mut()
            .unwrap()
            .versions
            .get_mut(&output.collection_id)
            .unwrap();
        v.remove(&output.version);
        if v.is_empty() {
            self.versions_to_delete_output
                .as_mut()
                .unwrap()
                .versions
                .remove(&output.collection_id);
        }

        if self
            .versions_to_delete_output
            .as_ref()
            .unwrap()
            .versions
            .is_empty()
        {
            let file_paths_to_delete = self
                .file_ref_counts
                .iter()
                .filter_map(|(path, count)| {
                    if *count == 0 {
                        Some(path.clone())
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>(); // todo: doesn't need to be a set
            let delete_percentage =
                file_paths_to_delete.len() as f32 / self.file_ref_counts.len() as f32 * 100.0;

            tracing::info!(
                delete_percentage = delete_percentage,
                "Deleting {} files out of a total of {}",
                file_paths_to_delete.len(),
                self.file_ref_counts.len()
            );

            let task = wrap(
                Box::new(DeleteUnusedFilesOperator::new(
                    self.storage.clone(),
                    self.cleanup_mode,
                    "test".to_string(), // todo: remove collection ID
                )),
                DeleteUnusedFilesInput {
                    unused_s3_files: file_paths_to_delete,
                    hnsw_prefixes_for_deletion: vec![],
                    epoch_id: 0, // todo
                },
                ctx.receiver(),
            );
            if let Err(e) = self.dispatcher().send(task, Some(Span::current())).await {
                self.terminate_with_result(Err(GarbageCollectorError::Channel(e)), ctx)
                    .await;
                return;
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<DeleteUnusedFilesOutput, DeleteUnusedFilesError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<DeleteUnusedFilesOutput, DeleteUnusedFilesError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
    }
}
