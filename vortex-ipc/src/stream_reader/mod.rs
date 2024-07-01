use std::ops::Deref;
use std::sync::Arc;

use futures_util::stream::try_unfold;
use futures_util::Stream;
use vortex::stream::ArrayStream;
use vortex::{Context, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::io::VortexRead;
use crate::MessageReader;

pub struct StreamArrayReader<R: VortexRead> {
    msgs: MessageReader<R>,
    view_context: Option<Arc<ViewContext>>,
    dtype: Option<Arc<DType>>,
}

impl<R: VortexRead> StreamArrayReader<R> {
    pub async fn try_new(read: R) -> VortexResult<Self> {
        Ok(Self {
            msgs: MessageReader::try_new(read).await?,
            view_context: None,
            dtype: None,
        })
    }

    /// Manually configure the view context.
    pub fn with_view_context(self, view_context: ViewContext) -> Self {
        assert!(self.view_context.is_none(), "View context already set");
        Self {
            view_context: Some(Arc::new(view_context)),
            ..self
        }
    }

    /// Load the view context from the stream.
    pub async fn load_view_context(mut self, ctx: &Context) -> VortexResult<Self> {
        assert!(self.view_context.is_none(), "View context already set");
        self.view_context = Some(self.msgs.read_view_context(ctx).await?);
        Ok(self)
    }

    /// Retrieve the loaded view_context
    pub fn view_context(&self) -> Option<Arc<ViewContext>> {
        self.view_context.clone()
    }

    pub fn with_dtype(self, dtype: DType) -> Self {
        assert!(self.dtype.is_none(), "DType already set");
        Self {
            dtype: Some(Arc::new(dtype)),
            ..self
        }
    }

    pub async fn load_dtype(mut self) -> VortexResult<Self> {
        assert!(self.dtype.is_none(), "DType already set");
        self.dtype = Some(Arc::new(self.msgs.read_dtype().await?));
        Ok(self)
    }

    /// Reads a single array from the stream.
    pub fn array_stream(&mut self) -> impl ArrayStream + '_ {
        let view_context = self
            .view_context
            .as_ref()
            .expect("View context not set")
            .clone();
        let dtype = self.dtype.as_ref().expect("DType not set").deref().clone();
        self.msgs.array_stream(view_context, dtype)
    }

    pub fn into_array_stream(self) -> impl ArrayStream {
        let view_context = self
            .view_context
            .as_ref()
            .expect("View context not set")
            .clone();
        let dtype = self.dtype.as_ref().expect("DType not set").deref().clone();
        self.msgs.into_array_stream(view_context, dtype)
    }

    /// Reads a single page from the stream.
    pub async fn next_page(&mut self) -> VortexResult<Option<Buffer>> {
        self.msgs.maybe_read_page().await
    }

    /// Reads consecutive pages from the stream until the message type changes.
    pub async fn page_stream(&mut self) -> impl Stream<Item = VortexResult<Buffer>> + '_ {
        try_unfold(self, |reader| async {
            match reader.next_page().await? {
                Some(page) => Ok(Some((page, reader))),
                None => Ok(None),
            }
        })
    }
}
