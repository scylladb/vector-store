#[cfg(not(feature = "opensearch"))]
pub mod usearch;
#[cfg(feature = "opensearch")]
pub mod opensearch;

#[cfg(not(feature = "opensearch"))]
pub use usearch::{Index, IndexExt, new};
#[cfg(feature = "opensearch")]
pub use opensearch::{Index, IndexExt, new};