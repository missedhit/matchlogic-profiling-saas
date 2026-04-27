using System;

namespace MatchLogic.Infrastructure.Configuration
{
    /// <summary>
    /// Operation mode for the application
    /// </summary>
    public enum OperationMode
    {
        /// <summary>
        /// Batch processing mode - full record linkage pipeline
        /// </summary>
        Batch,

        /// <summary>
        /// Live Search mode - real-time record matching against indexed corpus
        /// </summary>
        LiveSearch
    }

    /// <summary>
    /// Configuration for operation mode and feature toggles
    /// </summary>
    public class ApplicationOperationConfig
    {
        /// <summary>
        /// Current operation mode
        /// </summary>
        public OperationMode Mode { get; set; } = OperationMode.Batch;

        /// <summary>
        /// Whether this is a Live Search indexing node (builds and persists index)
        /// Only applicable when Mode = LiveSearch
        /// </summary>
        public bool IsIndexingNode { get; set; } = false;

        /// <summary>
        /// Project ID for Live Search operations
        /// Only applicable when Mode = LiveSearch
        /// </summary>
        public Guid? ProjectId { get; set; }
    }

    
}