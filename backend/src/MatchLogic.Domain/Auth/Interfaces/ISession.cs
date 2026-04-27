using System;
using MatchLogic.Domain.Entities.Common;

namespace MatchLogic.Domain.Auth.Interfaces;

/// <summary>
/// Obsolete: Use ICurrentUser instead. Kept for backward compatibility.
/// </summary>
[Obsolete("Use ICurrentUser instead.", error: false)]
public interface ISession : ICurrentUser
{
    /// <summary>Strongly-typed UserId for legacy consumers.</summary>
    new UserId UserId { get; }
}
