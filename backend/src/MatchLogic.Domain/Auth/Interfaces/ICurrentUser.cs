using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Auth.Interfaces;

public interface ICurrentUser
{
    Guid UserId { get; }
    string? Email { get; }
    string? UserName { get; }
    IReadOnlyList<string> Roles { get; }
    bool IsAuthenticated { get; }
    DateTime UtcNow { get; }
}
