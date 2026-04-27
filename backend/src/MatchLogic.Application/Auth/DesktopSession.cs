using System;
using System.Collections.Generic;
using MatchLogic.Domain.Auth.Interfaces;
using MatchLogic.Domain.Entities.Common;
using DomainISession = MatchLogic.Domain.Auth.Interfaces.ISession;

namespace MatchLogic.Application.Auth;

/// <summary>
/// Fixed admin identity for desktop mode — no HTTP claims, no Keycloak.
/// Registered as singleton (the identity never changes).
/// </summary>
#pragma warning disable CS0618 // ISession is obsolete
public class DesktopSession : ICurrentUser, DomainISession
#pragma warning restore CS0618
{
    private static readonly Guid AdminId = new("00000000-0000-0000-0000-000000000001");

    // ICurrentUser
    public Guid UserId => AdminId;
    public string? Email => "admin@localhost";
    public string? UserName => "DesktopAdmin";
    public IReadOnlyList<string> Roles { get; } = new[] { "admin" };
    public bool IsAuthenticated => true;
    public DateTime UtcNow => DateTime.UtcNow;

    // ISession (legacy — UserId as strongly-typed struct)
    #pragma warning disable CS0618
    UserId DomainISession.UserId => AdminId;
    #pragma warning restore CS0618
}
