using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using MatchLogic.Domain.Auth.Interfaces;
using MatchLogic.Domain.Entities.Common;
using Microsoft.AspNetCore.Http;
using DomainISession = MatchLogic.Domain.Auth.Interfaces.ISession;

namespace MatchLogic.Application.Auth;

#pragma warning disable CS0618 // ISession is obsolete
public class CurrentUser : ICurrentUser, DomainISession
#pragma warning restore CS0618
{
    private readonly Guid _userId;
    private readonly string? _email;
    private readonly string? _userName;
    private readonly IReadOnlyList<string> _roles;
    private readonly bool _isAuthenticated;

    // ICurrentUser
    public Guid UserId => _userId;
    public string? Email => _email;
    public string? UserName => _userName;
    public IReadOnlyList<string> Roles => _roles;
    public bool IsAuthenticated => _isAuthenticated;
    public DateTime UtcNow => DateTime.UtcNow;

    // ISession (legacy — UserId as strongly-typed struct)
    #pragma warning disable CS0618
    UserId DomainISession.UserId => _userId;
    #pragma warning restore CS0618

    public CurrentUser(IHttpContextAccessor httpContextAccessor)
    {
        var user = httpContextAccessor.HttpContext?.User;

        _isAuthenticated = user?.Identity?.IsAuthenticated ?? false;

        var sub = user?.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        _userId = Guid.TryParse(sub, out var parsed) ? parsed : Guid.Empty;

        _email    = user?.FindFirst(ClaimTypes.Email)?.Value;
        _userName = user?.FindFirst(ClaimTypes.Name)?.Value;

        _roles = user?.FindAll(ClaimTypes.Role)
                      .Select(c => c.Value)
                      .ToList()
                 ?? new List<string>();
    }
}
