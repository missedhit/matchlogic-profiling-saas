using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace MatchLogic.Api.Middleware;

/// <summary>
/// Desktop mode: sets HttpContext.User to a fixed admin principal so that
/// authorization policies (which read claims from context.User) pass
/// without requiring a JWT token.
/// </summary>
public class DesktopAuthMiddleware
{
    private readonly RequestDelegate _next;

    private static readonly ClaimsPrincipal DesktopAdmin = new(new ClaimsIdentity(
        new[]
        {
            new Claim(ClaimTypes.NameIdentifier, "00000000-0000-0000-0000-000000000001"),
            new Claim(ClaimTypes.Name, "DesktopAdmin"),
            new Claim(ClaimTypes.Email, "admin@localhost"),
            new Claim(ClaimTypes.Role, "admin")
        },
        authenticationType: "Desktop"   // non-null → IsAuthenticated = true
    ));

    public DesktopAuthMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public Task Invoke(HttpContext context)
    {
        context.User = DesktopAdmin;
        return _next(context);
    }
}
