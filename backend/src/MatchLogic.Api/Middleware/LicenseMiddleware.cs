using System;
using System.Linq;
using System.Threading.Tasks;
using Ardalis.Result;
using MatchLogic.Application.Licensing;
using Microsoft.AspNetCore.Http;

namespace MatchLogic.Api.Middleware;

/// <summary>
/// Enforces license restrictions on API requests.
/// - NotActivated / Tampered → blocks POST/PUT/PATCH operations (full app block).
/// - Trial expired / record limit / grace expired → blocks import routes only.
/// - GET and DELETE are never blocked — read access and quota reclaim always allowed.
/// - License, health, version, and identity endpoints are always allowed for activation flow.
/// Returns HTTP 402 with Ardalis.Result envelope for consistency with all other API errors.
/// </summary>
public class LicenseMiddleware
{
    private readonly RequestDelegate _next;

    // These routes always work regardless of license state.
    // License endpoint must always be accessible so activation is possible.
    private static readonly string[] AlwaysAllowedPrefixes =
    [
        "/api/license",
        "/api/healthcheck",
        "/api/version",
        "/api/identity",
    ];

    // Import routes — blocked when CanImport = false
    private static readonly string[] ImportPrefixes =
    [
        "/api/dataimport/datasource",
        "/api/dataimport/file",
        "/api/dataimport/datasource",
    ];

    public LicenseMiddleware(RequestDelegate next) => _next = next;

    public async Task InvokeAsync(HttpContext context, ILicenseService licenseService)
    {
        var path   = context.Request.Path.Value?.ToLowerInvariant() ?? "";
        var method = context.Request.Method;

        // GET requests are always allowed — read-only access never blocked
        // DELETE is always allowed — users must be able to delete datasources/projects
        // to reclaim trial quota, even when license is expired or limit reached
        if (HttpMethods.IsGet(method) || HttpMethods.IsDelete(method))
        {
            await _next(context);
            return;
        }

        // License activation and health endpoints always work
        if (AlwaysAllowedPrefixes.Any(p =>
                path.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
        {
            await _next(context);
            return;
        }

        var info = await licenseService.GetLicenseInfoAsync();

        // FULL BLOCK — Tampered or NotActivated
        if (info.IsFullyBlocked)
        {
            await WriteLicenseError(context, info.StatusMessage);
            return;
        }

        // IMPORT BLOCK — trial expired, record limit, grace expired
        if (!info.CanImport &&
            ImportPrefixes.Any(p =>
                path.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
        {
            await WriteLicenseError(context, info.StatusMessage);
            return;
        }

        await _next(context);
    }

    private static async Task WriteLicenseError(HttpContext context, string message)
    {
        context.Response.StatusCode  = StatusCodes.Status402PaymentRequired;
        context.Response.ContentType = "application/json";
        var result = Result.Error(message);
        await context.Response.WriteAsJsonAsync(result);
    }
}
