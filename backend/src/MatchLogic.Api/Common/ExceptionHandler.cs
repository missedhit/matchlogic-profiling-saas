using Ardalis.Result;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Mime;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Common;

internal sealed class ExceptionHandler : IExceptionHandler
{
    private readonly ILogger<ExceptionHandler> _logger;

    public ExceptionHandler(ILogger<ExceptionHandler> logger)
    {
        _logger = logger;
    }

    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        var ex = exception.Demystify();
        _logger.LogError(ex, "An error occurred: {Message}", ex.Message);
        httpContext.Response.ContentType = MediaTypeNames.Application.Json;
        httpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
        var result = Result.Error(exception.ToStringDemystified());
        await httpContext.Response.WriteAsJsonAsync(result, cancellationToken);
        return true;
    }
}

public class ExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ExceptionHandlingMiddleware> _logger;

    public ExceptionHandlingMiddleware(
        RequestDelegate next,
        ILogger<ExceptionHandlingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception, "Exception occurred: {Message}", exception.Message);

            var problemDetails = new ProblemDetails
            {
                Status = StatusCodes.Status500InternalServerError,
                Title = "Server Error"
            };

            _logger.LogError(exception, "An error occurred: {Message}", exception.Message);
            context.Response.ContentType = MediaTypeNames.Application.Json;
            //context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            //var result = Result.Error(exception.ToStringDemystified());
            var result = Result.Error(exception.Message);
            await context.Response.WriteAsJsonAsync(result);
        }
    }
}