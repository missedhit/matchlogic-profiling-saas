using MatchLogic.Api.Handlers.DataSource.CheckRemoteUpdates;
using MatchLogic.Api.Handlers.DataSource.Refresh;
using MatchLogic.Api.Handlers.DataSource.AvailableDatabases;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Data;
using MatchLogic.Api.Handlers.DataSource.Delete;
using MatchLogic.Api.Handlers.DataSource.GetHeaders;
using MatchLogic.Api.Handlers.DataSource.List;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.Handlers.DataSource.Preview.Tables;
using MatchLogic.Api.Handlers.DataSource.TestConnection;
using MatchLogic.Api.Handlers.DataSource.Update;
using MatchLogic.Api.Handlers.File.Confirm;
using MatchLogic.Api.Handlers.File.Delete;
using MatchLogic.Api.Handlers.File.List;
using MatchLogic.Api.Handlers.File.PresignedUpload;
using MatchLogic.Api.Handlers.File.Upload;
using MatchLogic.Domain.Project;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using System;
using System.Collections.Generic;
using System.Net.Mime;
using MatchLogic.Application.Identity;


namespace MatchLogic.Api.Endpoints;

public static class DataImportEndpoints
{
    public static string PATH = "DataImport";

    public static void MapFileDataImportEndpoints(this IEndpointRouteBuilder builder)
    {
        var group = builder.MapGroup($"api/{PATH}")
        .WithTags("Data Import");

        //Upload File (legacy multipart path — kept while frontend transitions to presigned PUT, M2)
        group.MapPost("/File", async (IFormFile File, Guid ProjectId, string SourceType, IMediator mediator) =>
        {
            return await mediator.Send(new FileUploadRequest()
            { File = File, ProjectId = ProjectId, SourceType = SourceType });
        })
        .DisableAntiforgery()
        .Accepts<IFormFile>(MediaTypeNames.Multipart.FormData)
        .Produces<FileUploadResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        // Mint presigned PUT URL for browser-direct upload to S3 (M2)
        group.MapPost("/File/PresignedUpload", async (PresignedUploadRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        })
        .Produces<PresignedUploadResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        // Confirm S3 upload completed; persists FileImport doc + returns metadata (M2)
        group.MapPost("/File/Confirm", async (ConfirmUploadRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        })
        .Produces<FileUploadResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status404NotFound)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);
        // Get All Files
        group.MapGet("/File", async (IMediator mediator) =>
        {
            return await mediator.Send(new FileListRequest());
        }).Produces<List<FileListResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.View);
        //Delete File
        group.MapDelete("/File/{id}", async (IMediator mediator, Guid id) =>
        {
            return await mediator.Send(new FileDeleteRequest(id));
        }).Produces<FileDeleteResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //Test Connection
        group.MapPost("Preview/TestConnection", async (BaseConnectionInfo request, IMediator mediator) =>
        {
            return await mediator.Send(new TestConnectionRequest(request));
        }).Produces<PreviewTablesResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //Available Databases
        group.MapPost("Preview/Databases", async (BaseConnectionInfo request, IMediator mediator) =>
        {
            return await mediator.Send(new AvailableDatabasesRequest(request));
        }).Produces<PreviewTablesResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //Preview List of Tables
        group.MapPost("Preview/Tables", async (BaseConnectionInfo request, IMediator mediator) =>
        {
            return await mediator.Send(new PreviewTablesRequest(request));
        }).Produces<PreviewTablesResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);
        //Preview List of Columns
        group.MapPost("Preview/Columns", async (BaseConnectionInfo request, IMediator mediator) =>
        {
            return await mediator.Send(new PreviewColumnsRequest(request));
        }).Produces<PreviewColumnsResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //Preview Meta File
        group.MapPost("Preview/Data", async (PreviewDataRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);

        }).Produces<PreviewDataResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //Add Data Source
        group.MapPost("/DataSource", async (CreateDataSourceRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        }).Produces<CreateDataSourceResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        group.MapPost("/DataSource/Refresh", async (RefreshDataSourceRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        }).Produces<RefreshDataSourceResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //List Data Sources
        group.MapGet("/DataSource", async (IMediator mediator, Guid ProjectId) =>
        {
            return await mediator.Send(new ListDataSourceRequest(ProjectId));
        }).Produces<List<ListDataSourceResponse>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.View);
        //List Data Source Headers
        group.MapGet("/DataSource/Headers/{id}", async (Guid id, IMediator mediator) =>
        {
            return await mediator.Send(new GetHeadersDataSourceRequest(id));
        }).Produces<List<string>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.View);

        //List Data Source Headers
        group.MapGet("/DataSource/CleanseHeaders/{id}", async (Guid id, IMediator mediator) =>
        {
            return await mediator.Send(new GetHeadersDataSourceRequest(id, true));
        }).Produces<List<string>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.View);

        // Update Data Source Name
        group.MapPatch("/DataSource", async (UpdateDataSourceRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        }).Produces<UpdateDataSourceResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);
        //Delete Data Source
        group.MapDelete("/DataSource/{id}",
            async (Guid id, Guid projectId, IMediator mediator) =>
        {
            return await mediator.Send(new DeleteDataSourceRequest(projectId, id));
        }).Produces<DeleteDataSourceResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

        //List Data Preview of Data Source
        group.MapGet("/DataSource/Data", async (IMediator mediator, Guid Id, int? PageNumber, int? PageSize, string? FilterText, string? SortColumn, bool? Ascending) =>
        {
            var request = new PreviewDataSourceRequest
            {
                Id = Id,
                PageNumber = PageNumber ?? 1,
                PageSize = PageSize ?? 10,
                FilterText = FilterText,
                SortColumn = SortColumn,
                Ascending = Ascending ?? true
            };
            return await mediator.Send(request);
        }).Produces<PreviewDataSourceResponse>(StatusCodes.Status200OK)
        .Produces<PreviewDataSourceResponse>(StatusCodes.Status400BadRequest)
        .Produces<PreviewDataSourceResponse>(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.View);

        //Check Remote Data Sources for Updates
        group.MapPost("/DataSource/CheckRemoteUpdates", async (CheckRemoteUpdatesRequest request, IMediator mediator) =>
        {
            return await mediator.Send(request);
        }).Produces<List<CheckRemoteUpdateResult>>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest)
        .Produces(StatusCodes.Status500InternalServerError)
        .RequireAuthorization(AppPermissions.DataImport.Execute);

    }



}
