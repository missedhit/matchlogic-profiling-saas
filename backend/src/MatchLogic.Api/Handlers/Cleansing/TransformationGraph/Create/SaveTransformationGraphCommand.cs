using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;
using System.Text.Json;

namespace MatchLogic.Api.Handlers.Cleansing.TransformationGraph.Create;

public class SaveTransformationGraphCommand : IRequest<Result<Domain.CleansingAndStandaradization.TransformationGraph>>
{
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }
    public JsonDocument GraphJson { get; set; } = JsonDocument.Parse("{}");
}
