using Ardalis.Result;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.HealthCheck.Echo;
public record HealthCheckEchoRequest : IRequest<Result<HealthCheckEchoResponse>>
{
    public string Text { get; init; }
}
