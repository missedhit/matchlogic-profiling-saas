using Ardalis.Result;
using Mapster;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

namespace MatchLogic.Application.Features.HealthCheck.Echo;
public class HealthCheckEchoHandler : IRequestHandler<HealthCheckEchoRequest, Result<HealthCheckEchoResponse>>
{
    public Task<Result<HealthCheckEchoResponse>> Handle(HealthCheckEchoRequest request, CancellationToken cancellationToken)
    {
        HealthCheckEchoResponse response = new HealthCheckEchoResponse($"Echo -- {request.Text}");
        return Task.FromResult(new Result<HealthCheckEchoResponse>(response));
    }
}
