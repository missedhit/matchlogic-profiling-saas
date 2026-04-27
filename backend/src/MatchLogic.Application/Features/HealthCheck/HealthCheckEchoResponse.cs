using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.HealthCheck;
public record HealthCheckEchoResponse
{
    public HealthCheckEchoResponse(string text)
    {
        Text = text;
    }

    public string Text { get; init; }
}