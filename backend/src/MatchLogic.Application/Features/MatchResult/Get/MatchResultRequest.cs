using Ardalis.Result;
using MatchLogic.Application.Features.Import;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchResult.Get;
public record MatchResultRequest(Guid JobId) : IRequest<Result<MatchResultResponse>>;

