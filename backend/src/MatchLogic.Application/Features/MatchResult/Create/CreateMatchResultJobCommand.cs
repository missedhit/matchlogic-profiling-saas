using Ardalis.Result;
using MatchLogic.Application.Features.Import;
using MatchLogic.Domain.Entities;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchResult.Create;
public class CreateMatchResultJobCommand : IRequest<Result<CreateMatchResultJobResponse>>
{
    public Guid JobId { get; set; }    
    //public string Name { get; set; }    
}
