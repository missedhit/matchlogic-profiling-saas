using Ardalis.Result;
using MediatR;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Upload;
public record FileUploadRequest : IRequest<Result<FileUploadResponse>>
{
    public IFormFile File { get; init; }
}
