using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Upload;
public record FileUploadResponse
{
    public Guid FileId { get; init; }

    public FileUploadResponse(Guid fileId)
    {
        FileId = fileId;
    }
}
