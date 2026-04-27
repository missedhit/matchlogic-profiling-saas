using System;

namespace MatchLogic.Domain.Import;

public interface IFileConnectionInfo
{
    Guid FileId { get; set; }
    bool ValidateFileExtension(string filePath, params string[] allowedExtensions);
}



