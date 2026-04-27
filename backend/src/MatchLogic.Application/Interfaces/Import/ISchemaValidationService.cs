using MatchLogic.Domain.Project;
using System.Collections.Generic;

namespace MatchLogic.Application.Interfaces.Import;

public interface ISchemaValidationService
{
    string ComputeSignature(IEnumerable<string> headers, SchemaPolicy policy);
    void ValidateHeadersAgainstDataSource(DataSource dataSource, IEnumerable<string> headers);
}
