using System;

namespace MatchLogic.Domain.Entities.Common;

public interface IAuditableEntity
{
    Guid CreatedBy { get; set; }
    DateTime CreatedAt { get; set; }
    Guid? ModifiedBy { get; set; }
    DateTime? ModifiedAt { get; set; }
}
