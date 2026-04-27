using System;

namespace MatchLogic.Domain.Entities.Common;

public abstract class AuditableEntity : IEntity, IAuditableEntity
{
    public Guid CreatedBy { get; set; }
    public DateTime CreatedAt { get; set; }
    public Guid? ModifiedBy { get; set; }
    public DateTime? ModifiedAt { get; set; }
}
