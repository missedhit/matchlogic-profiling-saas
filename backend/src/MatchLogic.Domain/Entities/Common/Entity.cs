using System;

namespace MatchLogic.Domain.Entities.Common;

/*public abstract class Entity<T> 
{
    public virtual T Id { get; set; } = default!;
}*/

public abstract class Entity<TKey>
{
    public TKey Id { get; set; } = default!;
}

public  class IEntity : Entity<Guid>
{
    protected IEntity() => Id = Guid.NewGuid();
}