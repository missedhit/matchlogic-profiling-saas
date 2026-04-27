using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    //PairItemRowReference
    public readonly struct PairItemRowReference : IEquatable<PairItemRowReference>, IComparable<PairItemRowReference>
    {
        public Guid DataSourceId { get; init; }
        public int RowNumber { get; init; }

        public PairItemRowReference(Guid dataSourceId, int rowNumber)
        {
            DataSourceId = dataSourceId;
            RowNumber = rowNumber;
        }

        public bool Equals(PairItemRowReference other) =>
            DataSourceId.Equals(other.DataSourceId) && RowNumber == other.RowNumber;

        public override bool Equals(object obj) =>
            obj is PairItemRowReference other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(DataSourceId, RowNumber);

        public int CompareTo(PairItemRowReference other)
        {
            var sourceComparison = DataSourceId.CompareTo(other.DataSourceId);
            return sourceComparison != 0 ? sourceComparison : RowNumber.CompareTo(other.RowNumber);
        }

        public override string ToString() => $"{DataSourceId:N}:{RowNumber}";
    }
}
