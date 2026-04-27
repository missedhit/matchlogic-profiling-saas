using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.MappedFieldRow.Get;

public class MappedFieldRowResponse
{
    public List<MappedFieldRowDto> MappedFieldsRow { get; set; }   
}
public class MatchCriteriaResponse
{
    public string FieldName { get; set; }
    //public bool Include { get; set; }
    public MatchingType MatchingType { get; set; }
    public CriteriaDataType DataType { get; set; }
    public double Weight { get; set; }
    public Dictionary<ArgsValue, string> Arguments { get; set; }
}