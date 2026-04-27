using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.Validators;

public class BaseConnectionInfoValidator : AbstractValidator<BaseConnectionInfo>
{
    public BaseConnectionInfoValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;

        RuleFor(x => x.Parameters)
            .NotNull()
            .WithMessage(ValidationMessages.CannotBeNull("Parameters"))
            .Must(x => x != null && x.Count > 0)
            .WithMessage(ValidationMessages.CannotBeEmpty("Parameters"));


        //RuleForEach(x => x.Parameters)
        //    .Must(x => x.Key != null && x.Value != null)
        //    .WithMessage(ValidationMessages.CannotBeNull("Parameters"));

        // Dabase Specific Validations
        When(x=> x.Parameters != null && (x.Parameters.ContainsKey("Server") || x.Parameters.ContainsKey("Database")), () =>
        {
            RuleFor(x => x)
               .SetValidator(new DatabaseConnectionValidator());
        });

        // Validate that Parameters contains a valid FileId for File-based data sources
        When(x => x.Parameters != null && x.Parameters.ContainsKey("FileId"), () => 
        {
            //Validate that FileId parameter exists and is a valid Guid
            RuleFor(x => x)
                .Must(x => x.Parameters.TryGetValue("FileId", out var fileIdStr) && Guid.TryParse(fileIdStr, out _))
                //.WithMessage("FileId must be a valid Guid.")
                .WithMessage(ValidationMessages.CannotBeNull("FileId"))
                .DependentRules(() =>
                {
                    RuleFor(x => Guid.Parse(x.Parameters["FileId"]))
                        .SetValidator(new FileBasedDataSourceValidator(fileImportRepository));

                    // Validate Excel specific connection info
                    When(x => x.Type is DataSourceType.Excel, () =>
                    {
                        //RuleFor(x => (ExcelConnectionInfo)x)
                        RuleFor(x => x)
                            .SetValidator(new ExcelConnectionValidator(fileImportRepository));
                    });
                    // Validate CSV specific connection info
                    When(x => x.Type is DataSourceType.CSV, () =>
                    {
                        //RuleFor(x => (CSVConnectionInfo)x)
                        RuleFor(x => x)
                            .SetValidator(new CSVConnectionValidator(fileImportRepository));
                    });
                });
        });



        

    }

    
}
